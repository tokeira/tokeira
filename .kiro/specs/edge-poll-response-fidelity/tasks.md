# Implementation Tasks: Edge Poll Response Fidelity

## Task 1: Kernel — Add `previous_started_event_id` to `WorkflowState`
> Requirements: 1 (AC 1.1, 1.2, 1.3)
> Design: Component 1

- [x] 1.1 Add `previous_started_event_id: i64` field to `WorkflowState` in `crates/tokeira-kernel/src/state.rs`, initialized to `0`
- [x] 1.2 In `apply_workflow_task_completed` in `crates/tokeira-kernel/src/kernel.rs`, set `builder.state.previous_started_event_id = started_event_id` (the started_event_id of the completing WFT) before clearing `pending_workflow_task`
- [x] 1.3 In `replay_history_prefix` in `crates/tokeira-kernel/src/kernel.rs`, update `previous_started_event_id` when replaying `WorkflowTaskCompleted` events
- [x] 1.4 Initialize `previous_started_event_id: 0` in `apply_start` and `apply_signal_with_start` initial state construction
- [x] 1.5 Write unit test: first WFT completion sets `previous_started_event_id` to the WFT's `started_event_id`; before any completion it is 0
- [x] 1.6 [PBT] Write property test: for any sequence of N WFT completions, `state.previous_started_event_id` equals the `started_event_id` of the Nth completed WFT (Property 1)

## Checkpoint: Kernel `previous_started_event_id` — verify `cargo test -p tokeira-kernel` passes

## Task 2: Kernel — Enrich `WorkflowTaskScheduled` event with task_queue, timeout, attempt
> Requirements: 3 (AC 3.1)
> Design: Component 2

- [x] 2.1 Add `task_queue: TaskQueueName`, `workflow_task_timeout: Duration`, and `attempt: u32` fields to `HistoryEventKind::WorkflowTaskScheduled` in `crates/tokeira-kernel/src/event.rs`
- [x] 2.2 Add `workflow_task_attempt: u32` field to `WorkflowState` in `crates/tokeira-kernel/src/state.rs`, initialized to `1`
- [x] 2.3 In `TransitionBuilder::schedule_workflow_task` in `crates/tokeira-kernel/src/kernel.rs`, populate the new event fields from `self.state.task_queue`, `self.state.workflow_task_timeout`, and `self.state.workflow_task_attempt`
- [x] 2.4 In `apply_workflow_task_completed`, reset `state.workflow_task_attempt = 1` (successful completion resets the counter)
- [x] 2.5 In `apply_workflow_task_failed`, increment `state.workflow_task_attempt += 1` before re-scheduling
- [x] 2.6 In `apply_workflow_task_timed_out`, increment `state.workflow_task_attempt += 1` before re-scheduling (matching Temporal's `failWorkflowTask` with `incrementAttempt=true` for start-to-close timeouts)
- [x] 2.7 Update `apply_replayed_event` to handle the new fields when replaying `WorkflowTaskScheduled` events
- [x] 2.8 Fix all pattern matches on `WorkflowTaskScheduled` across the kernel crate to destructure the new fields

## Task 3: Kernel — Add `scheduled_at` to `PendingWorkflowTask`
> Requirements: 4 (AC 4.1)
> Design: Component 4

- [x] 3.1 Add `scheduled_at: OffsetDateTime` field to `PendingWorkflowTask` in `crates/tokeira-kernel/src/state.rs`
- [x] 3.2 In `TransitionBuilder::schedule_workflow_task`, set `scheduled_at: self.now` when constructing the `PendingWorkflowTask`
- [x] 3.3 Update `replay_history_prefix` to set `scheduled_at` from the `WorkflowTaskScheduled` event's `happened_at` when reconstructing `PendingWorkflowTask`

## Checkpoint: Kernel enrichment — verify `cargo test -p tokeira-kernel` and `cargo lint` pass

## Task 4: History Serializer — Populate `WorkflowTaskScheduled` proto attributes
> Requirements: 3 (AC 3.2, 3.3, 3.4)
> Design: Component 3

- [x] 4.1 In `history_serializer.rs`, update the `WorkflowTaskScheduled` match arm to destructure `task_queue`, `workflow_task_timeout`, and `attempt`, and populate `task_queue`, `start_to_close_timeout`, and `attempt` on `WorkflowTaskScheduledEventAttributes`
- [x] 4.2 [PBT] Write property test: for any `HistoryEvent` with `WorkflowTaskScheduled` kind, the serialized proto has non-default `task_queue`, `start_to_close_timeout`, and `attempt` (Property 2)

## Checkpoint: History serializer — verify `cargo test -p tokeira-edge` passes

## Task 5: Runtime — Enrich `StartedWorkflowTask` with new fields
> Requirements: 1 (AC 1.4), 4 (AC 4.1)
> Design: Component 4

- [x] 5.1 Add `previous_started_event_id: i64`, `scheduled_time: OffsetDateTime`, and `started_time: OffsetDateTime` fields to `StartedWorkflowTask` in `crates/tokeira-runtime/src/runtime.rs`
- [x] 5.2 In `start_workflow_task_inner`, populate `previous_started_event_id` from `new_state.previous_started_event_id`
- [x] 5.3 In `start_workflow_task_inner`, populate `scheduled_time` from `pending.scheduled_at` and `started_time` from `now` (the wall-clock time passed to the kernel command)
- [x] 5.4 Fix all construction sites of `StartedWorkflowTask` across the runtime crate and test files

## Checkpoint: Runtime enrichment — verify `cargo test -p tokeira-runtime` passes

## Task 6: Edge DTO — Add fields to `PollWorkflowTaskQueueResponse` and `StartWorkflowExecutionResponse`
> Requirements: 1 (AC 1.5), 2 (AC 2.3), 4 (AC 4.2, 4.3)
> Design: Components 5, 6

- [x] 6.1 Add `previous_started_event_id: i64`, `scheduled_time: Option<OffsetDateTime>`, and `started_time: Option<OffsetDateTime>` to `PollWorkflowTaskQueueResponse` in `crates/tokeira-edge/src/translate/mod.rs`
- [x] 6.2 Add `started: bool` to `StartWorkflowExecutionResponse` in `crates/tokeira-edge/src/translate/mod.rs`
- [x] 6.3 In `from_internal::poll_response`, populate the new fields from `StartedWorkflowTask`
- [x] 6.4 In `from_internal::start_response`, set `started: true` (new workflows always started)
- [x] 6.5 In `workflow_service.rs::start_workflow_execution`, ensure the `started` field is set correctly based on `StartWorkflowResult` variant

## Task 7: Edge gRPC translate — Populate proto fields
> Requirements: 1 (AC 1.5), 2 (AC 2.1, 2.2), 4 (AC 4.2, 4.3, 4.4)
> Design: Components 5, 6

- [x] 7.1 In `grpc/translate.rs::poll_response_to_proto`, populate `previous_started_event_id`, `scheduled_time`, and `started_time` from the edge DTO
- [x] 7.2 In `grpc/translate.rs::start_response_to_proto`, populate `started` from the edge DTO
- [x] 7.3 [PBT] Extend the existing `property_poll_response_projection` test to assert `previous_started_event_id`, `scheduled_time`, and `started_time` are correctly mapped (Property 3)
- [x] 7.4 [PBT] Extend the existing `property_start_response_projection` test to assert `started` is correctly mapped (Property 4)

## Checkpoint: Edge translation — verify `cargo test -p tokeira-edge` passes

## Task 8: Fix downstream compilation and test updates
> Requirements: All
> Design: All components

- [x] 8.1 Fix all pattern matches on `WorkflowTaskScheduled` in the history serializer (already covered by 4.1, but verify no other match sites exist)
- [x] 8.2 Fix all construction sites of `PollWorkflowTaskQueueResponse` in test files and mock implementations
- [x] 8.3 Fix all construction sites of `StartWorkflowExecutionResponse` in test files and mock implementations
- [x] 8.4 Update proptest arbitrary generators (`arb_poll_response`, `arb_start_response`, `arb_history_event_kind`) to include the new fields

## Checkpoint: Full build — verify `cargo lint` and `cargo test` pass across the workspace

## Task 9: Kernel — Add `started_at` to `PendingWorkflowTask` for timeout enforcement
> Requirements: 3 (AC 3.6, 3.7), 4 (AC 4.5, 4.6)
> Design: Component 7

- [x] 9.1 Add `started_at: Option<OffsetDateTime>` field to `PendingWorkflowTask` in `crates/tokeira-kernel/src/state.rs`
- [x] 9.2 In `apply_workflow_task_started` in `crates/tokeira-kernel/src/kernel.rs`, set `pending.started_at = Some(req.now)` alongside the existing `pending.started_event_id` assignment
- [x] 9.3 Initialize `started_at: None` when constructing `PendingWorkflowTask` in `schedule_workflow_task`
- [x] 9.4 In `apply_workflow_task_failed` and `apply_workflow_task_timed_out`, clear `started_at` back to `None` when reverting the WFT to scheduled-but-not-started
- [x] 9.5 Update `replay_history_prefix` to set `started_at` from the `WorkflowTaskStarted` event's `happened_at`

## Task 10: Runtime — WFT start-to-close timeout scanner
> Requirements: 3 (AC 3.6, 3.7), 4 (AC 4.5, 4.6)
> Design: Component 7

- [x] 10.1 Create `crates/tokeira-runtime/src/wft_timeout.rs` with `WftTimeoutTrackingState` (in-memory `HashMap<RunKey, WftTimeoutEntry>` behind `Arc<Mutex>`) and `WftTimeoutEntry` struct carrying `run_key`, `shard_id`, `logical_seq`, `started_event_id`, `started_at`, `workflow_task_timeout`
- [x] 10.2 Implement `evaluate_wft_timeout(entry, now) -> bool` that returns true when `now > started_at + workflow_task_timeout`
- [x] 10.3 Implement `scan_wft_timeouts_once` following the same pattern as `scan_workflow_timeouts_once` in `timeout.rs`
- [x] 10.4 Implement `run_wft_timeout_scanner` background loop following the same pattern as `run_workflow_timeout_scanner`
- [x] 10.5 In `start_polled_workflow_task`, insert a `WftTimeoutEntry` into the tracking state after the kernel applies `WorkflowTaskStarted`
- [x] 10.6 In `complete_workflow_task`, remove the entry from tracking state on successful completion
- [x] 10.7 Wire the scanner into `TokeiraRuntime::new` and add a `shutdown_wft_timeout_scanner` method
- [x] 10.8 Write unit test: a started WFT that exceeds its timeout triggers a `WorkflowTaskTimedOut` command; a WFT within its timeout does not
- [x] 10.9 Write unit test: a WFT that is completed before the scanner fires is removed from tracking and does not produce a timeout command

## Task 11: Storage and recovery — WFT timeout tracking reconstruction
> Requirements: 4 (AC 4.6)
> Design: Component 7

- [x] 11.1 Add `WftSweepEntry` struct to `crates/tokeira-storage/src/api.rs` with fields: `run_key`, `logical_seq`, `started_event_id`, `started_at`, `workflow_task_timeout`
- [x] 11.2 Add `list_runs_with_started_wfts_for_shard(shard_id, limit) -> Vec<WftSweepEntry>` to the `RunRepository` trait
- [x] 11.3 Implement the query in `InMemoryStore` — scan open runs where `pending_workflow_task.started_at.is_some()`
- [x] 11.4 Add `wft_timeout_tracking: &WftTimeoutTrackingState` parameter to `sweep_shard` in `crates/tokeira-runtime/src/recovery.rs`
- [x] 11.5 In `sweep_shard`, call `list_runs_with_started_wfts_for_shard` and insert each result into `WftTimeoutTrackingState`
- [x] 11.6 Add `wft_timeout_entries_reconstructed: usize` to `SweepResult`
- [x] 11.7 Update all `sweep_shard` call sites (runtime, tests) to pass the new tracking state parameter
- [x] 11.8 Write unit test: after `sweep_shard`, `WftTimeoutTrackingState` contains entries for runs with started WFTs

## Checkpoint: WFT timeout enforcement — verify `cargo test -p tokeira-runtime` and `cargo test -p tokeira-storage` pass
