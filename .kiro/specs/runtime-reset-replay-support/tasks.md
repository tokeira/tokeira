# Implementation Plan: Runtime Reset Replay Support

## Overview

Add the smallest runtime/storage support needed for correct reset successor replay. Work is ordered so the contracts become explicit before edge wiring depends on them.

## Tasks

- [x] 0. Implement kernel history replay function (prerequisite for all other tasks)
  - [x] 0.1 Add `replay_history_prefix(ctx: ReplayContext, events: &[HistoryEvent]) -> Result<WorkflowState>` to `BasicKernel`
    - Define `ReplayContext` struct with `run_key`, `namespace_id`, `workflow_id`, `run_id`, `deployment`, `build_id`, `parent_run_key`, `parent_workflow_id`, `first_run_started_at`
    - Process `WorkflowExecutionStarted` to initialize state from both the event fields and the `ReplayContext` envelope fields
    - Process `WorkflowTaskScheduled/Started/Completed/Failed/TimedOut` to manage WFT lifecycle and `pending_workflow_task`
    - Process `ActivityTaskScheduled/Started/Completed/Failed/TimedOut/Canceled/CancelRequested` to manage `activities` map
    - Process `TimerStarted/Fired/Canceled` to manage `timers` map
    - Process `WorkflowExecutionSignaled/CancelRequested/Paused/Unpaused/Terminated/Completed/Failed/TimedOut/ContinuedAsNew` for status and close state
    - Process child workflow events, external signal events, nexus events, update events, marker events
    - Set `transition_seq = TransitionSeq::ZERO` (history doesn't encode transition boundaries)
    - Set `last_event_id` to the last event's `event_id`
    - Set non-historical fields to reset defaults: `sticky = None`, `wft_stamp = 0`, `ActivityState.pause_info = None`, `ActivityState.stamp = 0`
    - Reconstruct `pause_info` from `WorkflowExecutionPaused`/`Unpaused` events, and `versioning_override` and `completion_callbacks` from `WorkflowExecutionOptionsUpdated` events (these ARE in history)
    - Reject empty event sequences or sequences not starting with `WorkflowExecutionStarted`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12_
  - [x] 0.2 Add unit tests for replay function
    - Replay a simple workflow: Started → TaskScheduled → TaskStarted → TaskCompleted → verify state has no pending WFT
    - Replay with activity: Started → TaskScheduled → TaskStarted → TaskCompleted → ActivityScheduled → ActivityStarted → verify activity in state
    - Replay with timer: Started → TaskScheduled → TaskStarted → TaskCompleted → TimerStarted → verify timer in state
    - Replay empty sequence → error
    - Replay sequence not starting with Started → error
    - _Requirements: 3.1, 3.7_
  - [ ]* 0.3 Add property test: replay produces deterministic state
    - For any valid history prefix, replaying twice produces identical `WorkflowState`
    - _Requirements: 3.5_

- [x] 1. Add runtime-facing reset API
  - [x] 1.1 Extend `WorkflowRuntimeApi` in `crates/tokeira-edge/src/workflow_service.rs` with `reset_workflow(...)`
  - [x] 1.2 Implement that method in `crates/tokeira-edge/src/grpc/runtime_adapter.rs`
  - [x] 1.3 Add `TokeiraRuntime::reset_workflow(...)` in `crates/tokeira-runtime/src/runtime.rs`
    - Choose successor `RunKey` before reset submission
    - Return `ResetWorkflowResult`, not bare `CommitResult`

- [x] 2. Add storage materialization primitive
  - [x] 2.1 Extend `RunRepository` in `crates/tokeira-storage/src/api.rs` with `materialize_reset_successor(...)`
  - [x] 2.2 Implement the method in `crates/tokeira-storage/src/memory.rs`
    - Copy history prefix `[1..fork_event_id]` from base run
    - Build `ReplayContext` from predecessor run's metadata (namespace_id, workflow_id, etc.) and successor identity (successor_run_key, successor_run_id)
    - Call `kernel.replay_history_prefix(ctx, &copied_events)` to derive successor `WorkflowState`
    - The derived state has `transition_seq = TransitionSeq::ZERO` and `last_event_id` at the fork point
    - Persist the derived state as the successor's durable state
    - Derive `successor_run_key` deterministically from `successor_run_id` as `RunKey(run_id.0)` for crash-recovery
    - Update current-execution mapping
    - Make successor history readable immediately after success
  - [x] 2.3 Add focused storage tests covering:
    - valid prefix materialization with state derived from replay
    - invalid `fork_event_id`
    - successor durable visibility after materialization
    - replayed state matches expected activity/timer/WFT state at fork point

- [x] 3. Add lane reset-successor orchestration
  - [x] 3.1 Extend the lane post-commit path in `crates/tokeira-runtime/src/lane.rs` to detect reset metadata from committed history
  - [x] 3.2 Call `materialize_reset_successor(...)` after a committed reset close
  - [x] 3.3 Make the reset path synchronous from the caller's perspective rather than detached fire-and-forget work
  - [x] 3.4 Log failure without affecting the predecessor's committed reset

- [x] 4. Wire edge reset endpoint to the new runtime support
  - [x] 4.1 Add reset DTO translations in `crates/tokeira-edge/src/grpc/translate.rs`
  - [x] 4.2 Implement `WorkflowService::reset_workflow_execution(...)` in `crates/tokeira-edge/src/workflow_service.rs`
  - [x] 4.3 Replace the gRPC stub in `crates/tokeira-edge/src/grpc/workflow_service.rs`

- [ ] 5. Add edge/runtime tests
  - [x] 5.1 Reset of missing execution returns `NOT_FOUND`
  - [x] 5.2 Invalid reset event ID returns `INVALID_ARGUMENT`
    - Accept valid targets for `WorkflowTaskCompleted`, `WorkflowTaskFailed`, `WorkflowTaskTimedOut`, and `WorkflowTaskStarted`
  - [x] 5.3 Successful reset returns the successor `run_id`
  - [x] 5.4 Successor is durably queryable after reset returns
  - [x] 5.5 Non-reset terminate does not trigger reset successor creation

- [x] 6. Checkpoint
  - [x] 6.1 Run `cargo test --workspace --no-run`
  - [x] 6.2 Run targeted reset tests

## Notes

- Task 0 (kernel replay) is the prerequisite that unblocks Task 2 (storage materialization). Without replay, there is no way to derive correct `WorkflowState` at an arbitrary fork point.
- The kernel replay function is narrowly scoped to reset — it reconstructs state from events but does not implement a general replay engine with command re-execution.
- This feature is intentionally specific to reset. Do not expand it into a general replay/import abstraction unless the existing implementation proves that unavoidable.
- Continue-As-New behavior is out of scope except where shared lane helper logic can be reused cleanly.
- Tasks marked with `*` are optional.
