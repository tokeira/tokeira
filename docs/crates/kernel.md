# tokeira-kernel

Pure deterministic state machine that owns workflow semantic correctness. The kernel derives the authoritative next state for a workflow run given its current state and a command. It produces history events and explicit transition effects, but never executes I/O; storage derives visibility snapshots from the committed next state.

## Dependencies

- `tokeira-types` — identity types, payloads, search attributes, retry policy
- External: `smallvec`, `thiserror`, `time`, `tracing`

## Module Structure

| File | Contents |
|---|---|
| `state.rs` | `WorkflowState`, `LoadedRun`, `PendingWorkflowTask`, `ActivityState` (with `started_event_id`), `TimerState`, `ChildWorkflowState`, `PendingExternalSignal`, `PendingExternalCancel`, `PendingUpdate`, `PendingNexusOperation`, `VersioningOverride`, `CompletionCallback`, `ParentClosePolicy`, `PauseInfo`, `ActivityPauseInfo` |
| `command.rs` | `Command` enum (~27 top-level variants), `WorkflowCommand` enum (~16 worker-issued variants), all request structs, conflict/reuse policies, retry state, timeout types |
| `event.rs` | `HistoryEvent`, `HistoryEventKind` (40+ variants including `ActivityTaskStarted`), `ActivityResolution`, `CloseInfo` |
| `kernel.rs` | `Kernel` trait, `BasicKernel` implementation, `TransitionBuilder`, `Reject` error enum, `ReplayContext` |
| `transition.rs` | `Transition`, `DispatchOp`, `ActivityOp`, `TimerOp`, `RequestDedupeOp` |

## Command Variants

Top-level `Command`: Start, SignalWithStart, Signal, Update, Cancel, Terminate, PauseWorkflow, UnpauseWorkflow, UpdateActivityOptions, PauseActivity, UnpauseActivity, ResetActivity, Reset, UpdateExecutionOptions, WorkflowExecutionTimedOut, WorkflowTaskStarted, WorkflowTaskCompleted, ActivityStarted, ActivityResolved, ChildStartConfirmed, ChildResolved, ExternalSignalResolved, ExternalCancelResolved, NexusOperationResolved, WorkflowTaskFailed, WorkflowTaskTimedOut, TimerDue.

Worker `WorkflowCommand`: ScheduleActivity, StartTimer, CancelTimer, RequestCancelActivity, CompleteWorkflow, FailWorkflow, ContinueAsNew, CancelWorkflow, StartChildWorkflow, SignalExternalWorkflow, RequestCancelExternalWorkflow, ScheduleNexusOperation, RequestCancelNexusOperation, RecordMarker, ProtocolMessage, UpdateExecutionOptions.

## Kernel Entry Point

`BasicKernel::apply(loaded, command) -> Result<Transition, Reject>` dispatches to ~27 internal apply methods. `replay_history_prefix` replays a history up to a fork point for reset materialisation.

## Activity Support

- `ActivityTaskStarted` event in the event model
- `apply_activity_started` kernel operation
- `scheduled_event_id` and `started_event_id` on activity resolution events
- `activity_type`, `header`, `retry_policy` threaded through `ScheduleActivity` command and `ActivityTaskScheduled` event
- `started_event_id` field on `ActivityState`

## Tests

- `tests/golden_tests.rs` — deterministic golden-file tests for command→event sequences
- `tests/property_tests.rs` — proptest-based property tests for correctness invariants

## Key Invariants

- Pure: no I/O, no async, no side effects
- At most one pending WFT per run at any time
- History event IDs are contiguous and never reused within a run
- `TransitionSeq` increments by exactly one per transition
- Close events are terminal — no further mutations after close
- Parent close policy is applied atomically with the parent's close
