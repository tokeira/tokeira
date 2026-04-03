# tokeira-kernel

**Purpose:** Pure deterministic state machine at the center of the system.

See [020-kernel](../architecture/020-kernel.md) for the full command taxonomy, reject taxonomy, and design rationale.

## What it owns

- **Command processing** — validates preconditions and applies one semantic command per `apply` call
- **Transition production** — assembles the complete `Transition` output describing what must be committed
- **History event emission** — assigns contiguous event IDs and produces typed history events
- **State mutation** — computes the full next `WorkflowState` (not a delta)
- **Projection op emission** — declares what visibility mutations follow from the state change
- **Dispatch op emission** — declares what task delivery actions the runtime must perform
- **Request dedup ops** — declares which request IDs must be persisted for idempotency

## What it does NOT own

- **I/O** — no database calls, no network, no file system
- **Storage** — does not know about DSQL, tables, or transactions
- **Delivery** — does not know about pollers, sync match, or backlog
- **Routing** — does not know about shards, lanes, or nodes
- **Time** — does not call the clock; timestamps are passed in as data
- **Randomness** — does not generate IDs; runtime passes exact values

## Core Interface

```rust
pub trait Kernel {
    fn apply(&self, loaded: LoadedRun, cmd: Command) -> Result<Transition, Reject>;
}
```

- `LoadedRun::Absent` — run does not exist; only `Start` accepts this
- `LoadedRun::Existing(WorkflowState)` — current authoritative state, already loaded and fenced
- `Transition` — bounded description of what must be committed
- `Reject` — precise enumerated rejection reason

## Module Map

```
tokeira-kernel/src/
  command.rs    — Command enum and per-command payloads
  event.rs      — HistoryEvent types
  kernel.rs     — Kernel trait and implementation
  state.rs      — WorkflowState, PendingWorkflowTask, StickyAffinity, entity states
  transition.rs — Transition struct, TransitionBuilder, ops
```

## Command Taxonomy

25 top-level commands, 20+ workflow commands within `WorkflowTaskCompleted`.

### Top-Level Commands

| Command | Origin | Requires open run | Request dedup |
|---|---|---|---|
| `Start` | External | No (requires `Absent`) | Yes |
| `Signal` | External | Yes | Yes |
| `Update` | External | Yes | Yes |
| `Cancel` | External / parent | Yes | Yes |
| `Terminate` | External / operator | Yes | Yes |
| `WorkflowTaskStarted` | Runtime | Yes | No |
| `WorkflowTaskCompleted` | Worker via runtime | Yes | No |
| `WorkflowTaskFailed` | Runtime | Yes | No |
| `WorkflowTaskTimedOut` | Runtime | Yes | No |
| `ActivityResolved` | Worker via runtime | Yes | No |
| `TimerDue` | Timer scanner | Yes | No |
| `ChildStartConfirmed` | Runtime | Yes | No |
| `ChildResolved` | Runtime | Yes | No |
| `ExternalSignalResolved` | Runtime | Yes | No |
| `ExternalCancelResolved` | Runtime | Yes | No |
| `NexusOperationResolved` | Runtime | Yes | No |
| `WorkflowExecutionTimedOut` | Runtime | Yes | No |
| `UpdateExecutionOptions` | External / operator | Yes | Yes |
| `PauseWorkflow` | External / operator | Yes | Yes |
| `UnpauseWorkflow` | External / operator | Yes | Yes |
| `UpdateActivityOptions` | External / operator | Yes | Yes |
| `PauseActivity` | External / operator | Yes | Yes |
| `UnpauseActivity` | External / operator | Yes | Yes |
| `ResetActivity` | External / operator | Yes | Yes |
| `Reset` | Operator | Yes | Yes |

### Workflow Commands (within WorkflowTaskCompleted)

`ScheduleActivity`, `StartTimer`, `CompleteWorkflow`, `FailWorkflow`, `CancelWorkflow`, `ContinueAsNew`, `RequestNewWorkflowTask`, `UpsertMemo`, `UpsertSearchAttributes`, `StartChildWorkflow`, `RequestCancelActivity`, `CancelTimer`, `SignalExternalWorkflow`, `RequestCancelExternalWorkflow`, `RecordMarker`, `ScheduleNexusOperation`, `CancelNexusOperation`, `ProtocolMessage`, `UpdateCompleted`, `UpdateRejected`.

## Transition Shape

```rust
pub struct Transition {
    pub expected_seq: TransitionSeq,
    pub next_state: WorkflowState,
    pub history_events: SmallVec<[HistoryEvent; 8]>,
    pub request_dedupe_ops: SmallVec<[RequestDedupeOp; 1]>,
    pub activity_ops: SmallVec<[ActivityOp; 4]>,
    pub timer_ops: SmallVec<[TimerOp; 4]>,
    pub dispatch_ops: SmallVec<[DispatchOp; 4]>,
    pub projection_ops: SmallVec<[ProjectionOp; 8]>,
}
```

- `expected_seq` — OCC fence; storage rejects if durable seq has moved past this
- `next_state` — full replacement state, not a delta
- One transition may append multiple history events but increments `transition_seq` exactly once

## Reject Taxonomy

Precise, enumerated rejection reasons the runtime can act on programmatically:

- **Existence:** `RunAlreadyExists`, `MissingRun`, `RunClosed(status)`
- **Sequence fencing:** `WorkflowTaskSeqMismatch`, `WorkflowTaskAlreadyStarted`, `WorkflowTaskNotStarted`, `WorkflowTaskStartedEventMismatch`
- **Token validation:** `WorkflowTaskTokenMismatch`
- **Uniqueness:** `DuplicateActivityId`, `DuplicateTimerId`, `DuplicateChildWorkflowId`, `DuplicateUpdateId`, `DuplicateNexusOperationId`
- **Entity resolution:** `UnknownActivity`, `UnknownTimer`, `UnknownChild`, `UnknownUpdate`, `UnknownExternalSignal`, `UnknownExternalCancel`, `UnknownNexusOperation`
- **Ordering:** `CommandsAfterClose`
- **Pause control:** `WorkflowPaused`, `AlreadyPaused`, `NotPaused`, `ActivityNotPaused`
- **Nexus:** `NexusOperationAlreadyStarted`
- **Reset:** `ResetConstraintViolation`

## Key Invariants

1. **At-most-one-WFT** — at most one workflow task is pending at any time
2. **Event ID monotonicity** — event IDs are strictly increasing within a run
3. **Terminal absorption** — once closed, no further transitions are possible
4. **No hidden I/O** — all reads happen before `apply`, all writes happen after

## Relationship to Temporal's History Service

The kernel is analogous to the core state-transition logic inside Temporal's History Service, but it is deliberately **not a service**. It is a pure library with no I/O, no RPC surface, and no storage access. The runtime calls it as a function.

## Implementation Status

| Feature | Spec | Status |
|---|---|---|
| F1: Foundation + WFT lifecycle | `kernel-foundation-wft-lifecycle` | ✅ Implemented |
| F2: WFT failure and timeout recovery | `kernel-wft-failure-timeout` | ✅ Implemented |
| F3: Cancel and terminate | `kernel-cancel-terminate` | ✅ Implemented |
| F4: ContinueAsNew and workflow timeout | `kernel-continue-as-new-timeout` | ✅ Implemented |
| F5: Child workflows | `kernel-child-workflows` | ✅ Implemented |
| F6: External signals and cancel requests | `kernel-external-signals-cancels` | ✅ Implemented |
| F7: Updates | `kernel-updates` | ✅ Implemented |
| F8: Markers and execution options | `kernel-markers-execution-options` | ✅ Implemented |
| F9: Nexus operations | `kernel-nexus-operations` | ✅ Implemented |
| F10: Reset | `kernel-reset` | ✅ Implemented |
| F11: Pause / activity management | `kernel-pause-activity-management` | ✅ Implemented |

All 11 kernel features are complete.

## Temporal Feature Coverage

| Feature | Kernel participation |
|---|---|
| Workflow lifecycle | Owns all state transitions (start → close) |
| Signals | Processes `Signal` command, emits event, coalesces WFT |
| Updates | Accepts update, tracks pending, completes via WFT |
| Cancel | Records request, schedules WFT for cooperative cleanup |
| Terminate | Hard close, clears all pending entities |
| Activities | Schedules via workflow cmd, resolves via top-level cmd |
| Timers | Starts via workflow cmd, fires via `TimerDue` |
| Children | Initiates, tracks, resolves, applies parent close policy |
| Continue-As-New | Closes current run, emits linkage for runtime |
| Queries | **Not involved** — queries are read-only, handled by runtime |
| Visibility | Emits `ProjectionOp`s; does not write visibility rows |
| Nexus | Schedules, cancels, resolves operations |
| Reset | Terminates run, emits metadata for runtime successor |
| Markers | Pass-through to history; no state change |
| Pause / unpause | Owns pause state, WFT suppression, and unpause redispatch |
| Activity management | Owns activity pause state, option updates, and reset redispatch |
