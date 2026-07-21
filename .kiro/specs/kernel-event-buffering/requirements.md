# Requirements Document: Event Buffering and Force-Close WFT Ordering (Kernel)

## Introduction

This document captures the requirements for a kernel state-machine feature that adopts Temporal's
**buffered-event model** and the **force-close workflow-task ordering** that terminate performs when a
workflow task is in flight. It was raised by the functional-conformance drive (see
`docs/HANDOVER-kernel-event-buffering.md`, retired to git history) as the
load-bearing gap behind `TestTerminateWorkflowOnMessageTooLargeFailure` and, more broadly, behind every
history assertion that involves an event arriving while a workflow task is started.

The authoritative kernel specification is
[docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent kernel
requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md).
This feature depends on Feature 1 (`kernel-foundation-wft-lifecycle`), Feature 2
(`kernel-wft-failure-timeout`), and Feature 3 (`kernel-cancel-terminate`), all complete.

### The behaviour being matched (ground truth, v1.31.0)

`TestTerminateWorkflowOnMessageTooLargeFailure` (`tests/workflow_test.go:993 @ v1.31.0`) starts a run,
polls (starting a WFT), sends a signal **while that WFT is started**, then calls
`RespondWorkflowTaskFailed` with cause `WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE`. The asserted
history is:

```
1 WorkflowExecutionStarted
2 WorkflowTaskScheduled
3 WorkflowTaskStarted
4 WorkflowTaskFailed          <- the started WFT is force-failed (FORCE_CLOSE_COMMAND)
5 WorkflowExecutionSignaled   <- the signal was BUFFERED while the WFT was started, flushed here
6 WorkflowExecutionTerminated
```

Two mechanisms are required and neither exists in the kernel today:

1. **Event buffering.** The signal arriving at step 3-time does **not** land at position 4. It is held
   (buffered) and flushed at position 5, *after* the workflow task closes. Tokeira today appends
   `WorkflowExecutionSignaled` immediately (`apply_signal`, `kernel.rs:662`), producing a different,
   non-conformant ordering. The no-buffering model is a **documented deliberate deviation**
   (`state.rs:187`: "buffered is always false in tokeira, which has no buffered-event model";
   `020-kernel.md:389`). **Adopting buffering reverses that decision.**

2. **Force-close WFT ordering on terminate.** When terminate (or the message-too-large force-close)
   runs while a WFT is started, the started WFT is failed first with cause
   `WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND`, the batch-first-event-id is pinned to that
   `WorkflowTaskFailed` event, buffered events flush, and only then is `WorkflowExecutionTerminated`
   appended (`service/history/workflow/util.go:115` `TerminateWorkflow`, and
   `service/history/api/respondworkflowtaskfailed/api.go:88` @ v1.31.0). Tokeira's current `Terminate`
   (kernel-cancel-terminate Req 3.1) emits only `WorkflowExecutionTerminated` and never fails a started
   WFT.

### Architectural decision required (blocking)

Per the AGENTS change-classification, adopting event buffering is an **Architectural** change (it
reverses a documented design decision and changes observable history ordering for every
event-during-started-WFT path) and requires **spec update AND explicit approval** before implementation.
This document IS that spec update; it does not presume approval. Requirement 0 records the decision and
its blast radius so the owner can accept it explicitly. Nothing below is implemented until Requirement 0
is accepted.

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`). Performs no I/O, async, storage,
  or metrics (AGENTS §2). Buffering is pure state-machine logic and stays within these bounds.
- **Command**: A semantic mutation request delivered to the Kernel (top-level or workflow command).
- **Transition**: The bounded description committed as a result of one `apply` call.
- **WorkflowState**: The compact durable summary of a single run's state.
- **PendingWorkflowTask**: The authoritative record of the run's single outstanding WFT, tracking
  `logical_seq`, `scheduled_event_id`, `started_event_id` (`None` when scheduled-but-not-started), and
  `attempt`.
- **Started WFT**: A `PendingWorkflowTask` whose `started_event_id` is `Some` — a worker holds it and
  its view of history is frozen at `started_event_id`.
- **Buffered event**: A history event whose authoring command was admitted while a WFT was started, held
  in durable state without a history event id and flushed into history when the WFT next closes.
- **WFT close**: Any transition that ends the *started* state of the current WFT — completion
  (`WorkflowTaskCompleted`), failure (`WorkflowTaskFailed`), timeout (`WorkflowTaskTimedOut`), or the
  force-close that terminate performs on a started WFT.
- **Flush**: Draining `buffered_events`, assigning contiguous event ids, reordering per the buffer
  reorder rule, and emitting them into history immediately after the WFT-close event.
- **Force-close**: Failing a started WFT with cause `ForceCloseCommand` as the first step of a
  terminate, so the terminate batch's first event is the resulting `WorkflowTaskFailed`.
- **WorkflowTaskFailedCause**: The kernel enum introduced in Feature 2 (`kernel-wft-failure-timeout`
  Req 1.1a); this feature extends it.

## Requirements

---

## Requirement 0: Architectural Decision — Adopt the Buffered-Event Model

**User Story:** As the Tokeira owner, I want the reversal of the no-buffered-event deviation recorded
explicitly with its blast radius, so that adopting Temporal's buffering is a deliberate accepted
decision and not a silent behaviour change.

#### Acceptance Criteria

1. THE decision to introduce a buffered-event model into `tokeira-kernel` SHALL be recorded as
   superseding the deliberate deviation documented at `crates/tokeira-kernel/src/state.rs:187` and
   `docs/architecture/020-kernel.md:389`.
2. THE decision record SHALL state the blast radius: event ordering changes for every path where an
   event is admitted while a WFT is started (signals, external signal/cancel results, activity and
   child resolutions, Nexus completions), and conformance histories that previously matched tokeira's
   immediate-append ordering will shift.
3. WHEN Requirement 0 is accepted, THE architecture doc (`020-kernel.md`) and the `state.rs` comment
   SHALL be updated to describe the buffered-event model rather than its absence (documentation is part
   of the deliverable, AGENTS §9).
4. UNTIL Requirement 0 is accepted, THE conformance leaves it unblocks SHALL remain classified skips
   with a cited reason (per the conformance no-kernel-additions discipline), not force-passed.

---

## New Types and State

### Requirement 1.1: WorkflowState Buffered-Event Storage

**User Story:** As a Tokeira developer, I want WorkflowState to hold buffered events durably between
transitions, so that an event admitted during a started WFT survives until the WFT closes.

#### Acceptance Criteria

1. THE `WorkflowState` struct SHALL include a `buffered_events` field of type `Vec<BufferedEvent>`
   holding events awaiting flush, in admission order.
2. THE `BufferedEvent` type SHALL carry the event's `HistoryEventKind` (without an assigned
   `event_id`, because ids are assigned at flush time) and any per-event data needed to assign the
   correct attributes on flush.
3. WHEN a new WorkflowState is initialized (via Start), THE `buffered_events` field SHALL be empty.
4. THE `buffered_events` field SHALL serialize and deserialize without loss (round-trip property).
5. FOR ALL closed runs, `buffered_events` SHALL be empty (buffered events are always flushed before, or
   as part of, the closing transition — see Requirement 4).

### Requirement 1.2: WorkflowTaskFailedCause Force-Close and Message-Too-Large Variants

**User Story:** As a Tokeira developer, I want the `WorkflowTaskFailedCause` enum to carry the causes
used by the terminate force-close and the message-too-large path, so that the emitted `WorkflowTaskFailed`
event carries the v1.31.0-correct cause.

#### Acceptance Criteria

1. THE `WorkflowTaskFailedCause` enum (Feature 2, `kernel-wft-failure-timeout` Req 1.1a) SHALL include a
   `ForceCloseCommand` variant, corresponding to
   `WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND = 17`
   (`proto/upstream/temporal/api/enums/v1/failed_cause.proto`).
2. THE `WorkflowTaskFailedCause` enum SHALL include a `GrpcMessageTooLarge` variant, corresponding to
   `WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE = 36`.
3. THE new variants SHALL preserve `Clone, Debug, PartialEq` and remain defined in `tokeira-kernel`.

---

## Buffering Behaviour

### Requirement 2.1: Buffer Eligible Events Admitted During a Started WFT

**User Story:** As a Tokeira developer, I want events admitted while a WFT is started to be buffered
rather than appended, so that the worker's frozen history view is not invalidated mid-task and the
observable ordering matches v1.31.0.

The buffering predicate mirrors `bufferEvent` (`service/history/historybuilder/event_store.go:263 @
v1.31.0`): workflow state-change events, workflow-task events, and events generated directly from a
worker command or protocol message are **never** buffered; other externally-originated events **are**
buffered while a WFT is in flight.

#### Acceptance Criteria

1. WHEN a `Signal` command is admitted for an open run whose `PendingWorkflowTask` is started, THE
   Kernel SHALL append a `WorkflowExecutionSignaled` entry to `buffered_events` INSTEAD OF emitting it
   into history in that transition.
2. WHEN a `Signal` command is admitted for an open run with no started WFT (no pending WFT, or a pending
   WFT that is scheduled-but-not-started), THE Kernel SHALL emit `WorkflowExecutionSignaled` into
   history immediately, exactly as today.
3. WHEN a `Signal` is buffered, THE Kernel SHALL still emit its `RequestDedupeOp` in the admitting
   transition (dedupe is durable at admission time, independent of buffering).
4. WHEN a `Signal` is buffered and no WFT is pending, THE Kernel SHALL NOT be reachable — a started WFT
   is by definition pending; a run with a started WFT always has a `PendingWorkflowTask`. (Guards the
   at-most-one-WFT invariant.)
5. WHEN an event that the predicate classifies as non-bufferable (a workflow state-change event, a
   workflow-task event, or an event generated directly from a worker command/message) is produced while
   a WFT is started, THE Kernel SHALL emit it into history immediately (never buffered).
6. THE set of externally-originated events that SHALL buffer while a WFT is started SHALL, at minimum
   for Phase 1, include `WorkflowExecutionSignaled` and `WorkflowExecutionCancelRequested`; Phase 2
   extends buffering to activity resolutions, child-workflow resolutions, external signal/cancel
   results, and Nexus completions per the full `bufferEvent` predicate (Requirement 7).

### Requirement 2.2: Buffering Does Not Create a Second WFT

**User Story:** As a Tokeira developer, I want buffering to preserve the at-most-one-WFT invariant, so
that a signal flood during a started WFT does not amplify wakeups.

#### Acceptance Criteria

1. WHEN an event is buffered during a started WFT, THE Kernel SHALL NOT schedule a new WFT (the started
   WFT will re-deliver the buffered events after it closes; the existing
   `pre_completion` follow-up-WFT logic covers this on completion).
2. FOR ALL transitions that buffer an event, `dispatch_ops` SHALL contain no `EnqueueWorkflowTask`.

---

## Flush Behaviour

### Requirement 3.1: Flush Buffered Events on WFT Close

**User Story:** As a Tokeira developer, I want buffered events flushed into history immediately after
the workflow-task-close event, so that the run's history matches v1.31.0 and the next WFT delivers them.

#### Acceptance Criteria

1. WHEN a WFT closes (via `WorkflowTaskCompleted`, `WorkflowTaskFailed`, `WorkflowTaskTimedOut`, or the
   terminate force-close of Requirement 5), THE Kernel SHALL flush `buffered_events` into history
   **after** the emitted WFT-close event.
2. WHEN flushing, THE Kernel SHALL assign each flushed event a contiguous `event_id` continuing from the
   WFT-close event's id, preserving the event-id contiguity invariant.
3. WHEN flushing, THE Kernel SHALL clear `buffered_events` to empty in `next_state`.
4. WHEN flushing, THE Kernel SHALL preserve admission order for all Phase-1 buffered event kinds
   (`WorkflowExecutionSignaled`, `WorkflowExecutionCancelRequested`).
5. WHEN there are no buffered events at WFT close, THE flush SHALL be a no-op (no reordering, no
   additional events).
6. WHEN buffered events are flushed on `WorkflowTaskCompleted`, THE existing follow-up-WFT scheduling
   (a new WFT is scheduled because events landed beyond the worker's started view) SHALL still occur, so
   the buffered events are delivered to a worker. This subsumes the current
   `pre_completion_last_event_id > started_event_id` check.

### Requirement 3.2: Buffer Reorder Rule (Phase 2)

**User Story:** As a Tokeira developer, I want flushed buffered events reordered per v1.31.0, so that
asynchronous completion events land after other buffered events.

#### Acceptance Criteria

1. WHEN flushing buffered events that include activity/child/Nexus completion-class events, THE Kernel
   SHALL place those completion-class events **after** the non-completion buffered events, matching
   `reorderBuffer` (`event_store.go:411 @ v1.31.0`).
2. Requirement 3.2 is Phase 2; Phase 1 (signals/cancel-requested only) has no completion-class buffered
   events and therefore preserves plain admission order.

---

## Terminate Force-Close Ordering

### Requirement 4.1: Terminate Fails a Started WFT First

**User Story:** As a Tokeira developer, I want terminate to fail the started WFT before terminating, so
that the history ordering (WorkflowTaskFailed → flushed buffered events → WorkflowExecutionTerminated)
matches v1.31.0.

Ground truth: `TerminateWorkflow` (`service/history/workflow/util.go:115 @ v1.31.0`) calls
`failWorkflowTask(..., FORCE_CLOSE_COMMAND)` when a WFT is started, pins the batch-first-event-id to the
resulting `WorkflowTaskFailed`, then appends `WorkflowExecutionTerminated`.

#### Acceptance Criteria

1. WHEN a `Terminate` command is received for an open run whose `PendingWorkflowTask` is **started**, THE
   Kernel SHALL first emit a `WorkflowTaskFailed` event carrying the pending WFT's `logical_seq`,
   `scheduled_event_id`, `started_event_id`, and cause `ForceCloseCommand`.
2. AFTER emitting the force-close `WorkflowTaskFailed`, THE Kernel SHALL flush `buffered_events`
   (Requirement 3.1) before emitting `WorkflowExecutionTerminated`.
3. AFTER flushing, THE Kernel SHALL emit `WorkflowExecutionTerminated` and close the run with
   `ExecutionStatus::Terminated`, performing the existing terminate cleanup (clear pending WFT, clear
   sticky, clear activities/timers/pending-external maps, emit `ProjectionOp::CloseExecution`).
4. WHEN a `Terminate` command is received for an open run whose pending WFT is **scheduled-but-not-
   started**, THE Kernel SHALL NOT emit a `WorkflowTaskFailed` (there is no started attempt to fail);
   any buffered events (none expected in this state) SHALL still be flushed before termination.
5. WHEN a `Terminate` command is received for an open run with **no** pending WFT, THE behaviour SHALL be
   the existing terminate behaviour (kernel-cancel-terminate Req 3.1) with the buffered-events flush
   (a no-op when empty) added before `WorkflowExecutionTerminated`.
6. THE resulting transition SHALL emit a single `RequestDedupeOp` for the terminate request, unchanged
   from kernel-cancel-terminate Req 3.1.1.

### Requirement 4.2: Message-Too-Large Terminate Command Surface

**User Story:** As a Tokeira developer, I want a kernel-expressible way for the message-too-large path
to force-close-then-terminate, so that the edge `RespondWorkflowTaskFailed` handler can drive it.

The edge `RespondWorkflowTaskFailed` handler with cause `GRPC_MESSAGE_TOO_LARGE` terminates the run
rather than taking the normal WFT-failed retry path
(`service/history/api/respondworkflowtaskfailed/api.go:88 @ v1.31.0`).

#### Acceptance Criteria

1. THE Kernel SHALL provide a command path such that a `RespondWorkflowTaskFailed`-originated request
   carrying cause `GrpcMessageTooLarge` results in the force-close-then-terminate transition of
   Requirement 4.1, with the terminate reason string set to the cause name
   (`"WorkflowTaskFailedCause: GrpcMessageTooLarge"` — match the v1.31.0 `request.GetCause().String()`
   reason) and identity set to the internal history-service identity.
2. THE force-close `WorkflowTaskFailed` emitted on this path SHALL carry cause `ForceCloseCommand` (NOT
   `GrpcMessageTooLarge`); the inbound `GrpcMessageTooLarge` cause selects the terminate route, while the
   emitted WFT-failed event's cause is the force-close cause, per v1.31.0.
3. WHETHER this is a distinct `Command` variant or a flag on the existing terminate/WFT-failed request
   is a design decision (see design.md); the kernel contract is the resulting transition, not the command
   shape.
4. THE fencing preconditions for the message-too-large path SHALL match the WFT-failed fencing (started
   WFT present, `logical_seq` and `started_event_id` match) so a stale task token is rejected.

---

## Reject Taxonomy

### Requirement 5.1: Reject Reuse

**User Story:** As a Tokeira developer, I want the buffered-event and force-close paths to reuse the
existing reject taxonomy, so that no new error surface is introduced without cause.

#### Acceptance Criteria

1. THE message-too-large / force-close path SHALL reuse `MissingRun`, `RunClosed`,
   `NoPendingWorkflowTask`, `WorkflowTaskNotStarted`, `WorkflowTaskSeqMismatch`, and
   `WorkflowTaskTokenMismatch` (Feature 2 taxonomy) for its precondition failures.
2. Buffering a signal SHALL NOT introduce a new reject; a signal that would be buffered is still subject
   to the existing `Signal` rejects (`MissingRun`, `RunClosed`).

---

## Structural Invariants

### Requirement 6.1: Event ID Contiguity Across Buffer and Flush

**User Story:** As a Tokeira developer, I want event-id contiguity to hold across buffering and flushing,
so that history integrity is preserved despite deferred id assignment.

#### Acceptance Criteria

1. FOR ALL transitions that buffer an event, the transition SHALL emit zero history events for the
   buffered command, and `next_state.last_event_id` SHALL be unchanged by the buffering (ids are not
   consumed until flush).
2. FOR ALL flush transitions, the flushed events SHALL receive contiguous ids continuing from the
   WFT-close event, and `next_state.last_event_id` SHALL equal the last flushed (or last emitted) event's
   id.
3. FOR ALL transitions, `next_state.transition_seq` SHALL equal `expected_seq + 1` (buffering and
   flushing each occur within a single transition and increment the fence exactly once).

### Requirement 6.2: At-Most-One-WFT Preserved

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold across buffering,
flushing, and force-close.

#### Acceptance Criteria

1. FOR ALL buffering transitions, `next_state` SHALL contain at most one `PendingWorkflowTask`.
2. FOR ALL terminate force-close transitions, `next_state.pending_workflow_task` SHALL be `None` (the
   run is closed).

### Requirement 6.3: Terminal Cleanliness

**User Story:** As a Tokeira developer, I want closed runs to carry no buffered events.

#### Acceptance Criteria

1. FOR ALL transitions that close a run, `next_state.buffered_events` SHALL be empty.

---

## Property Tests

### Requirement P1: Signal During Started WFT Buffers, Not Appends

1. FOR ALL open WorkflowState with a started WFT and FOR ALL valid `SignalRequest`, WHEN `Signal` is
   applied, THE transition SHALL emit no `WorkflowExecutionSignaled` history event, `next_state.buffered_events`
   SHALL contain the signal, and `next_state.last_event_id` SHALL be unchanged.
   `// Feature: kernel-event-buffering, Property 1`

### Requirement P2: Signal Without a Started WFT Appends Immediately

1. FOR ALL open WorkflowState with no started WFT and FOR ALL valid `SignalRequest`, WHEN `Signal` is
   applied, THE transition SHALL emit exactly one `WorkflowExecutionSignaled` event and
   `next_state.buffered_events` SHALL be empty.
   `// Feature: kernel-event-buffering, Property 2`

### Requirement P3: Flush On WFT Completion Preserves Order and Contiguity

1. FOR ALL open WorkflowState with a started WFT and N buffered events, WHEN `WorkflowTaskCompleted` is
   applied, THE flushed events SHALL appear after `WorkflowTaskCompleted` in admission order with
   contiguous ids, `next_state.buffered_events` SHALL be empty, and a follow-up WFT SHALL be scheduled.
   `// Feature: kernel-event-buffering, Property 3`

### Requirement P4: Terminate Force-Close Ordering

1. FOR ALL open WorkflowState with a started WFT and a single buffered `WorkflowExecutionSignaled`, WHEN
   the terminate force-close path is applied, THE emitted history SHALL be, in order,
   `WorkflowTaskFailed(ForceCloseCommand)`, `WorkflowExecutionSignaled`, `WorkflowExecutionTerminated`,
   with contiguous ids, and `next_state.status` SHALL be `Terminated`.
   `// Feature: kernel-event-buffering, Property 4`

### Requirement P5: Buffered Events Absent From Terminal State

1. FOR ALL transitions that close a run, `next_state.buffered_events` SHALL be empty.
   `// Feature: kernel-event-buffering, Property 5`

---

## Golden Transition Test

### Requirement G1: Message-Too-Large Terminate Golden

1. WHEN the run is started, a WFT is started, a signal is buffered, and the message-too-large
   force-close-terminate path is applied, THE assembled history SHALL exactly equal the v1.31.0 corpus
   assertion:
   `WorkflowExecutionStarted, WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskFailed,
   WorkflowExecutionSignaled, WorkflowExecutionTerminated` (`tests/workflow_test.go:993 @ v1.31.0`).

---

## Out of Scope / Dependencies

- **Edge wiring (dependency, not kernel).** `RespondWorkflowTaskFailed` is a no-op edge stub today; it
  must be wired to submit the message-too-large kernel command (Requirement 4.2) and, for other causes,
  the existing Feature 2 `WorkflowTaskFailed` retry command. This is edge/runtime work tracked under
  `edge-unimplemented.md` / the owning `api-conformance-wft-completion` spec, and depends on this kernel
  feature landing.
- **Full buffering fidelity (Phase 2).** Buffering and reordering of activity/child/Nexus completion
  events (Requirements 2.1.6, 3.2) is deferred to Phase 2. Phase 1 delivers signals/cancel-requested
  buffering, flush-on-close, and terminate force-close — the minimum for the raised conformance leaves.
- **`TestWorkflowRetry` / `TestWorkflowRetryFailures`** are **confirmed out of scope for this spec**:
  both assert plain 5-event per-attempt histories (`tests/workflow_test.go:1440-1520 @ v1.31.0`) with no
  buffered event; the 6-vs-4 delta is the retry-chain / `RespondWorkflowTaskFailed` edge-and-runtime
  path, owned under `api-conformance-wft-completion` / `edge-unimplemented.md`.
