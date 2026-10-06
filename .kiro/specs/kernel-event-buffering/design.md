# Design Document: Event Buffering and Force-Close WFT Ordering (Kernel)

## Overview

This feature adopts Temporal's **buffered-event model** and the **force-close workflow-task ordering**
that terminate performs when a workflow task is in flight, into `tokeira-kernel`.

Requirements: [requirements.md](./requirements.md). **Blocked on Requirement 0** — the owner must
accept the buffered-event model (an Architectural change per the AGENTS classification, because it
reverses a documented design decision and changes observable history ordering). This design assumes
acceptance; nothing is implemented until then.

Ground truth is v1.31.0 (`TEMPORAL_SERVER_COMPAT`), read from the local `../temporal` checkout and the
vendored protos (AGENTS §8). Tokeira's implementation stays original — this design adopts the observable
contract, not Temporal's Go structures.

### The gap in one picture

Tokeira today, signal during a started WFT (`apply_signal`, `kernel.rs:662`): `WorkflowExecutionSignaled`
is appended **immediately**. Temporal buffers it and flushes it after the WFT closes. For
terminate-on-message-too-large:

```
tokeira today (would be)          v1.31.0 (required)
3 WorkflowTaskStarted             3 WorkflowTaskStarted
4 WorkflowExecutionSignaled       4 WorkflowTaskFailed          (force-close, cause=FORCE_CLOSE_COMMAND)
  (no force-close, no WTFailed)   5 WorkflowExecutionSignaled   (buffered, flushed after WFT close)
  ...                             6 WorkflowExecutionTerminated
```

The no-buffering model is documented as deliberate (`state.rs:187`, `020-kernel.md:389`). This design
reverses that decision (Requirement 0) — the reason this is a spec, not an inline conformance patch.

### Why this is kernel work and is still pure

Buffering, flush ordering, and force-close ordering are pure deterministic state-machine logic: they read
`WorkflowState` and a command and produce a `Transition`. No I/O, async, storage, or metrics — within
AGENTS §2. The "no kernel additions" conformance rule is a *stop-and-raise* signal for leaf fixes, not a
prohibition on deliberate, spec'd kernel features. This is the deliberate, spec'd version.

### Ground-truth anchors

- `bufferEvent` predicate — which event kinds buffer while a WFT is in flight:
  `service/history/historybuilder/event_store.go:263 @ v1.31.0`. Never-buffered: workflow state-change
  events; workflow-task events; events generated directly from a worker command or protocol message.
  Everything else (default) buffers — including `WorkflowExecutionSignaled`.
- `reorderBuffer` — completion-class buffered events sort after the rest: `event_store.go:411 @ v1.31.0`.
- `TerminateWorkflow` — force-close of a started WFT before terminate:
  `service/history/workflow/util.go:115 @ v1.31.0`.
- `RespondWorkflowTaskFailed` message-too-large route:
  `service/history/api/respondworkflowtaskfailed/api.go:88 @ v1.31.0`.
- Cause enum values: `proto/upstream/temporal/api/enums/v1/failed_cause.proto`
  (`FORCE_CLOSE_COMMAND = 17`, `GRPC_MESSAGE_TOO_LARGE = 36`).
- Target corpus assertion: `tests/workflow_test.go:993 @ v1.31.0`.

## Architecture

All changes are additive to the existing kernel state machine, with two exceptions that are the point of
the feature: `apply_signal` stops appending immediately during a started WFT, and the WFT-close sites gain
a flush step. The buffered-event store persists across transitions on `WorkflowState`.

Phasing keeps the change bounded:

- **Phase 1 (unblocks the raised leaves):** buffer `WorkflowExecutionSignaled` /
  `WorkflowExecutionCancelRequested`; flush on WFT close; terminate force-close; new
  `WorkflowTaskFailedCause` variants; the message-too-large command; properties + golden.
- **Phase 2 (full fidelity):** buffer every externally-originated event (Requirement 2.3) through one
  append rule in the transition builder; the completion-class reorder rule (`reorderBuffer`) and
  started-id wiring (`wireEventIDs`) for activities and children. Delivered in two slices: activity
  events first, then child-workflow, external-workflow, Nexus, pause/unpause, and the activity starts
  the runtime commits itself.

### How this maps onto Tokeira's architecture

Tokeira matches v1.31.0's history, not its mechanics. Three differences shape this design:

- **An event gets its id when it is appended.** A Tokeira transition is one pure call,
  `apply(state, command)`, and the transition builder numbers each event as it is appended. v1.31.0
  instead collects external events in a buffer during the request and decides at transaction close: it
  converts a speculative task first, then flushes the buffer if no task is started
  (`closeTransaction` runs `closeTransactionHandleWorkflowTask` before `closeTransactionPrepareEvents`,
  `mutable_state_impl.go:7086-7100`; `closeTransactionHandleSpeculativeWorkflowTask`, `:7238-7251`;
  `Finish(!ms.HasStartedWorkflowTask())`, `:7800 @ v1.31.0`). Tokeira has no step between appending
  and numbering, so the same decision is made when the event is appended, by one append rule in the
  builder.
- **Tokeira buffers only while a task is started.** v1.31.0 routes every external event through the
  buffer and flushes it at transaction close when no task is started; Tokeira appends directly in that
  case. The resulting history is the same, so the classification of Requirement 2.3 only matters while a
  task is started.
- **The runtime commits some activity starts itself.** The activity poll path and the by-id completion
  of a not-yet-started activity commit their own transition (optimistic concurrency plus the shard epoch,
  outside the run's lane) instead of submitting a kernel command. They keep that commit path; what
  changes is that a kernel function computes the transition, so the same append rule and the same checks
  apply to it.

A started speculative task also stays speculative while events buffer against it, where v1.31.0 would
convert it at the next transaction close; `speculative-wft` design ("Reserved ids") explains why and what
that requires of every path that closes such a task.

## Components and Interfaces

### State: `WorkflowState.buffered_events`

`WorkflowState` gains `buffered_events: Vec<BufferedEvent>`. `BufferedEvent` wraps a `HistoryEventKind`
(or the minimal per-kind data) **without** an `event_id`, because ids are assigned only at flush. This is
durable state persisting across transitions (the signal arrives in one `apply`; the flush happens in a
later `apply` when the WFT closes) — hence it lives on `WorkflowState`, not the transient
`TransitionBuilder`.

### Predicate: `should_buffer(state, kind) -> bool`

A single helper encoding the `bufferEvent` classification (Requirement 2.3; cite
`event_store.go:263-318 @ v1.31.0`) so every handler shares one authority. It returns true for an
externally-originated kind while a WFT is started. It is an exhaustive `match` with no wildcard arm, so a
new `HistoryEventKind` does not compile until it is classified.

### One append rule: `TransitionBuilder::append_external`

`emit_or_buffer` becomes the single way an externally-originated event enters a transition:

```
fn append_external(kind) -> event id:
    if should_buffer(state, kind):           // a WFT is started
        push kind onto state.buffered_events
        return BUFFERED_EVENT_ID             // the id is assigned at flush
    materialize_scheduled_speculative()      // no-op unless a speculative WFT is scheduled
    emit(kind)                               // next contiguous id
```

Every kernel site that records an externally-originated event calls it: signal, cancel request, timer
fired, activity started and resolved, child started and resolved, external signal and cancel results,
Nexus started, resolved and cancel-request results, pause, unpause, options update. The sites keep their
surrounding logic (no second WFT while one is pending, the buffered-event limit after a buffer, which
[run-growth-limits](../run-growth-limits/bugfix.md) moves to the end of every transition). Paths
that write the workflow task's own events, command events, update events or run-closing events keep
calling `emit` directly. The per-site `materialize_scheduled_speculative()` calls fold into this rule.

The runtime's two activity-start transitions (the poll path and the by-id completion's start) are
computed by a kernel function built on the same rule. It covers what the runtime does by hand today: a
retry-policy activity records a transient start with no event; any other start appends or buffers
`ActivityTaskStarted`; the activity's stamp and started identity are updated. The runtime then commits
the result exactly as it does now.

### `apply_signal` buffering branch

```
if state.pending_workflow_task is started:
    push WorkflowExecutionSignaled onto state.buffered_events   // no event id
    emit RequestDedupeOp                                        // dedupe still durable at admission
    do NOT schedule a WFT (one is already started)
else:
    emit WorkflowExecutionSignaled into history (today's behaviour)
    schedule a WFT if none pending
```

WHY comment to carry: dedupe ops are emitted at *admission* even when buffered, because idempotency of
`SignalWorkflowExecution` is anchored to the request id at durable acceptance, not to eventual history
position.

### `TransitionBuilder::flush_buffered()`

Called at each WFT-close site:

1. If `buffered_events` is empty, return (no-op).
2. Reorder: stable-partition the completion-class events (Requirement 3.2: activity, child-workflow and
   Nexus completions) to the end (`reorderBuffer`, `event_store.go:413-443 @ v1.31.0`).
3. For each buffered event in final order, assign the next contiguous id. A flushed
   `ActivityTaskStarted` or `ChildWorkflowExecutionStarted` records its real id against the activity's
   scheduled id or the child's initiated id; a later completion in the batch for the same activity or
   child takes that id as its started event id; a still-pending activity or child gets it in
   `WorkflowState` (`wireEventIDs`, `event_store.go:339-406`; `updatePendingEventIDs`,
   `mutable_state_impl.go:7957-7981 @ v1.31.0`). Completions come after their started events because of
   step 2, so one pass suffices.
4. Clear `buffered_events`.

While a child's started event is buffered, `ChildWorkflowState.started_event_id` holds
`BUFFERED_EVENT_ID`, exactly as an activity's does; `apply_child_resolved` copies whatever the state
holds, so a completion buffered in that window carries the sentinel until the flush replaces it.

Call sites: `apply_workflow_task_completed` (after `WorkflowTaskCompleted`), `apply_workflow_task_failed`
and `apply_workflow_task_timed_out` (Feature 2 retry path, after the close event), and the terminate
force-close.

`apply_workflow_task_completed` follow-up-WFT scheduling: the current
`pre_completion_last_event_id > started_event_id` numeric check (kernel.rs:1497) is **replaced** by
"schedule a follow-up WFT if any events were flushed or `force_new_workflow_task`", because buffered
events no longer advance `last_event_id` before completion.

### Terminate force-close (`apply_terminate`)

```
emit RequestDedupeOp
if pending_workflow_task is started:
    emit WorkflowTaskFailed { logical_seq, scheduled_event_id, started_event_id,
                              cause: ForceCloseCommand }   // batch-first event
flush_buffered()
emit WorkflowExecutionTerminated { reason, details, identity }
close(Terminated) + existing cleanup (timers/pending-external, sticky, projection; activities stay)
```

### Message-too-large command (design decision)

Two viable command shapes:

- (a) A dedicated `Command::TerminateOnWorkflowTaskFailed(..)` carrying WFT fencing + cause.
- (b) A flag/variant on the existing WFT-failed request selecting the terminate route when
  `cause == GrpcMessageTooLarge`.

**Recommendation: (a).** It keeps `apply_workflow_task_failed` (retry path) unpolluted and makes the
force-close-terminate an explicit, testable transition. The edge `RespondWorkflowTaskFailed` handler
inspects the cause: `GrpcMessageTooLarge` → (a); every other cause → the Feature 2 retry command. The
emitted `WorkflowTaskFailed` on route (a) carries `ForceCloseCommand` (Req 4.2.2); the inbound
`GrpcMessageTooLarge` only selects the route. Terminate reason = inbound cause name
(`request.GetCause().String()` @ v1.31.0); identity = internal history-service identity.

## Data Models

- `WorkflowState.buffered_events: Vec<BufferedEvent>` (new field; empty on Start; empty for closed runs).
- `BufferedEvent` (new type; `HistoryEventKind` without an id; `Clone, Debug, PartialEq, Serialize,
  Deserialize`).
- `WorkflowTaskFailedCause::{ForceCloseCommand, GrpcMessageTooLarge}` (extends the Feature 2 enum).
- The message-too-large command variant (route (a)) carrying WFT fencing (`logical_seq`,
  `started_event_id`) + the terminate reason/identity.

No other state types change.

## Correctness Properties

*A property is a characteristic that should hold across all valid executions.*

- **P1 — Buffer, not append.** Signal during a started WFT emits no `WorkflowExecutionSignaled`,
  buffers it, and leaves `last_event_id` unchanged. (Req 2.1, P1)
- **P2 — Immediate append without a started WFT.** Signal with no started WFT emits exactly one
  `WorkflowExecutionSignaled` and buffers nothing. (Req 2.1, P2)
- **P3 — Flush order + contiguity on completion.** On `WorkflowTaskCompleted`, N buffered events flush in
  admission order with contiguous ids after the close event, `buffered_events` empties, and a follow-up
  WFT is scheduled. (Req 3.1, P3)
- **P4 — Terminate force-close ordering.** Started WFT + one buffered signal terminates as
  `WorkflowTaskFailed(ForceCloseCommand)`, `WorkflowExecutionSignaled`, `WorkflowExecutionTerminated`,
  contiguous, status `Terminated`. (Req 4.1, P4)
- **P5 — Terminal cleanliness.** Closed runs carry empty `buffered_events`. (Req 6.3, P5)
- **P6 — Every externally-originated event buffers during a started WFT.** For a started WFT of any mode
  and any command recording an externally-originated kind, nothing is appended, the event is buffered,
  and `last_event_id` is unchanged. (Req 2.1, 2.3, 2.4, P6)
- **P7 — Completion-class events flush last.** Any buffered batch flushes as non-completion events in
  admission order, then completion-class events in admission order, contiguous. (Req 3.2, P7)
- **P8 — Started ids are wired.** A flushed completion names the real id of the started event flushed
  with it; no flushed event carries the sentinel. (Req 3.3, P8)
- **P9 — Started-task history is frozen.** Over any command sequence, a transition across which the same
  WFT stays started appends nothing. (Req 6.4, P9)
- **Golden G1 — message-too-large history.** Exactly the `tests/workflow_test.go:993 @ v1.31.0`
  assertion.
- **Golden G2 — child completion during a started WFT.** `WorkflowTaskCompleted`,
  `ChildWorkflowExecutionCompleted`, `WorkflowTaskScheduled`. (Req G2)
- **Golden G3 — child start and completion buffered together.** `ChildWorkflowExecutionStarted`,
  `WorkflowExecutionSignaled`, `ChildWorkflowExecutionCompleted` naming the flushed start. (Req G3)

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 4.1, 6.1, 6.2, 6.3, 6.4, P1–P9, G1–G3.**

## Error Handling

The message-too-large / force-close path reuses the Feature 2 reject taxonomy (`MissingRun`,
`RunClosed`, `NoPendingWorkflowTask`, `WorkflowTaskNotStarted`, `WorkflowTaskSeqMismatch`,
`WorkflowTaskTokenMismatch`) so a stale worker token is rejected before any mutation. Buffering a signal
introduces no new reject; a buffered signal is still subject to the existing `Signal` rejects
(`MissingRun`, `RunClosed`).

A transition that appends history while the same WFT stays started (Requirement 6.4) is rejected by the
transition check that `speculative-wft` design ("Transition check") defines; it is the only new
rejection, and no correct path reaches it.

## Testing Strategy

### Property-Based Tests (proptest)

P1–P9 above, tagged `// Feature: kernel-event-buffering, Property N`, generating open `WorkflowState`
with/without a started WFT and with 0..N buffered events. P6 draws the started WFT from all three modes
and the command from every externally-originated kind; P9 drives interleaved sequences of updates,
workflow-task starts and completions, and every externally-originated command.

### Golden Transition Tests

G1: start → start WFT → buffer signal → message-too-large force-close-terminate, asserting the exact
v1.31.0 corpus history. G2 and G3: the child-workflow shapes of Requirements G2 and G3.

### Conformance

After the edge dependency lands (edge `RespondWorkflowTaskFailed` wiring, tracked under
`edge-unimplemented.md` / `api-conformance-wft-completion`), remove the
`TestTerminateWorkflowOnMessageTooLargeFailure` skip and confirm green in the harness; re-classify
`TestWorkflowRetry` / `TestWorkflowRetryFailures`.

The child-workflow, Nexus, external signal/cancel, pause and buffered-events suites are rerun after the
remaining kinds start buffering: their histories shift to v1.31.0's buffered ordering wherever an event
arrived while a workflow task was started (the blast radius Requirement 0.2 accepted).

### Documentation (Requirement 0.3)

On acceptance, update `020-kernel.md` (`Signal` rationale at :389 + a new buffered-events subsection) and
the `state.rs:187` comment to describe the model. Part of the change, not a follow-up (AGENTS §9). The
buffered-events subsection's predicate paragraph states the full classification, the append rule and the
child started-id wiring once the remaining kinds land.
