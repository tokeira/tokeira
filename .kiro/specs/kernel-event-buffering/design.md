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
- **Phase 2 (full fidelity, separate PR):** buffer activity/child/Nexus completions; the completion-class
  reorder rule (`reorderBuffer`) and started-id backfill (`wireEventIDs`).

## Components and Interfaces

### State: `WorkflowState.buffered_events`

`WorkflowState` gains `buffered_events: Vec<BufferedEvent>`. `BufferedEvent` wraps a `HistoryEventKind`
(or the minimal per-kind data) **without** an `event_id`, because ids are assigned only at flush. This is
durable state persisting across transitions (the signal arrives in one `apply`; the flush happens in a
later `apply` when the WFT closes) — hence it lives on `WorkflowState`, not the transient
`TransitionBuilder`.

### Predicate: `should_buffer(state, kind) -> bool`

A single helper encoding the `bufferEvent` predicate so every buffer-eligible handler shares one
authority (cite `event_store.go:263 @ v1.31.0`). Phase 1 covers `WorkflowExecutionSignaled` and
`WorkflowExecutionCancelRequested`; Phase 2 extends to resolution/completion events.

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
2. Reorder (Phase 2): stable-partition completion-class events to the end (`reorderBuffer`). Phase 1 is a
   plain drain.
3. For each buffered event in final order, `emit(kind)` — assigns the next contiguous id.
4. Clear `buffered_events`.

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
close(Terminated) + existing cleanup (activities/timers/pending-external, sticky, projection)
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
- **Golden G1 — message-too-large history.** Exactly the `tests/workflow_test.go:993 @ v1.31.0`
  assertion.

**Validates: Requirements 2.1, 2.2, 3.1, 4.1, 6.1, 6.2, 6.3, P1–P5, G1.**

## Error Handling

The message-too-large / force-close path reuses the Feature 2 reject taxonomy (`MissingRun`,
`RunClosed`, `NoPendingWorkflowTask`, `WorkflowTaskNotStarted`, `WorkflowTaskSeqMismatch`,
`WorkflowTaskTokenMismatch`) so a stale worker token is rejected before any mutation. Buffering a signal
introduces no new reject; a buffered signal is still subject to the existing `Signal` rejects
(`MissingRun`, `RunClosed`). No new `Reject` variants are needed.

## Testing Strategy

### Property-Based Tests (proptest)

P1–P5 above, tagged `// Feature: kernel-event-buffering, Property N`, generating open `WorkflowState`
with/without a started WFT and with 0..N buffered events.

### Golden Transition Test

G1: start → start WFT → buffer signal → message-too-large force-close-terminate, asserting the exact
v1.31.0 corpus history.

### Conformance

After the edge dependency lands (edge `RespondWorkflowTaskFailed` wiring, tracked under
`edge-unimplemented.md` / `api-conformance-wft-completion`), remove the
`TestTerminateWorkflowOnMessageTooLargeFailure` skip and confirm green in the harness; re-classify
`TestWorkflowRetry` / `TestWorkflowRetryFailures`.

### Documentation (Requirement 0.3)

On acceptance, update `020-kernel.md` (`Signal` rationale at :389 + a new buffered-events subsection) and
the `state.rs:187` comment to describe the model. Part of the change, not a follow-up (AGENTS §9).
