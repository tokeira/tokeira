# Tasks: Event Buffering and Force-Close WFT Ordering (Kernel)

Requirements: [requirements.md](./requirements.md). Design: [design.md](./design.md).

> **Blocked on Requirement 0.** Do not start Phase 1 until the owner accepts the buffered-event model
> (Architectural change, AGENTS classification). Until then the conformance leaves stay classified skips.

All kernel edits stay pure (AGENTS §2). Ground-truth every behaviour to v1.31.0 and cite the source in
code comments (AGENTS §8, §9). Verify with `cargo clippy -p tokeira-kernel --all-targets --tests -- -D
warnings`, `cargo +nightly fmt`, and `cargo test -p tokeira-kernel`.

## Phase 0 — Decision

- [x] 0.1 Record acceptance of Requirement 0 (buffered-event model supersedes the no-buffering
  deviation). **Accepted for Phase 1 (2026-07-01, owner).** Phase 2 deferred until a completion-during-
  started-WFT case demanded it (see Phase 2 below).

## Phase 1 — Minimum conformant buffering + terminate force-close

- [x] 1.1 Add `buffered_events: Vec<BufferedEvent>` to `WorkflowState` and the `BufferedEvent` type
  (`state.rs`). Initialize empty on Start. Add serde round-trip property coverage. (Req 1.1)
- [x] 1.2 Add `WorkflowTaskFailedCause::{ForceCloseCommand, GrpcMessageTooLarge}` and confirm the
  values against `failed_cause.proto` (`17`, `36`). (Req 1.2)
- [x] 1.3 Add a `should_buffer(state, kind) -> bool` helper encoding the `bufferEvent` predicate
  (`event_store.go:263 @ v1.31.0`), scoped Phase 1 to `WorkflowExecutionSignaled` /
  `WorkflowExecutionCancelRequested`. Cite the source. (Req 2.1)
- [x] 1.4 Rework `apply_signal` (`kernel.rs:655`) to buffer during a started WFT (push to
  `buffered_events`, still emit `RequestDedupeOp`, do not schedule a WFT) and append immediately
  otherwise. WHY-comment the admission-time dedupe. (Req 2.1, 2.2)
- [x] 1.5 Add `TransitionBuilder::flush_buffered()` (drain → assign contiguous ids → clear; Phase-1
  plain order). (Req 3.1)
- [x] 1.6 Wire `flush_buffered()` into `apply_workflow_task_completed`; replace the
  `pre_completion_last_event_id > started_event_id` follow-up check with "schedule a follow-up WFT if
  events were flushed or `force_new_workflow_task`". (Req 3.1)
- [x] 1.7 Wire `flush_buffered()` into `apply_workflow_task_failed` and
  `apply_workflow_task_timed_out` (Feature 2 retry path). (Req 3.1)
- [x] 1.8 Add the terminate force-close branch to `apply_terminate`: emit
  `WorkflowTaskFailed(ForceCloseCommand)` when the WFT is started, `flush_buffered()`, then
  `WorkflowExecutionTerminated` + existing cleanup. (Req 4.1)
- [x] 1.9 Add the message-too-large command path (design §7 route (a)): a dedicated command that carries
  WFT fencing + drives the Requirement 4.1 transition, emitting `ForceCloseCommand` and using the cause
  name as the terminate reason. Reuse Feature 2 fencing rejects. (Req 4.2, 5.1)
- [x] 1.10 Properties P1–P5 (`// Feature: kernel-event-buffering, Property N`). (Req P1–P5)
- [x] 1.11 Golden G1: the exact `tests/workflow_test.go:993 @ v1.31.0` history. (Req G1)
- [x] 1.12 Update `020-kernel.md` (`Signal` rationale + new buffered-events subsection) and the
  `state.rs:187` comment to describe the model. (Req 0.3)

## Phase 1 — Edge dependency (separate, owned elsewhere)

- [x] 1.13 Wire the edge `RespondWorkflowTaskFailed` handler: `GrpcMessageTooLarge` → the message-too-
  large kernel command (1.9); all other causes → the Feature 2 `WorkflowTaskFailed` retry command.
  Tracked under `edge-unimplemented.md` / `api-conformance-wft-completion`. Depends on Phase 1 landing.
- [x] 1.14 (adapted) No skip existed to remove — the leaf was a live FAIL, not a registry entry; with
  Phase 1 + the edge wiring it goes GREEN in the harness (verified 2026-07-02, out-of-process run:
  `TestTerminateWorkflowOnMessageTooLargeFailure` PASS, suite 20 PASS / 12 FAIL / 2 SKIP). Re-classify of
  `TestWorkflowRetry` / `TestWorkflowRetryFailures`: still FAIL after 1.13 — confirmed the retry-chain
  gap (`api-conformance-wft-completion` / `edge-unimplemented.md`), not buffering; no further kernel work
  from this spec.

## Phase 2 — Full buffering fidelity (separate PR)

> Trigger met 2026-07-07 by Tier 2.13 (`TestWorkflowBufferedEventsTestSuite` +
> `TestMaxBufferedEventSuite`). Landed as the **activity slice** — the first
> completion-during-started-WFT leaves demand activity buffering + reorder +
> the count limit. Child-workflow, external-workflow, Nexus and pause events
> are the second slice (tasks 2.4–2.10).

- [x] 2.1 Extend `should_buffer` + activity resolution handlers to buffer
  completion-class events during a started WFT **(DONE — activity started +
  resolutions + WorkflowExecutionOptionsUpdated added to `should_buffer`;
  emits routed through `emit_or_buffer`; the runtime's direct-construct
  activity-start path (`start_activity_task`, activity.rs) also buffers with
  the shared `BUFFERED_EVENT_ID` sentinel when a WFT is started)**. (Req 2.1.6)
- [x] 2.2 Implement the completion-class reorder rule in `flush_buffered()`
  **(DONE — `reorderBuffer` stable-partition (resolutions last) +
  `wireEventIDs` started-id backfill keyed on the shared scheduled id, with a
  durable back-fill of the activity's started id for a resolution arriving
  after flush; OptionsUpdated RequestIdInfo `buffered:true,event_id:0` →
  real-id flip)**. (Req 3.2)
- [x] 2.3 **(DONE — count-limit force-close (`enforce_buffered_event_limit`,
  `MAX_BUFFERED_EVENTS`=100 → `FORCE_CLOSE_COMMAND` fail + reschedule) landed;
  corpus (TestBufferedEvents / TestBufferedEventsOutOfOrder /
  TestRateLimitBufferedEvents / TestMaxBufferedEventsLimit) is the byte-stable
  reorder+wire+limit verification, 3× stress. Dedicated kernel goldens for the
  activity-reorder spine + a broadened property remain a follow-up nicety.)**
  Broaden property coverage to completion-class buffering + reordering.

> Not implemented: a mutable-state SIZE-limit terminate —
> `TestBufferedEventsMutableStateSizeLimit` needs
> `OverrideDynamicConfig(MutableStateSizeLimitError=410KB)`, undeliverable
> out-of-process, so it is a registered OverrideDynamicConfig-class skip.

### Phase 2, second slice — the remaining externally-originated events

> Trigger: GitHub issue #235. A child workflow's result, recorded while an
> update's speculative workflow task was started, took the task's reserved
> event id: the worker could receive two events numbered 42, or never be given
> the child's result. While a task holds reserved ids, an event that is not
> buffered corrupts the worker's history rather than only reordering it. This
> slice lands in one change with `speculative-wft` Phase V, whose exploration
> tests (V.1) come first and fail until both land.

- [ ] 2.4 Classify every `HistoryEventKind` in `should_buffer` per Requirement
  2.3, as an exhaustive match with no wildcard arm; cite `bufferEvent`
  (`event_store.go:263-318 @ v1.31.0`). (Req 2.1.6, 2.3)
- [ ] 2.5 Replace `emit_or_buffer` with `TransitionBuilder::append_external`
  (buffer while a WFT is started; otherwise convert a scheduled speculative
  WFT, then append) and route every externally-originated kernel site through
  it: child started and resolved, external signal and cancel results, Nexus
  started, resolved and cancel-request results, pause, unpause. Move the
  signal, cancel, timer, activity and options sites onto the same rule.
  (Req 2.1, 2.4)
- [ ] 2.6 Child started-id wiring: the sentinel while
  `ChildWorkflowExecutionStarted` is buffered; the flush back-fills the pending
  child's started id and patches flushed child completions (keyed by initiated
  id); child and Nexus completions join the completion class. (Req 3.2, 3.3)
- [ ] 2.7 Runtime activity starts: compute the poll-path start and the by-id
  completion's start with a kernel function built on the append rule (covering
  the retry-policy transient start and the stamp and identity updates the
  runtime does by hand today); keep the runtime's commit path. (Req 2.4)
- [ ] 2.8 Properties P6–P9 (`// Feature: kernel-event-buffering, Property N`)
  and goldens G2, G3. (Req P6–P9, G2, G3)
- [ ] 2.9 Checkpoint: the full bar; conformance rerun of the child-workflow,
  Nexus, external signal/cancel, pause and buffered-events suites — shifted
  histories must match v1.31.0. (Req 0.2)
- [ ] 2.10 Update `020-kernel.md` (the predicate paragraph: full
  classification, the append rule, child started-id wiring) and the
  `should_buffer` / `is_buffered_resolution_class` comments. (Req 0.3)
