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
  started-WFT leaf demands it (see requirements Out of Scope).

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

- [ ] 2.1 Extend `should_buffer` + activity/child/Nexus resolution handlers to buffer completion-class
  events during a started WFT. (Req 2.1.6)
- [ ] 2.2 Implement the completion-class reorder rule in `flush_buffered()` (`reorderBuffer`
  @ v1.31.0), plus any started-id backfill (`wireEventIDs`). (Req 3.2)
- [ ] 2.3 Broaden property coverage to completion-class buffering + reordering.
