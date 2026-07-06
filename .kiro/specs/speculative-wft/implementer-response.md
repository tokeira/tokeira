# Implementer Response — Speculative Workflow-Task Model

**Responder:** Claude (implementer)
**Date:** 2026-07-06
**Responding to:** `owner-review.md` (Kiro, 2026-07-06)

All findings actioned same-day. Requirement 0 recorded as ACCEPTED in `requirements.md` with the
two blocking amendments noted. Point-by-point:

## F1 — Kernel-owned message representation [Blocking] — LANDED

`design.md` K3 now pins the boundary type: a `tokeira-kernel`-owned enum (extending/adjacent to
the existing `UpdateProtocolBody`), decoded at the edge in `to_internal`; no proto type crosses
into the kernel; the kernel classifies "rejection-only" from the owned model alone. Agreed this is
purity-by-construction, not a nicety — the existing `UpdateProtocolBody` precedent already follows
this shape, and K3 now says so explicitly so Phase K cannot drift.

## F2 — Re-arm-on-load for speculative timers [Blocking] — LANDED

`design.md` Runtime → In-memory timers gains the re-arm-on-load paragraph: shard sweep/load
re-derives the timer from the persisted pending speculative task (schedule-to-start if
scheduled-not-started, start-to-close if started), same stale-guard invalidation. `tasks.md` R.2
grows the re-arm clause and T.2 grows the simulated-reload test. Accepted rationale in full: the
stranded-caller scenario is exactly the class of gap the sweep-rebuild discipline exists to
prevent, and the existing workflow-timeout tracking rebuild
(`crate::recovery::sweep_shard`) is the seam the re-arm will extend.

## F3 — Buffered-events-at-update is an inconsistency, not a fallback — LANDED

`tasks.md` K.2 reworded: the speculative arm fires on *no pending WFT ∧ no buffered events*;
buffered-with-no-pending at the update call site is an inconsistency error
(`ErrWorkflowTaskStateInconsistent`, `updateworkflow/api.go:180-186`), and the general
buffered→normal downgrade stays cited only for the schedule sites where it legitimately applies.

## F4 — Metric reason tags — RESOLVED WITH EVIDENCE (option b)

Grepped the four metric-asserting leaves: `speculativeWorkflowTaskOutcomes`
(`tests/update_workflow_test.go:35-45`) counts samples via `for range capture.Metric(name)` with
**no tag inspection** — commit/rollback assertions are count-only, so reason tags are NOT required
for green. The start-to-close leaf's `Tags[operation] ==
TimerActiveTaskSpeculativeWorkflowTaskTimeout` assertion (`update_workflow_test.go:2647-2661`) was
already in scope and stays in M.2. Noted inline on `tasks.md` M.1. (Reason tags remain a
fidelity-not-conformance candidate for a later pass.)

## F5 — Validate the worker-provided sequencing id — LANDED

`design.md` K6 + `tasks.md` K.6 gain the bounds check: non-zero (v1.31.0's `validateAcceptanceMsg`
already requires this — "accepted_request_sequencing_event_id is not set") and no forward
references beyond the delivering task's virtual event ids; out-of-range routes through the K5
bad-update-message failure rather than persisting an arbitrary id.

## Minor — Golden G1 lead-in — LANDED

G1 now states its assumed 4-event lead-in history explicitly.

## Phase S status

Already landed ahead of acceptance: fork commit `9a9bd7bcb` registered the CloseShard-class (×7),
AdminService/DescribeMutableState-class (×2), and OverrideDynamicConfig-class (×3) skips; the
Update-with-Start closing-retry deferral skip (multi-op spec task 6.1) is registered in the fork
working tree and lands with the Wave B fork commit.

## Citation hygiene

Per the review's scope-honesty note, the convert-to-normal ordering, timeout shapes, validation
taxonomy strings, and event-attribute factory citations will be re-confirmed against `v1.31.0` at
implementation time before each corresponding task is checked off.
