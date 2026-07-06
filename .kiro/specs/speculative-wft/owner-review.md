# Owner Review — Speculative Workflow-Task Model

**Reviewer:** Kiro (owner-side review)
**Date:** 2026-07-06
**Artifact reviewed:** `.kiro/specs/speculative-wft/{requirements,design,tasks}.md` (Req 0 = PROPOSED)
**Ground truth:** Temporal `v1.31.0` (`TEMPORAL_SERVER_COMPAT = 1.31.0`), read from the local
`../temporal` checkout at tag `v1.31.0` per AGENTS §8. This review is read-only — no spec, kernel,
or worktree files were modified.

---

## Verdict

**Accept Requirement 0**, contingent on two small design deltas landing **before** Phase K work
begins (F1 and F2 below). Neither changes the model; each closes a place where an implementer could
accidentally violate kernel purity (F1) or strand a caller across a crash (F2).

Phase S (fork-side skip hygiene) is independent and should proceed **immediately** — it is a
prerequisite for a clean first suite run regardless of the Req 0 decision.

This is an unusually disciplined, high-fidelity raise. Every load-bearing ground-truth anchor I
spot-checked matched the tagged source exactly; the kernel/runtime/edge split respects kernel
purity; and the postcard append-only discipline is applied correctly.

---

## Ground-truth verification (v1.31.0)

I confirmed the six most load-bearing claims read the tagged source correctly (verbatim, not
paraphrased). All passed.

| Claim | Source (`v1.31.0`) | Result |
|---|---|---|
| Drop predicate: SPECULATIVE-only ∧ no commands ∧ not `ForceCreateNewWorkflowTask` ∧ events-window ∧ every message a `Rejection` (empty ≡ all-rejections) | `service/history/workflow/workflow_task_state_machine.go:676` (`skipWorkflowTaskCompletedEvent`) | ✅ exact |
| Events-window: `NextEventID > LastCompleted+2` without capability; `+2+Discard…Count()` with it | same file, `:725` | ✅ exact |
| `DiscardSpeculativeWorkflowTaskMaximumEventsCount` default | `common/dynamicconfig/constants.go:2447` → literal `10` | ✅ exact |
| `SpeculativeWorkflowTaskScheduleToStartTimeout` | `service/history/tasks/workflow_task_timer.go:18` → `5 * time.Second` | ✅ exact |
| `ResetHistoryEventId = LastCompletedWorkflowTaskStartedEventId` iff `completedEvent == nil`, else 0 | `service/history/api/respondworkflowtaskcompleted/api.go:770` | ✅ exact |
| Creation `AddWorkflowTaskScheduledEvent(false, SPECULATIVE)`; noop-attach on `alreadyExisted \|\| HasPendingWorkflowTask`; `OnSuccess` direct-to-matching sticky-first, `StickyWorkerUnavailable`→normal, **log-only** on other errors | `service/history/api/updateworkflow/api.go:176`, `:216`+ | ✅ exact |
| Metric names `speculative_workflow_task_commits` / `_rollbacks` | `common/metrics/metric_defs.go:940-941` | ✅ exact |

Given this hit rate, I'm treating the remaining ~35 citations as trustworthy without exhaustive
re-verification. (Scope honesty: I did **not** independently re-verify the convert-to-normal
ordering at `:1466-1530`, the timeout shapes at `:270-306`/`:934-990`, the validation-taxonomy
strings, or the event-attribute factory — Claude should keep those cited and re-confirm at
implementation time.)

**Bonus finding from reading the drop predicate** — the commit counter is recorded with a
`ReasonTag`, and the tag set is: `worker_returned_commands`, `force_create_task`,
`interleaved_events`, `too_many_interleaved_events`, `update_accepted`; the rollback counter carries
no tag. See F4.

---

## Strengths (keep these)

- **Req 0 as a blocking Architectural gate is correct** — this changes the kernel WFT model,
  `PendingWorkflowTask`, and two persisted enums. "No kernel work until accepted" matches the AGENTS
  change classification.
- **The existence-bit rationale (K1) is provably necessary, not stylistic.** The drop decision
  switches on `workflowTaskType != SPECULATIVE`; speculative is attempt 1, so the transient
  `attempt > 1` predicate genuinely cannot classify it. Confirmed against `:677`.
- **Append-only postcard discipline (K5/K6) is handled right** — cause variant appended last; a
  *new* failure-capable `WorkflowExecutionUpdateCompleted` variant with the old one decode-only,
  rather than an in-place shape change. The "`cargo test --workspace` after every enum/event change"
  reminder is the correct guardrail.
- **Reuse-not-fork of the transient machinery** (one predicate, three modes; Invariant I.2) keeps
  the blast radius contained.
- **Phase S before the first full run** — unregistered nil-panic leaves would mask the whole
  parallel suite; sequencing it first is correct operational discipline.

---

## Findings

Ordered by weight. Each has an explicit **Ask**. Please respond point-by-point.

### F1 — [Blocking for Phase K] Pin the kernel-side message representation (K3)

**What.** The design correctly observes that the completion command set alone cannot distinguish
"only rejections" from "no messages processed," so the wire-message taxonomy must reach the kernel
(today rejections/messages are dropped edge-side at `to_internal.rs:398-416`). But K3 does not pin
*what type* crosses the boundary.

**Why it matters.** This is the single largest interface change in the spec and the most likely
place for edge/proto concepts to leak into the pure kernel (AGENTS §2). The kernel must stay a
deterministic `state + command → Transition` machine with no proto dependency.

**Ask.** Before Phase K, add a design note to K3 specifying a **kernel-owned** message value type
(a `tokeira-kernel` enum such as `UpdateMessage::{Acceptance, Response, Rejection}` decoded at the
edge in `to_internal`), not an upstream/proto type crossing the boundary. State explicitly that the
kernel classifies "rejection-only" from this owned model, and that the edge is responsible for the
decode. This is a purity-by-construction requirement, not a nicety.

### F2 — [Blocking for Phase K] Specify re-arm-on-load for the speculative task + in-memory timers (R2, Invariant I.1)

**What.** A pending speculative WFT lives in mutable state with **no persisted events**, and its
STS / start-to-close timers are **in-memory** (no durable rows) — faithful to v1.31.0. But the
recovery story is only gestured at ("stale-guard analogue of `CheckSpeculativeWorkflowTaskTimeoutTask`").

**Why it matters.** On a bundle/shard reload after a node crash, the speculative pending task
survives in state but the in-memory timer does not. Without an explicit re-derivation, the update
caller can be stranded (no dispatch, no timeout, no completion). This is exactly the kind of
correctness gap that "history is authority; queues are disposable" is supposed to make impossible —
but only if the timer is *re-derived from the authoritative state on load*.

**Ask.** Add an R2 design note describing the **re-arm-on-load** behavior: when a bundle/shard
reloads mutable state carrying a pending speculative WFT, the runtime re-derives the appropriate
in-memory timer (STS if scheduled-not-started, start-to-close if started) from the persisted
attempt/timestamps, with the same stale-guard invalidation. Add a property/runtime test to T.2:
"speculative task + timer survive a simulated reload and still dispatch/time-out correctly."

### F3 — [Precision] Req 1.2's "buffered events → normal" is a should-never-happen at the update call site

**What.** The general downgrade rule (`AddWorkflowTaskScheduledEvent`, ~`:329`) is real, but in
`updateworkflow/api.go` a non-speculative result is treated as `ErrWorkflowTaskStateInconsistent`
("no pending WFT ⇒ no buffered events, therefore this should never happen", `:180-186`). Req 1.2 /
K.2 read as if buffered-events-at-update is a graceful normal-WFT fallback.

**Why it matters.** An implementer following Req 1.2 literally might build a fallback path that
v1.31.0 treats as an invariant violation, masking a real inconsistency.

**Ask.** Tighten K.2: the speculative arm fires on *no pending WFT ∧ no buffered events*; the
buffered-with-no-pending case is an **inconsistency error**, not a normal-WFT fallback. Keep the
general downgrade rule cited for the *other* schedule sites where it legitimately applies.

### F4 — [Completeness] Metric reason tags may be asserted by the gated leaves (M.1, Req 10.1)

**What.** The v1.31.0 commit counter is recorded with `ReasonTag(...)`; tag set (confirmed at
`workflow_task_state_machine.go:684-733`): `worker_returned_commands`, `force_create_task`,
`interleaved_events`, `too_many_interleaved_events`, `update_accepted`. Req 10.1 / M.1 mention the
counters but not the tags.

**Why it matters.** The metric-asserting leaves (`TestEmptySpeculativeWorkflowTask_AcceptComplete`
×2, `TestRunningWorkflowTask_NewEmptySpeculativeWorkflowTask_Rejected`,
`TestSpeculativeWorkflowTask_StartToCloseTimeout`) may assert the reason tag, not just the count. If
so, M.1 must emit the matching tags or the leaves stay red.

**Ask.** Before Phase M, grep the four metric-asserting leaves for `ReasonTag` / tag assertions and
either (a) add the tag set to M.1's scope, or (b) note in M.1 that the leaves assert count-only.

### F5 — [Robustness] Validate the worker-provided sequencing id (Req 7.1, K.6)

**What.** Dropping the hardcoded `0` (`kernel.rs:4129`) in favor of the worker-provided
`accepted_request_sequencing_event_id` is correct, but the value is now attacker/bug-controlled and
lands in a persisted event.

**Why it matters.** A buggy or hostile worker shouldn't be able to write an arbitrary/out-of-range
sequencing id into authoritative history.

**Ask.** Add a bounds/consistency check to K.6 (e.g. the sequencing id must reference a valid prior
event id for the run) and cite where v1.31.0 validates it, if it does.

---

## Minor

- **Golden G1** event ids (Scheduled 5 / Started 6 / …) assume a specific lead-in history. Add a
  one-line note of the assumed prior events so the golden isn't brittle to unrelated event-numbering
  changes elsewhere.
- **Docs (D.1)** are correctly scoped as part of the change per AGENTS §9 (the three-mode WFT table
  in `020-kernel.md`, the appended cause/variant + wire-message model in `command-surface.md`) —
  good that they're a task, not an afterthought. No change requested.

---

## Suggested spec edits (if you want them pre-drafted)

If it's useful, the two blocking deltas map to:

- **design.md → "Kernel Decisions" → K3**: append a paragraph pinning the kernel-owned
  `UpdateMessage` model and the edge-decode responsibility (F1).
- **design.md → "Components and Interfaces" → Runtime → In-memory timers**: append the
  re-arm-on-load paragraph (F2), and **tasks.md → R.2** grows a re-arm clause + **T.2** grows a
  reload test.
- **tasks.md → K.2**: reword the buffered-events clause per F3.

I can draft any of these as a diff on request — but since this is your spec-acceptance decision and
Claude's implementation, I've left the spec untouched and captured everything as feedback here.

---

## Summary for the implementer

- The model is sound and the ground truth is accurate — proceed.
- Land **Phase S now**.
- Before **Phase K**: resolve **F1** (kernel-owned message model) and **F2** (re-arm-on-load), then
  record Req 0 acceptance in `requirements.md` with the date and these two amendments noted.
- Carry **F3–F5** into the relevant task items; they're precision/robustness, not blockers.
- Keep every behavioral claim cited to `v1.31.0` and re-confirm the un-re-verified citations
  (convert ordering, timeout shapes, taxonomy strings, event factory) at implementation time.
