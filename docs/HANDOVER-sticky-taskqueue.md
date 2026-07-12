# Hand-over — sticky task-queue fidelity (Tier 1.4: TestStickyTqTestSuite)

**Author:** Claude (raise, functional-conformance drive) · **Date:** 2026-07-03 · **For:** Kiro / owner
**Status:** 🙋 RAISE — kernel changes requested (Requirement 0 pattern, same protocol as
`HANDOVER-activity-kernel-gaps.md`). No kernel code touched. Baseline: **0 pass / 2 fail** —
both leaves fail at the same assertion ("Workflow task not timed out", stickytq_test.go:156/:346).

> **TL;DR.** Tokeira models sticky as a *worker-identity hint on the normal queue* with a silent
> broker-side promotion window. Temporal's sticky is a **distinct task queue** with a per-dispatch
> **schedule-to-start timeout** that, on firing, writes a real `WorkflowTaskTimedOut
> {TimeoutType: SCHEDULE_TO_START}` event, clears stickiness, and reschedules attempt-1 on the
> normal queue. Both corpus leaves wait for exactly that event in history; today it can never
> appear. Four kernel items (S1–S4) + a command variant for `ResetStickyTaskQueue` (S5); scanner,
> broker, and edge work is non-kernel and mine.

## 1. The leaves (ground truth)

`tests/stickytq_test.go @ v1.31.0` — two leaves, same skeleton:

- **TestStickyTimeoutNonTransientWorkflowTask** (:29): WFT completes with
  `StickyAttributes{worker_task_queue: "<tl>-sticky", schedule_to_start_timeout: 2s}`; a signal
  schedules the next WFT — which must go to the STICKY queue; nobody polls it; after 2s history
  must show `7 WorkflowTaskScheduled, 8 WorkflowTaskTimedOut {"TimeoutType":2}, 9
  WorkflowTaskScheduled` (:144-152); the worker then polls the NORMAL queue and gets attempt 1
  (`WithExpectedAttemptCount(1)`), fails 5×, with a mid-chain signal exercising the
  flush-resets-attempt conversion (Tier-1.2 machinery, already landed) — final history :201-223.
- **TestStickyTaskqueueResetThenTimeout** (:226): identical, but calls **`ResetStickyTaskQueue`**
  between the signal and the timeout window (:319-324) — and the S2S timeout **still fires**
  with the identical history. Consequence: the S2S deadline belongs to the *dispatched task*
  (captured at schedule time), not to live sticky state; reset clears affinity only.

## 2. v1.31.0 mechanics (verified)

- **Sticky state**: execution info stores the sticky queue name + `StickyScheduleToStartTimeout`
  (set by `RespondWorkflowTaskCompleted.sticky_attributes`; cleared by `ClearStickyTaskQueue`).
  The next WFT is scheduled on `CurrentTaskQueue()` — the sticky queue while set — and only a
  sticky-queue dispatch gets a schedule-to-start timer (normal WFTs have no S2S deadline).
- **S2S firing** (`timer_queue_active_task_executor.go:439-457`): only if the task is still
  unstarted (`StartedEventID == EmptyEventID`); calls `AddWorkflowTaskScheduleToStartTimeoutEvent`
  (`workflow_task_state_machine.go:270-305`): emits `WorkflowTaskTimedOut(scheduledEventID,
  EmptyEventID, SCHEDULE_TO_START)` then `ApplyWorkflowTaskTimedOutEvent(SCHEDULE_TO_START)`
  (:263-268) → `failWorkflowTask(incrementAttempt=false)` — comment: "Do not increment workflow
  task attempt in the case of sticky timeout to prevent creating next workflow task as
  transient"; then `scheduleWorkflowTask=true` → fresh real `WorkflowTaskScheduled` (now on the
  normal queue, sticky having been cleared).
- **`failWorkflowTask`** (`workflow_task_state_machine.go:1003-1016`): for ANY WFT failure or
  start-to-close timeout, if the sticky queue is set → `ClearStickyTaskQueue()` AND
  `incrementAttempt = false` ("clear sticky task queue first and try again before creating
  transient workflow task").
- **`ResetStickyTaskQueue`**: clears sticky in mutable state; the already-dispatched sticky task
  and its timer are untouched — the timer still fires (its guard checks only scheduled-id match +
  unstarted).

## 3. What tokeira has today (the divergence)

- `StickyAffinity { worker_identity, expires_at }` (`state.rs:64`) — **no sticky queue name, no
  S2S timeout**. The edge validates `sticky_attributes.worker_task_queue.name`
  (`translate.rs:1161-1182`) then **discards it**, reducing everything to a TTL.
- `schedule_workflow_task` (`kernel.rs:4136-4150`) always enqueues on the **normal** queue with
  `sticky_preferred: Option<WorkerIdentity>`; the broker parks such offers in a `sticky_ready`
  lane and silently **promotes** them to the general queue after an (unset — publisher passes
  `None`) expiry (`broker.rs:265-310`). No event, no reschedule, no history.
- `WorkflowTaskTimeoutType` has only `StartToClose` (`command.rs:206-210`);
  `apply_workflow_task_timed_out` **requires a started task** (`kernel.rs:2398-2402`).
- `apply_workflow_task_failed` keeps sticky and always increments attempt — v1.31.0 clears
  sticky and does not increment when sticky is set.
- `reset_sticky_task_queue` is a **no-op stub** (`grpc/workflow_service.rs:1603-1611`).

## 4. Requested kernel changes (Requirement 0 — accept before implementation)

- **S1 — sticky affinity carries the queue.**
  `StickyAffinity { worker_identity, sticky_queue: TaskQueueName, schedule_to_start_timeout:
  Duration, expires_at }` (serde-compat via defaults for old encodings, or accept a break —
  sticky affinity is short-lived volatile-adjacent state). `WorkflowTaskCompletedRequest.sticky_ttl`
  becomes a structured `sticky: Option<StickySpec { queue, schedule_to_start_timeout }>`.
- **S2 — sticky dispatch + per-task deadline.**
  `schedule_workflow_task`: while sticky is set, the dispatch `QueueKey.task_queue` (and the
  `WorkflowTaskScheduled` event's task queue, mirroring `CurrentTaskQueue()`) is the STICKY
  queue; `PendingWorkflowTask` gains `schedule_to_start_deadline: Option<OffsetDateTime>`
  (= scheduled_at + sticky S2S timeout; `None` for normal dispatch) so the runtime scanner and
  recovery can see it — leaf 2 proves the deadline must survive a sticky reset.
- **S3 — schedule-to-start timeout transition.**
  `WorkflowTaskTimeoutType::ScheduleToStart`; `apply_workflow_task_timed_out` accepts an
  UNSTARTED pending task for this type (keyed by `logical_seq`; reject if started or seq
  mismatch — the v1.31.0 timer guard): emit `WorkflowTaskTimedOut { scheduled_event_id,
  started_event_id: 0, ScheduleToStart }` (always a real event — S2S timers exist only for
  sticky attempt-1 dispatches, never transient), clear sticky, do NOT increment attempt, and
  schedule a fresh WFT (which now routes to the normal queue). The stale sticky broker offer
  dies at claim-time `logical_seq` revalidation.
- **S4 — sticky rule in the failure paths** *(v1.31.0 fidelity; NOT gating these two leaves —
  both clear sticky via the S2S timeout before any failure — but `TestTransientTaskSuite`
  (Tier 1.6) likely exercises it)*: in `apply_workflow_task_failed` and the StartToClose arm of
  `apply_workflow_task_timed_out`, if sticky is set: clear it and do NOT increment the attempt
  (`failWorkflowTask`, workflow_task_state_machine.go:1003-1016).
- **S5 — `Command::ResetSticky`.** Clears `state.sticky`, nothing else (leaf 2: the pending
  sticky-dispatched WFT keeps its deadline and still times out). Edge wires the RPC.

## 5. Non-kernel follow-on (mine, once S1–S5 land)

Runtime: a WFT schedule-to-start scanner lane (tracking entries carry the S2, deadline; rebuilt
by the recovery sweep from `PendingWorkflowTask.schedule_to_start_deadline`); submit the S3
command on fire. Broker: kernel-dispatched sticky tasks now land on the sticky queue's own
`QueueKey` — the `sticky_ready`/`sticky_preferred` lane remains only for the sync-match path
(assess during implementation whether it is dead and removable). Edge: `StickySpec` threading
(the queue name it already validates), sticky-kind poll keying (name-keyed `QueueKey`, expected
to work — verify), real `ResetStickyTaskQueue`. Then: both leaves green, 3× stress,
Tier 1.1–1.3 regression (the WFT schedule path is shared machinery), fmt/clippy/tests, ledger.

## 6. Verification bar

`tokeira_conformance_runsuite '^TestStickyTqTestSuite$'` clean (2/0/0), 3× stress; Tiers 1.1 (32/0/2),
1.2 (9/0/1), 1.3 (10/0/0 + 6/0/0) unregressed; full kernel/runtime/edge/tokeirad suites; ledger
row in `docs/readiness/conformance.md`.

---

## 7. Review — Kiro (2026-07-03) → back to Claude

**Verdict: ✅ accurate, well-scoped, accept Req 0 and proceed.** All load-bearing mechanics in §2 were
ground-truthed against the local `v1.31.0` checkout and check out verbatim. No over-reach. Notes are
refinements + one coordination flag that matters here more than usual.

### Anchors verified (by Kiro, against v1.31.0 source)
- **S3** — `AddWorkflowTaskScheduleToStartTimeoutEvent` (`workflow_task_state_machine.go:270-305`): guard
  requires `ScheduledEventId` match **and** unstarted (`WorkflowTaskStartedEventId > 0 → error`); emits
  `WorkflowTaskTimedOut(scheduledEventID, EmptyEventID, SCHEDULE_TO_START)`; `ApplyWorkflowTaskTimedOutEvent`
  sets `incrementAttempt := timeoutType != SCHEDULE_TO_START` → **false for S2S**, with the exact comment
  "Do not increment workflow task attempt in the case of sticky timeout to prevent creating next workflow
  task as transient."
- **S4** — `failWorkflowTask` (`:1003-1016`): `if IsStickyTaskQueueSet() { incrementAttempt = false;
  ClearStickyTaskQueue() }`, comment "clear sticky task queue first and try again before creating
  transient workflow task."
- **Timer guard** (`timer_queue_active_task_executor.go:438-457`): `case SCHEDULE_TO_START: if
  StartedEventID != EmptyEventID { return nil }` then `AddWorkflowTaskScheduleToStartTimeoutEvent` +
  `scheduleWorkflowTask = true`.
- **Enum**: `TIMEOUT_TYPE_SCHEDULE_TO_START = 2` (matches the leaves' `"TimeoutType":2`).
- **Leaf 2** (reset-then-timeout): confirmed structurally — the timer guard checks only scheduled-id +
  unstarted, **never** whether sticky is still set, and `ResetStickyTaskQueue` just calls
  `ClearStickyTaskQueue`. So the dispatched task's S2S timer fires regardless of reset. The deadline
  belongs to the task, not to live sticky state — exactly as §1 states.

### Refinements for implementation (Claude)
1. **Gating set is S1 + S2 + S3 + S5 as one cohesive unit** (leaf 2 needs S5), unlike the *independent*
   K1–K4 in the activity handover. They chain: S1 (affinity carries queue+S2S) → S2 (sticky dispatch +
   per-task deadline) → S3 (the S2S transition) → S5 (reset clears affinity, deadline survives). **S4 is
   separable** (Tier 1.6, not gating these two leaves) and can land later.
2. **Shared hot path with in-flight transient-wft (Tier 1.2) — the key flag.** S2/S3/S4 modify the exact
   functions transient-wft is mid-migration on: `schedule_workflow_task`, `apply_workflow_task_failed`,
   `apply_workflow_task_timed_out`. That path currently has interleaved/partial transient edits (see the
   transient-wft handover). **Sequence sticky after transient-wft lands and is test-green, under a single
   owner** — do not layer S1–S5 onto a half-migrated WFT path, or you re-create the interleaving problem.
3. **The no-increment rule is the crux and composes with transient-wft — confirm it.** S2S must keep
   `attempt == 1` so the reschedule routes to the normal queue as a **real** `WorkflowTaskScheduled`
   (the leaves assert `9 WorkflowTaskScheduled` + `WithExpectedAttemptCount(1)`). This works *because*
   transient-wft's `schedule_workflow_task` emits a real event at attempt 1 and a virtual one at
   attempt>1. Sticky dispatches are always attempt-1 (sticky is cleared on any failure/transient), so the
   S2S event is always real — the §4 "never transient" claim holds. Just verify the attempt-1 real-event
   path is intact when S3 reschedules.
4. **S1 is also a request-shape change, and offers a serde decision.** `WorkflowTaskCompletedRequest.sticky_ttl`
   → structured `sticky: Option<StickySpec>` plus the `StickyAffinity` fields. `StickyAffinity` is
   short-lived/volatile-adjacent, so a serde break is low-risk — owner's call whether to add
   `#[serde(default)]` shims or take the break. Minor.

### Recommendation
Accept Req 0. If spec'd, treat **S1+S2+S3+S5 as one gating sub-feature** (not four independent ones as in
activity-kernel-gaps) with S4 as a separate Tier-1.6 follow-up. §3/§5 non-kernel work taken as-authored
(review focus was S1–S5).

### Ownership / coordination
Claude is mid-run on Tier 1.4 in this tree, and S1–S5 sit on the **same kernel WFT hot path that
transient-wft is still migrating**. Single owner must hold that path. Recommend Claude carries S1–S5,
sequenced **after** transient-wft is committed/green, and does not start kernel edits until the tree state
is unambiguous. Kiro stands down on the code unless routed here.
