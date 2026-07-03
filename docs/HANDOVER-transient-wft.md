# Hand-over — transient WFT model + WFT heartbeat timeout (TestWorkflowTaskTestSuite)

**Author:** Claude (raise, functional-conformance drive) · **Date:** 2026-07-03 · **For:** Kiro
**Status:** RAISED → SPEC. Formalized as [`.kiro/specs/transient-wft/`](../.kiro/specs/transient-wft/requirements.md).
**Requirement 0 accepted (2026-07-03, owner)** — adopt the transient-WFT model, amending the Feature-2
(`kernel-wft-failure-timeout`) per-attempt-event decision. **Owner decisions:** (1) adopt the transient
model; (2) **item C (heartbeat timeout) is OUT OF SCOPE** — classified an `OverrideDynamicConfig`
out-of-scope skip (30m default is not operationally wrong; no config knob added).

**Implementation status (2026-07-03, Kiro):**
- **Item A — DONE + verified in-repo.** `apply_workflow_task_failed` resets attempt→1 and schedules a
  fresh real `WorkflowTaskScheduled` when buffered events flush; property test
  `wft_failed_with_buffered_events_schedules_fresh_normal_task` passes; kernel/runtime/edge suites green.
  Its skip entry is reclassified "REAL-GAP (implemented Phase A; pending harness confirmation)" — an
  operator harness run flips `…AfterRegularWorkflowTaskStartedAndFailWorkflowTask` to required-pass.
- **Item C — CLOSED (skip only).** Heartbeat leaf reclassified to a permanent `OverrideDynamicConfig`
  out-of-scope skip; not implemented, no knob.
- **Item B — NOT STARTED (next focused effort).** This is the large piece: a cohesive, **all-or-nothing**
  migration of the core WFT lifecycle. See "Item B — implementation notes" below before starting.

> **TL;DR.** `TestWorkflowTaskTestSuite` baseline: 4 pass / 5 fail, no hangs (the event-buffering +
> force-close + SA-codec groundwork carries the regular-WFT cases). The 5 failures need three kernel
> features, one umbrella: **(A)** the buffered-flush-resets-retry rule (small; 1 leaf),
> **(B)** the transient-WFT model (large; 3 leaves), **(C)** WFT heartbeat timeout (medium; 1 leaf).
> A is a sub-rule of B and can land first. All ground truth below is agent-verified against v1.31.0
> with live-run confirmation on tokeira main `2d9f19a4`.

## 0. The architectural decision required (blocking, mirrors event-buffering Req 0)

`kernel-wft-failure-timeout/design.md:5` codifies "emit one history event per attempt … re-dispatch".
v1.31.0's model is different: **a WFT with attempt>1 is *transient*** (`IsTransientWorkflowTask`,
`mutable_state_impl.go:2250`) — it lives only in mutable state. Its Scheduled/Started events are
virtual (ids beyond `last_event_id`), failing/timing-out/force-closing it writes **nothing**, and the
events materialize only when an attempt finally succeeds. Adopting this reverses the documented
per-attempt-event decision and changes observable history for every WFT-retry path → owner sign-off.

## A. Buffered-flush-resets-retry (1 leaf: …AfterRegularWorkflowTaskStartedAndFailWorkflowTask)

**Live failure:** history length 5 vs expected 6 — missing `6 WorkflowTaskScheduled` after
`4 WorkflowTaskFailed, 5 WorkflowExecutionSignaled`.

**Ground truth:** on WFT-failed close, v1.31.0 deletes the failed WFT and, because buffered events
flushed, **resets attempt to 1, forces type NORMAL, and persists a real fresh `WorkflowTaskScheduled`**
(`workflow_task_state_machine.go:332-338, 368-375`). tokeira's `apply_workflow_task_failed`
(kernel.rs ~2216-2243) flushes correctly but resurrects the SAME pending WFT (attempt+1, same
logical_seq, no new Scheduled event).

**Fix sketch (kernel, small):** capture `let flushed = builder.flush_buffered();` — when `flushed > 0`:
`workflow_task_attempt = 1`, drop the pending WFT, `schedule_workflow_task()` (fresh event + new
logical_seq + enqueue). Empty flush keeps today's behavior. Goldens for both branches; amend the
Feature-2 design note.

## B. Transient-WFT model (3 leaves: TerminationSignal{Before,After}TransientWorkflowTaskStarted[,AndFailWorkflowTask])

**Live failure (all 3, same first assertion):** attempt-2 poll `StartedEventId` = 5 (tokeira persists a
real Started event) vs expected 6 (virtual id; nothing persisted). Downstream every history literal
diverges — the tests fail a WFT 10×, and v1.31.0's history stays frozen at
`[1 Started, 2 Scheduled, 3 Started(wft), 4 Failed]` while the retries live off-history.

**Ground truth (all agent-cited):**
- transient = attempt>1 (`mutable_state_impl.go:2250`); only the attempt-1 failure writes
  `WorkflowTaskFailed` (`workflow_task_state_machine.go:892-905`); retries write nothing on
  fail/timeout (`:892-895, :965-967`).
- Retry scheduling persists NO Scheduled event — virtual `scheduledEventID = nextEventID`
  (`:322-327, :376-379`); transient start persists NO Started event — virtual
  `startedEventID = scheduledEventID + 1` (`:456-597, :480`).
- **Transient→normal conversion:** buffered/new events at scheduling → attempt=1 + real Scheduled
  (`:329-338`); new events by start time (`ScheduledEventID != nextEventID`) → attempt=1 + real
  Scheduled+Started (`:559-576`). This produces the tests' `[6 Scheduled, 7 Started]` after a signal.
- Terminate force-close of a transient WFT writes nothing (`workflow/util.go:118-120`) — tokeira's
  Phase-1 `force_close_started_workflow_task` (kernel.rs ~3815) must become transient-aware.
- Successful completion materializes Scheduled+Started late (`:750-800`).
- Poll + GetHistory synthesize the unpersisted suffix (`GetTransientWorkflowTaskInfo`,
  `mutable_state_impl.go:1189-1250`; `recordworkflowtaskstarted/api.go:430`;
  `getworkflowexecutionhistory/api.go:32-116` appendTransientTasks — tokeira's edge already has the
  cached-transient plumbing STUBBED in its GetHistory port; the poll side needs the synthesis).

**Fix sketch (kernel + runtime/edge, large):** kernel — suppress Started/Failed/TimedOut persistence
for attempt>1; virtual id assignment (`scheduled = last_event_id+1`, `started = +2`); the two
conversion rules; transient-aware force-close; late materialization in
`apply_workflow_task_completed`. Runtime/edge — poll response carries the synthesized Scheduled/Started
suffix; GetHistory appends it on the last page. tokeira's existing retry model is structurally close
(no new Scheduled on retry; attempt tracked) — the delta is event suppression + virtual ids +
conversion + synthesis.

## C. WFT heartbeat timeout (1 leaf: TestWorkflowTaskHeartbeatingWithEmptyResult)

**Live failure:** `s.Equal(2, hbTimeout)` got 0 — tokeira never times out heartbeat WFTs (test
heartbeats every ~1s under a 5s `WorkflowTaskHeartbeatTimeout`).

**Ground truth:** v1.31.0 tracks `OriginalScheduledTime` on the WFT, carries it across heartbeat
completions (`AddWorkflowTaskScheduledEventAsHeartbeat`, respondworkflowtaskcompleted
`api.go:571-578`), and when `now > original + heartbeatTimeout` (default 30m,
`dynamicconfig constants.go:2427`; the corpus overrides to 5s) it writes `WorkflowTaskTimedOut`
instead of Completed, clears sticky, schedules + inline-starts a fresh WFT, and returns
`NotFound("workflow task heartbeat timeout")` to the worker (`api.go:298, :534, :588, :733-736`).

**Fix sketch:** kernel — `PendingWorkflowTask.original_scheduled_at: Option<OffsetDateTime>`
(serde-default) + heartbeat carry-over in the force-new branch of `apply_workflow_task_completed`.
Runtime — heartbeat-timeout check in `complete_workflow_task` (heartbeat = force_new && empty
commands) routing to the existing `WorkflowTaskTimedOut` transition + a `RuntimeConfig` knob.
Edge — the orphaned inline start + NotFound return. Config — a `policy` TOML knob wired in tokeirad;
the harness runner then sets 5s in its generated config (legitimate TOML config, not dynamic-config
injection — the corpus override `dynamic_config_overrides.go:34` maps cleanly).

## Suite state + interim classification

4 pass / 5 raised. All five are classified registry skips citing this handover (they fail fast — no
suite-abort risk — but "pass clean" requires zero unclassified non-pass). **Post-decision flip order:**
A (1 leaf) → B (3 leaves) as they land; **C (1 leaf) stays a permanent out-of-scope skip** (not
implemented — see spec Item C). Spec phasing and flip details:
[`.kiro/specs/transient-wft/tasks.md`](../.kiro/specs/transient-wft/tasks.md). The drive continues at
Tier 1.3 (`TestActivityTestSuite` / `TestActivityClientTestSuite`) meanwhile.

## Item B — implementation notes (Kiro checkpoint, 2026-07-03)

Full requirements/design/tasks: [`.kiro/specs/transient-wft/`](../.kiro/specs/transient-wft/requirements.md)
(Req 0 accepted; Phase A done). Read these before starting.

**Item B is all-or-nothing — do not ship it partially.** The sub-parts are interdependent:
- Suppressing attempt>1 Scheduled/Started/Failed events (B.1–B.3) **requires** late materialization on
  success (B.6): otherwise a workflow that eventually succeeds after transient retries has *missing*
  Scheduled/Started events → corrupt history.
- It also **requires** poll synthesis (B.8): a worker polling a transient WFT gets a task referencing
  virtual event ids its history does not contain → it cannot process the task.
- So B.1–B.6 (kernel) + B.8–B.9 (runtime/edge) land together as one unit.

**tokeira current vs target (the delta):**
- Today (Feature-2 per-attempt): the failed WFT is kept alive with the *same* `logical_seq` and reuses
  the attempt-1 real `WorkflowTaskScheduled` id; `apply_workflow_task_started` emits a *real*
  `WorkflowTaskStarted` per retry attempt. Tests expect attempt-2 `StartedEventId = 6` (virtual:
  `scheduled = last_event_id+1 = 5`, `started = 6`) vs tokeira's persisted `5`.
- Target: at `attempt > 1` (the `IsTransientWorkflowTask` predicate), scheduling sets a **virtual**
  `scheduled_event_id = last_event_id + 1` and emits no event; start sets **virtual**
  `started_event_id = scheduled_event_id + 1` and emits no event; fail/timeout emit nothing (only the
  attempt-1 failure emits `WorkflowTaskFailed`); `last_event_id` is unchanged across all transient
  transitions.

**Kernel functions to change (all pure):**
- `apply_workflow_task_failed` (~2199) / `apply_workflow_task_timed_out` (~2258): the *no-flush* branch
  becomes the transient reschedule (virtual scheduled id, no event); Phase A's flush branch already
  handles the conversion-at-scheduling case (B.4 rule i) — keep it.
- `apply_workflow_task_started` (locate): at `attempt > 1`, assign virtual `started_event_id`, emit no
  event; add B.4 rule (ii) — if new events arrived by start time (`scheduled_event_id != last_event_id+1`),
  reset to attempt 1 and emit real Scheduled+Started.
- `schedule_workflow_task` (3869): guard the emit on `!transient` (attempt>1 → no event, virtual id).
- `apply_workflow_task_completed` (~1476): when the completing WFT was transient, materialize real
  Scheduled+Started before `WorkflowTaskCompleted` (B.6).
- `force_close_started_workflow_task` (~3815, event-buffering): emit nothing when the started WFT is
  transient (B.5); preserve the attempt-1 force-close `WorkflowTaskFailed`.
- Also make `apply_workflow_task_failed` suppress the top-of-function `WorkflowTaskFailed` emit when the
  failing task is transient (`workflow_task_attempt > 1`) — only attempt-1 emits it.

**Runtime/edge:** poll response synthesizes the virtual Scheduled/Started (B.8); GetHistory completes the
edge port's stubbed cached-transient suffix append (B.9). Both derived/read-only — nothing persisted.

**Verification bar (critical):** the WFT lifecycle here is what passing **Tier 1.1** depends on. After
Item B, run the full kernel + runtime + edge suites AND (operator) the Tier-1.1 + Tier-1.2 corpus before
flipping the three `TerminationSignal…TransientWorkflowTaskStarted[…]` skips. A regression here breaks
green Tier 1.1, so verify hard.
