# Tier 3.22 `TemporalReportedProblems` — forensic findings

**Date:** 2026-07-11 (diagnosis); remediation landed 2026-07-12 — see §6.
**Scope:** originally diagnosis-only. §§1–5 are the forensic report as delivered; §6 records the
remediation outcome. Companion to `docs/HANDOVER-reported-problems.md` (the investigation brief).
**Observable under investigation:** `TestWFTFailureReportedProblemsTestSuite` = 1 PASS
(`_NotClearedBySignals`) / 4 FAIL (`_SetAndClear` 20s, `_SetAndClear_FailAfterActivity` 20s,
`_DynamicConfigChanges` 15s). **After remediation: 5 PASS / 0 FAIL / 0 SKIP.**

---

## 1. The identified issue

**The defect is a missing increment source, not a broken one.** None of the handover's H1/H2/H3
holds as framed. The chain is:

1. The Temporal Go SDK — **v1.41.1**, pinned by the corpus harness
   (`../temporal/go.mod:68`) — sends `RespondWorkflowTaskFailed` **only when the task's attempt
   is 1**. For attempts ≥ 2 it processes the task, panics, logs, and **sends nothing**, letting
   the server's start-to-close timer drive the retry:

   ```go
   // go.temporal.io/sdk@v1.41.1/internal/internal_task_pollers.go:558-563 (sendTaskCompletedRequest)
   case *workflowservice.RespondWorkflowTaskFailedRequest:
       // Only fail workflow task on first attempt, subsequent failure on the same workflow task will timeout.
       // This is to avoid spin on the failed workflow task. Checking Attempt not nil for older server.
       if task.GetAttempt() == 1 {
           _, err = wtp.service.RespondWorkflowTaskFailed(grpcCtx, request)
   ```

   The `if` block is the *entire* body of that case: when `attempt != 1` no RPC of any kind is
   sent and the task is dropped client-side. The SDK documents the intended server interplay
   itself (`internal/internal_task_handlers.go:1318-1322`, `applyWorkflowPanicPolicy`,
   `BlockWorkflow` — the default policy): the panic error "will be convert to
   WorkflowTaskFailed for the first time, and ignored for subsequent attempts which will cause
   WorkflowTaskTimeout and server will retry forever". The gate reads
   `PollWorkflowTaskQueueResponse.Attempt`. There is no configuration escape: the fork has no
   `replace` directive or `vendor/` dir (the module-cache source is what compiles); the gate
   guards the SDK's **only** production `RespondWorkflowTaskFailed` call site, for every cause;
   the one related knob (`WorkflowPanicPolicy = FailWorkflow`) eliminates fail RPCs entirely
   by completing the workflow instead; the suite's harness uses `tests/testcore` with default
   `sdkworker.Options{}` (`functional_test_base.go:410-411`), i.e. `BlockWorkflow`; and an
   *unset* attempt (`GetAttempt() == 0`) would also suppress the RPC.

2. So on tokeira, after the one attempt-1 fail RPC, the rest of the transient chain is driven
   by the WFT start-to-close **timeout** path. Precisely: the attempt 1→2 transition happens
   inside the explicit-fail commit itself, which bumps the attempt and re-enqueues the retry
   immediately (`kernel.rs:3003, 3013-3026`) — this is why leaf 4's `Attempt ≥ 2` assertion
   passes within ~1s. Every transition after that (2→3, 3→4, …) is timeout-driven:
   `wft_timeout.rs` scanner (1s tick) → `Command::WorkflowTaskTimedOut` → kernel
   `apply_workflow_task_timed_out`, which advances `workflow_task_attempt` (why the SDK logs
   show attempts 1→2→3) but **never touches `WorkflowTaskProblemTracker`** — the tracker's
   sole production increment site is the `RespondWorkflowTaskFailed` RPC path
   (`crates/tokeira-runtime/src/runtime/workflow_task.rs:800-810`).

3. Temporal v1.31.0 does not have this gap because its explicit-fail and timeout paths converge
   on the same bookkeeping: the start-to-close timer task
   (`service/history/timer_queue_active_task_executor.go:364,423-437 @ v1.31.0`) calls
   `AddWorkflowTaskTimedOutEvent` → `ApplyWorkflowTaskTimedOutEvent(START_TO_CLOSE)` →
   `failWorkflowTask(true)`, which increments `AttemptsSinceLastSuccess` and writes the SA at
   threshold (`service/history/workflow/workflow_task_state_machine.go:263-268, 934-983,
   1003-1054 @ v1.31.0`).

Result: tokeira's counter reaches **1** (the attempt-1 explicit fail) and stalls, below the
suite's threshold of 2. The SA never appears for leaves 1, 3, 4. Leaf 2 passes because every one
of its failures is a fresh **attempt-1** WFT, so the SDK's gate lets each fail RPC through.

**The single wrong premise in the handover** (§3.2, "the SDK re-sends
`RespondWorkflowTaskFailed` each time") is what made H1–H3 look like the whole candidate space.
The SDK logs do show the workflow re-failing at attempts 1, 2, 3 — it *processes and panics*
each attempt — but the responder drops the report unless `attempt == 1`.

## 2. Proof (traced code paths)

### 2.1 tokeira reports the attempt truthfully, so the SDK gate engages

- Token minted from post-`WorkflowTaskStarted` state: `crates/tokeira-runtime/src/runtime/workflow_task.rs:1146-1160`
  (`attempt: pending.attempt`).
- Poll response populates `attempt` from the token and synthesizes the transient
  Scheduled/Started suffix at the virtual ids:
  `crates/tokeira-edge/src/translate/from_internal.rs:59-92`; the proto layer forwards it
  verbatim (`crates/tokeira-edge/src/grpc/translate.rs:2937`).
- On the return trip the task token is decoded inline at
  `crates/tokeira-edge/src/grpc/workflow_service.rs:1294-1295` via `serde_json::from_slice`
  into the same zero-serde-attribute struct that was serialized at
  `from_internal.rs:89` — byte-faithful both ways.
- So for the retry the SDK sees `attempt = 2, 3, …` and (correctly, per its own design) goes
  silent.

### 2.2 The tracker has exactly one increment source

- Counter: `crates/tokeira-runtime/src/runtime/mod.rs:161` (`record_failure`), reset:
  `:172` (`record_success`), consult: `:186` (`reported_problem`, live threshold).
- Production callers (grep-verified, all other hits are `#[cfg(test)]` or the unrelated
  `fairness.rs` counters):
  - `record_failure` — only `crates/tokeira-runtime/src/runtime/workflow_task.rs:808`, gated on
    `Ok(CommitResult::Applied)` from the `RespondWorkflowTaskFailed` submit.
  - `record_success` — only `crates/tokeira-runtime/src/runtime/workflow_task.rs:686`
    (completion `Applied | Duplicate`).
- Nothing in `wft_timeout.rs`, the lane handling of `WorkflowTaskTimedOut`, or any commit
  reaction touches the tracker.

### 2.3 The transient chain is timeout-driven and tracker-blind

Timeline for leaf 1 (`_SetAndClear`, threshold 2; the test sets no WorkflowTaskTimeout, so the
10s default applies — SDK-side, and tokeira's edge default agrees,
`crates/tokeira-edge/src/translate/to_internal.rs:95-97`):

| t (≈) | event | tracker |
|-------|-------|---------|
| 0s | WFT attempt 1 starts, panics; SDK sends fail RPC → `fail_workflow_task` commits `Applied`; kernel emits `WorkflowTaskFailed`, re-arms pending task with virtual ids, attempt→2, retry enqueued **in the same commit** (`kernel.rs:2986-3027`) | **counter = 1** |
| ~0.5s | attempt 2 starts (virtual ids, no events, `kernel.rs:1589-1593`); SDK panics, **sends nothing** | 1 |
| ~10.5-12s | attempt 2's start-to-close expires (strict `now - started_at > timeout`, `wft_timeout.rs:184`, on a 1s tick, `:171`); scanner submits `WorkflowTaskTimedOut(StartToClose)` (`:284-292`); kernel: no event (transient, `kernel.rs:3204-3214`), attempt→3 (`:3225`), reschedule virtual (`:3239-3244`, `:5649-5659`) | **still 1** |
| ~11-12.5s | attempt 3 starts; SDK panics, silent; its timeout would land ~21-24s | 1 |
| 20s | `EventuallyWithT` window (test.go:98-110, 20s/500ms) expires at the SA-presence `require.True(t, ok)` (test.go:102) | 1 < 2 → FAIL |

This reconciles the observed durations exactly: leaves 1 and 3 fail at their 20s windows; leaf 4
fails at its 15s phase-2 window (test.go:264-273). The observed run also **jointly excludes
both rival models**: if tokeira counted start-to-close timeouts, every failing leaf would pass
in-window (leaf 1 SA at ~11s of 20s, leaf 3 ~12s of 20s, leaf 4 ~11s of 15s); and if fail RPCs
were arriving for attempts ≥ 2, the fail path's immediate re-enqueue would advance attempts
sub-second, putting the SA up at ~1s and attempt counts far past 3 within the window. Attempts
1, 2, 3 across ~20s with failures at exact window exhaustion matches only
tracker-stuck-at-1 under 10s-timeout-driven retries.

### 2.4 The v1.31.0 anchors tokeira diverges from

All read at tag `v1.31.0`, first-hand:

- **Timeouts increment the counter.** `executeWorkflowTaskTimeoutTask`
  (`timer_queue_active_task_executor.go:364`, `case TIMEOUT_TYPE_START_TO_CLOSE` at
  `:424-437`) → `AddWorkflowTaskTimedOutEvent` (`workflow_task_state_machine.go:934-983`,
  which **always** calls `ApplyWorkflowTaskTimedOutEvent(START_TO_CLOSE)` at `:981`) →
  `incrementAttempt := timeoutType != TIMEOUT_TYPE_SCHEDULE_TO_START` →
  `failWorkflowTask(incrementAttempt)` (`:263-268`).
  `failWorkflowTask` increments `AttemptsSinceLastSuccess` exactly when `incrementAttempt`
  (`:1017-1027`, propagated to `executionInfo` via `UpdateWorkflowTask`, `:1117`) and writes
  the SA when
  `AttemptsSinceLastSuccess >= NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute`
  (`:1050-1054`).
- **Why the SA still reads `category=WorkflowTaskFailed` when a *timeout* crosses the
  threshold.** The SA is composed from the **persisted**
  `executionInfo.LastWorkflowTaskFailure` oneof
  (`mutable_state_impl.go:6478-6491`): the `FailureCause` variant renders the two strings the
  test asserts. That field is written only inside the non-transient guards — explicit fail
  (`workflow_task_state_machine.go:893-911`, only when the `WorkflowTaskFailed` event is
  actually emitted) and attempt-1 timeout (`:966-979`). A **transient** timeout emits no event
  and does **not** overwrite the cause — it silently bumps the count. So on v1.31.0, leaf 1
  reaches ASLS=2 at the first transient timeout (~10s) with the cause strings still those of the
  attempt-1 explicit failure. Exactly the test's expectation.

### 2.5 What this rules out (H1/H2/H3 as framed)

- **H1 (token round-trip / submit verdict):** the task token is a full-struct serde-JSON blob
  echoed opaquely (`from_internal.rs:89`; round-trip fidelity test
  `to_internal.rs:948-981`); the fail-path fences match the virtual ids; the lane commits
  empty-event transitions as `Applied` (`lane.rs:1376-1497`, `memory.rs:309/641` per handover
  §3.5). H1's *spirit* was right — the transient submit indeed never returns `Ok(Applied)` —
  but only because **the submit never happens**. Inverting this closes the loop: had
  attempt-≥2 fail RPCs existed, the kernel's fences (`kernel.rs:2663-2671`) would have
  accepted their tokens and the counter would have advanced — the SA's absence is itself
  independent evidence the RPCs never arrived.
- **H2 (spurious `record_success`):** sole caller is the completion `Applied | Duplicate` arm
  (`workflow_task.rs:686`); no completion occurs during a failing chain. The transient-drop arms
  (`:565`, `:645`) belong to the *completion-rejected* path and never reach the success match.
- **H3 (sticky as the failure cause):** leaf 3 fails for the same reason as leaf 1. Sticky
  changes the counting lineage (see §4) but not the outcome; the missing timeout increment
  dominates.

## 3. Blast radius (per leaf)

- **Leaf 1 `_SetAndClear` — explained.** Counter stalls at 1 (§2.3). Fails the SA-presence
  `require.True` at test.go:102 after the 20s window.
- **Leaf 2 `_NotClearedBySignals` — passes, and the mechanism confirms the diagnosis.** The
  workflow self-signals before each panic (test.go:50-56). The buffered signal flushes at each
  WFT-failed close, which resets the retry to a **normal attempt-1 task**
  (`kernel.rs:2974-2985`). Every failure is therefore attempt 1 → the SDK gate passes → a fail
  RPC arrives each time → the counter genuinely advances 1, 2 → SA appears. The test itself
  proves this is the mechanism on real Temporal too: its second Eventually (test.go:154-174)
  asserts the first 9 history events **exactly**, with *persisted* `WorkflowTaskFailed` events
  at positions 4 and 8 — i.e. upstream also expects each post-signal failure to be a fresh,
  non-transient, explicitly-failed attempt-1 task. (Both v1.31.0's ASLS and tokeira's tracker
  survive the task reset — they only clear on *success* — which is the property this leaf
  exists to check.)
- **Leaf 3 `_SetAndClear_FailAfterActivity` — explained.** WFT1 completes (activity scheduled)
  → `record_success`. Post-activity WFT2 is sticky-dispatched, panics at attempt 1 → one fail
  RPC (counter = 1), tokeira increments attempt→2 (no sticky exemption; §4). All later attempts
  are silent → counter stalls at 1. On v1.31.0 the lineage differs: the sticky failure counts
  **nothing** (ASLS stays 0, sticky cleared, attempt stays 1), the non-sticky re-run panics at
  attempt 1 again → second fail RPC (ASLS=1, attempt→2), then a transient timeout (ASLS=2) →
  SA.
- **Leaf 4 `_DynamicConfigChanges` — explained, including the passing phase.** Phase 1
  (threshold 0, 10s window): SA absent ✓ (threshold-0 suppression at
  `runtime/mod.rs:191` — and the counter is 1 anyway) and `Attempt ≥ 2` ✓ (kernel attempt
  advances on the fail and on each timeout). Phase 2 (threshold 2, 15s window): counter still 1
  → SA never appears → fails at test.go:264-273. Override delivery cannot be the culprit —
  a counter stuck at 1 fails any threshold ≥ 2 regardless of what was delivered — though see
  §5.5 for why the run's evidence proves delivery *innocent* without proving it *exercised*.

## 4. The benign divergence (H3 converse) — confirmed, with a correction

Confirmed, in two parts, from `apply_workflow_task_failed` (`kernel.rs:2636-3030`) and
`fail_workflow_task` (`workflow_task.rs:772-812`):

1. **Sticky over-count + attempt inflation.** tokeira has **no sticky branch** on the WFT-failed
   path: every `Applied` failure increments `workflow_task_attempt` (`kernel.rs:3003`) and every
   non-`GrpcMessageTooLarge` `Applied` failure increments the tracker
   (`workflow_task.rs:800-810`). v1.31.0, when a sticky queue is set, forces
   `incrementAttempt = false`, clears the sticky queue, and increments **neither** the attempt
   **nor** `AttemptsSinceLastSuccess` (`workflow_task_state_machine.go:1012-1027`).
   **Correction to the handover's "benign, appears-earlier" framing:** the attempt inflation is
   not benign in effect under the SDK's attempt-1 gate. Because tokeira's retry after a sticky
   failure is attempt **2** (v1.31.0's would be attempt **1**), the SDK suppresses a fail RPC
   that a v1.31.0 lineage would have received — i.e. the sticky divergence *costs* an increment
   as well as adding one. In leaf 3 the two effects net out at counter = 1 either way, so it is
   not the failure cause, but "over-count ⇒ SA earlier" is not the right model.
2. **Sticky affinity is not cleared on failure — a known, deliberately deferred gap.**
   v1.31.0's `failWorkflowTask` calls `ClearStickyTaskQueue()` (`:1012-1015`); tokeira's
   failed-path keeps `state.sticky` (it even reuses it as the `sticky_preferred` dispatch hint,
   `kernel.rs:2990-2994`) and clears it only on the *timeout* paths (`kernel.rs:3111`,
   `:3227`). tokeira's own comment labels the missing clear "v1.31.0 clears sticky on any WFT
   failure … (S4, deferred)" (`kernel.rs:5619-5622`). Real sticky affinity is set only by a
   completion carrying sticky attributes (`kernel.rs:1922-1927`, sticky raise S1). Two
   observable consequences: (a) the transient retry still lands on the normal queue (sticky
   dispatch is gated to attempt 1, `kernel.rs:5627-5629`), so the impact in this suite is nil;
   but (b) if buffered events convert the retry to a fresh attempt-1 task
   (`kernel.rs:2974-2985`), that task **re-dispatches on the sticky queue** — v1.31.0, having
   cleared sticky, would dispatch it on the normal queue.

**Latent gaps in the same family (not exercised by Tier 3.22, recorded while here):**

- **Timeout category rendering.** tokeira's Describe-side derive
  (`apps/tokeirad/src/lib.rs:1623`) can only render `category=WorkflowTaskFailed` +
  `cause=WorkflowTaskFailedCause…`. v1.31.0 renders
  `category=WorkflowTaskTimedOut` + `cause=WorkflowTaskTimedOutCause…` when the last
  **non-transient** problem was an attempt-1 timeout (`mutable_state_impl.go:6486-6491`). A
  timeout-first corpus leaf would expose this; worth folding into the same remediation.
- **Server-decided WFT failures don't count.** Three paths submit
  `Command::WorkflowTaskFailed` without going through `fail_workflow_task`, so they never
  reach `record_failure`: the invalid-command arm
  (`runtime/workflow_task.rs:566-580`), the bad-update-message arm (`:646-660`), and the
  reset fork-point failure (`lane.rs:768-783`). v1.31.0 routes all of these through
  `failWorkflowTask(true)` and increments `AttemptsSinceLastSuccess`.

## 5. Recommended remediation direction (prose only — nothing implemented)

Runtime-side, exactly as the handover's honesty boundary requires (the real counter must
advance; the kernel stays untouched; the derive stays untouched):

1. **Add the timeout increment source.** Where the WFT start-to-close timeout submit commits
   `Applied` (the scanner's submit closure wired in `runtime/mod.rs` around
   `wft_timeout.rs:284-292`), record a problem for the run. Mirror v1.31.0's gate precisely:
   **StartToClose increments; ScheduleToStart does not**
   (`workflow_task_state_machine.go:263-268`). Two completeness notes from the same funnel:
   v1.31.0's sticky suppression lives inside `failWorkflowTask` itself, so it applies to
   timeout-driven increments identically (the first start-to-close timeout of a
   sticky-dispatched task is also "free"); and the WFT **heartbeat**-exceeded completion is
   another `AddWorkflowTaskTimedOutEvent` caller
   (`respondworkflowtaskcompleted/api.go:288-306 @ v1.31.0`) that a full port would cover.
2. **Split count from cause, the way v1.31.0 does.** v1.31.0's increment
   (`AttemptsSinceLastSuccess`) and its cause identity (`LastWorkflowTaskFailure`) are separate;
   transient problems bump the count without overwriting the cause (§2.4). The tracker needs the
   same split: a timeout-driven increment must **not** clobber an existing `Failed`-cause entry
   — reusing `record_failure(cause = timeout-ish)` verbatim would make the derive emit strings
   Tier 3.22's asserts reject. Concretely: a transient (attempt>1) timeout bumps the count
   only; a **non-transient** (attempt-1) timeout sets the `TimedOut` category/cause, whose
   v1.31.0 rendering is exactly `["category=WorkflowTaskTimedOut",
   "cause=WorkflowTaskTimedOutCauseStartToClose"]` — which also closes the latent derive gap
   in §4. Whether the "was this transient/sticky" signal comes from the commit's `new_state` or
   from return metadata is the owner's design choice; both stay runtime-side.
3. **Sticky parity (separable, smaller — the deferred S4 raise).** Skip the tracker increment
   and the attempt increment for a sticky-dispatched failure, and clear sticky on failure, per
   `workflow_task_state_machine.go:1012-1027`. Leaf 3 is *doubly*-caused — either fix alone
   rescues it: with #1 alone it passes with the wrong lineage (over-counted sticky fail +
   first transient timeout = 2); with #3 alone the sticky-cleared retry becomes a fresh
   attempt-1 normal-queue task the SDK explicitly fails, so `record_failure` fires a second
   time. With both, the lineage matches v1.31.0's exactly (uncounted sticky fail → counted
   attempt-1 non-sticky fail → counted transient timeout). Leaves 1 and 4 are rescued only
   by #1.
4. **Dynamic evidence worth capturing before implementing** (not gathered here, per the
   no-instrumentation constraint): a gRPC-level trace of leaf 1 confirming zero
   `RespondWorkflowTaskFailed` calls after the first, and the timeout scanner firing at ~10s
   cadence driving attempts 3+. Both are predicted, not observed, by this report — everything
   else above is static-verified.
5. **A verification caveat for the remediation bar.** The handover treats leaf 2 as proof that
   the threshold-2 override is delivered. Strictly, leaf 2 would also pass at the pinned
   default of 5 (its attempt-1 fail loop cycles sub-second, so the counter clears 5 well
   inside the 20s window), and leaf 4's threshold-0 phase asserts an absence that counter=1
   produces at any threshold ≥ 2. Delivery is very likely fine (it cannot explain counter ≤ 1,
   so it does not touch this diagnosis), but when re-running after the fix, confirm delivery
   positively — the bridge logs "not delivered" lines on fallback
   (`tests/testcore/tokeira_dynamic_config_bridge.go:68-93`) and the control listener logs its
   bind.

## 6. Remediation outcome (2026-07-12)

All issues in this report were addressed, with one architectural upgrade over §5's sketch: rather
than teaching the runtime tracker to reverse-engineer kernel decisions, the counter moved **into
kernel state**, where the sticky/transient knowledge lives natively — v1.31.0 couples
`AttemptsSinceLastSuccess` to the attempt counter under the *same* `if incrementAttempt` guard,
so the kernel transition is the faithful home. This also retired the tracker's documented
volatile-restart OPEN.

What landed (all uncommitted, alongside the Tier 3.22 base):

- **`WorkflowState` gains `workflow_task_attempts_since_last_success` and
  `last_workflow_task_problem`** (`WorkflowTaskProblem::Failed(cause) | TimedOutStartToClose`,
  the `LastWorkflowTaskFailure` analog), `#[serde(default)]`
  (`crates/tokeira-kernel/src/state.rs`).
- **Counting in the kernel transitions** (`crates/tokeira-kernel/src/kernel.rs`):
  `apply_workflow_task_failed` and `apply_workflow_task_timed_out(StartToClose)` increment under
  v1.31.0's exact guard; ScheduleToStart never counts; `apply_workflow_task_completed` clears
  both; the buffered-limit and terminate force-close helpers count through the same rule; the
  replay arms mirror all of it. The problem identity is written only when the event persists
  (non-transient), so a transient timeout crossing the threshold reports the attempt-1 cause —
  §2.4's contract.
- **Sticky raise S4** (closes §4.1 and §4.2): a failure or start-to-close timeout with the real
  sticky queue set clears the affinity, counts nothing, resets the attempt to 1, and reschedules
  a fresh normal-queue task; `apply_workflow_task_started` no longer lets the poll-side hint
  clobber a real sticky affinity (which would have blinded the fail-time sticky check).
- **§4's latent gaps closed by construction:** the server-decided WFT-failure arms submit
  `Command::WorkflowTaskFailed` and therefore now count; the Describe renderer emits the
  `TimedOut` pair (`["category=WorkflowTaskTimedOut",
  "cause=WorkflowTaskTimedOutCauseStartToClose"]`).
- **Runtime/tokeirad:** `WorkflowTaskProblemTracker` deleted; Describe derives from committed
  state via `reported_problem_from_state` with the threshold still read live (conformance
  override honored; kernel stays threshold-free and conformance-free).

Verification: `TestWFTFailureReportedProblemsTestSuite` **5/0/0** (leaf timings match §2.3's
model — the SA appears at the first transient timeout ~11s); regressions
`TestStickyTqTestSuite` 3/0/0, `TestTransientTaskSuite` 3/0/1 and `TestMaxBufferedEventSuite`
2/0/1 (both skips pre-existing registry entries); `cargo test --workspace` green (126 suites);
9 new kernel tests in `crates/tokeira-kernel/tests/reported_problems.rs`. §5.5's delivery caveat
is retired: the control listener bound, the bridge logged no fallback for the threshold key, and
leaf 4's 15s phase-2 window is unreachable at the pinned default of 5 — the pass itself proves
threshold-2 delivery.

An adversarial multi-agent review (three lenses, confirmed findings re-verified) ran over the
remediation. Two confirmed majors were **fixed in place**: the paused-with-sticky retention
corners (fail and timeout paths) would have left a retained attempt-1 task paired with a
virtual/stale scheduled id — malformed history on resume — so the attempt-1 reset is now scoped
to the fresh-reschedule route, keeping paused retentions in the transient shape the resume
conversion expects (kernel tests re-run green: 209 golden + 9 counting).

**Residuals recorded for the owner (none reachable by a green corpus leaf):**

- **Sticky clear on mismatched-queue start.** v1.31.0 clears stickiness when a task starts from
  a poller whose queue differs from the sticky queue (`recordworkflowtaskstarted/api.go:113-135`
  — the StickyWorkerUnavailable normal-queue fallback). Tokeira's live dispatch cannot produce
  that mismatch, but a recovery/sweep republish after restart/failover can start a
  sticky-scheduled task from the normal queue with `state.sticky` still set; a failure there
  would take the S4 no-count path where v1.31.0 would count. Needs poller-queue provenance on
  `StartWorkflowTaskRequest`; belongs to the sticky raise series.
- **Reset base-run close counting.** v1.31.0 terminates the reset BASE run via the
  force-fail-counting terminate path; tokeira's base close during reset does not run the
  force-close counting. Affects only Describe of a closed base run mid-reset (Tier 3.17
  territory).
- **Replay of a reset successor's fork-point failure.** The replay arm treats every
  `WorkflowTaskFailed(ResetWorkflow)` as a base-run close marker (pre-existing semantics), so a
  successor's own fork-point failure replays without the +1/cause the hot path records.
- **Speculative rejection-only drop.** The drop-completion early return does not reset the
  accumulator; whether v1.31.0's `deleteWorkflowTask` zeroes `AttemptsSinceLastSuccess` there
  should be pinned before mirroring (speculative-wft territory; no update-driven leaf asserts
  the SA today).
- **Derive-at-read threshold dynamics.** A mid-run threshold *raise* retracts an
  already-surfaced SA on the next Describe, where v1.31.0's persisted SA would linger until the
  next `failWorkflowTask`/completion; conformance-override builds only.
- **Capability advertisement.** `reported_problems_search_attribute` is pinned `true`; v1.31.0
  advertises `threshold > 0` (`namespace_handler.go:865`), diverging only under an override of 0.
- **Visibility projection.** The SA surfaces on Describe only; v1.31.0 also upserts it to
  visibility for `ListWorkflowExecutions` (recorded in the compatibility matrix note).

## 7. Provenance

Every behavioural claim above was verified by direct reading in this investigation:
tokeira paths in the working tree (uncommitted Tier 3.22 state on `main`); Temporal server at
tag `v1.31.0` via `git -C ../temporal show v1.31.0:<path>`; Go SDK at
`~/go/pkg/mod/go.temporal.io/sdk@v1.41.1` (version pinned by `../temporal/go.mod:68`; no
`replace`/`vendor` overrides); the test contract from
`../temporal/tests/workflow_task_reported_problems_test.go` (working tree, clean vs HEAD). An
independent nine-agent verification pass was then run against the central finding: six
ground-truth readers (v1.31.0 server, Go SDK, test contract, tokeira timeout path, tokeira
sticky semantics, tokeira edge fidelity) and three adversarial refutation lenses (SDK
behavior, tokeira alternative causes, timing reconciliation). All three refutation lenses
returned **not refuted**; their corrections — attempt-2 provenance (fail-commit-driven, not
timeout-driven), leaf 3's exact counter value and double causation, the 10–11s effective
timeout latency, the extra under-count seams, and the delivery-verification caveat — are
incorporated above.
