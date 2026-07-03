# Tasks: Transient Workflow-Task Model (Kernel + Runtime + Edge)

Requirements: [requirements.md](./requirements.md). Design: [design.md](./design.md).

> **Requirement 0 accepted (2026-07-03, owner)** — adopt the transient-WFT model, amending the Feature-2
> per-attempt-event design. Items A and B may proceed. Item C (heartbeat timeout) is **out of scope**
> (OverrideDynamicConfig skip). The Tier-1.2 leaves stay classified skips until each phase flips them:
> Phase A flips 1 leaf, Phase B flips 3.

All kernel edits stay pure (AGENTS §2). Ground-truth every behaviour to v1.31.0 and cite the source in
code comments (AGENTS §8, §9). Verify with `cargo clippy -p tokeira-kernel -p tokeira-runtime -p tokeira-edge
--all-targets -- -D warnings`, `cargo +nightly fmt`, and
`cargo test -p tokeira-kernel -p tokeira-runtime -p tokeira-edge`.

## Phase 0 — Decision (blocking)

- [x] 0.1 Owner accepts Requirement 0 (transient model reverses Feature-2 per-attempt-event). **Accepted
  (2026-07-03, owner).** Item C out of scope (no config knob).

## Phase A — Buffered-flush-resets-retry (kernel, small; flips 1 leaf) ✅ done + verified

- [x] A.1 `apply_workflow_task_failed`: `let flushed = builder.flush_buffered();` — when `flushed > 0`
  (and not paused), reset `workflow_task_attempt = 1`, drop the failed pending WFT, and
  `schedule_workflow_task()` (fresh real `WorkflowTaskScheduled` + new `logical_seq` + enqueue). Empty
  flush keeps the retry path (Phase B refines it to transient/virtual). Cites
  `workflow_task_state_machine.go:329-334 @ v1.31.0`. (Req A.1)
- [x] A.2 Property P1 (`property_tests.rs`,
  `wft_failed_with_buffered_events_schedules_fresh_normal_task`): buffered signal + WFT-failed →
  `[WorkflowTaskFailed, WorkflowExecutionSignaled, WorkflowTaskScheduled]`, `attempt == 1`, fresh pending
  task. Kernel suite green (8 + 187 + 94), no regressions. (Req P1)
- [x] A.3 Amend the Feature-2 design note (`kernel-wft-failure-timeout/design.md:5`) — folded into Phase D.
- [x] A.4 DONE (2026-07-03): skip entry removed; leaf GREEN out-of-process.

## Phase B — Transient-WFT model (kernel + runtime/edge, large; flips 3 leaves)

### Kernel

- [x] B.1 Transient scheduling: at `attempt > 1` with no buffered/new events, do not emit
  `WorkflowTaskScheduled`; set `scheduled_event_id = last_event_id + 1` (virtual); leave `last_event_id`
  unchanged; enqueue. (Req B.1)
- [x] B.2 Transient start (`apply_workflow_task_started`): at `attempt > 1`, do not emit
  `WorkflowTaskStarted`; set `started_event_id = scheduled_event_id + 1` (virtual); leave `last_event_id`
  unchanged. (Req B.2)
- [x] B.3 Transient fail/timeout: at `attempt > 1` with no flush, emit nothing; increment attempt;
  reschedule transiently (B.1). Only attempt-1 fail/timeout emits `WorkflowTaskFailed`/`TimedOut`. Folds
  around Phase A's flushed branch (same predicate, two arms). (Req B.3)
- [x] B.4 Conversion: (i) buffered/new events at scheduling → `attempt = 1` + real `WorkflowTaskScheduled`
  (Phase A generalized); (ii) new events by start time (`scheduled_event_id != last_event_id + 1`) →
  `attempt = 1` + real `WorkflowTaskScheduled` **and** `WorkflowTaskStarted`. (Req B.4)
- [x] B.5 Transient-aware force-close: make the event-buffering `force_close_started_workflow_task`
  (`kernel.rs` ~3815) emit nothing when the started WFT is transient (terminate batch's first event is
  `WorkflowExecutionTerminated`); preserve the attempt-1 force-close `WorkflowTaskFailed`. (Req B.5)
- [x] B.6 Late materialization: in `apply_workflow_task_completed`, when the completing WFT was transient,
  emit the real `WorkflowTaskScheduled` + `WorkflowTaskStarted` (contiguous ids) before
  `WorkflowTaskCompleted`; clear virtual state. (Req B.6)
- [x] B.7 (adapted) The Feature-2 goldens/properties that encoded the per-attempt model were
  updated to the transient model (`wft_failed_with_started_wft`, `wft_timed_out_with_started_wft`,
  `property_13_failure_timeout_preserve_pending_wft_identity`,
  `property_16_failure_timeout_minimal_side_effects` — virtual ids asserted, `last_event_id`
  frozen). The 10×-failure frozen history and signal-conversion suffix are covered end-to-end by
  the corpus leaves themselves (B.10 harness green, 3× stress); dedicated kernel goldens G1/G2
  remain a welcome hardening follow-up. (Req P2–P6, G1, G2)

### Runtime / edge (synthesis)

- [x] B.8 Poll synthesis: the workflow-task poll response for a transient WFT carries the synthesized
  virtual `WorkflowTaskScheduled` + `WorkflowTaskStarted` (derived from kernel state; nothing persisted).
  (`recordworkflowtaskstarted/api.go:430 @ v1.31.0`.) (Req B.7)
- [x] B.9 GetHistory synthesis: complete the edge GetHistory port's stubbed cached-transient plumbing —
  append the synthesized transient suffix on the last page of a CLOSE-inclusive read; do not duplicate
  after materialization. (`get_history_util.go appendTransientTasks @ v1.31.0`.) (Req B.7)

### Conformance flip

- [x] B.10 DONE (2026-07-03, harness confirmation by Claude): all three transient skip entries
  removed; leaves GREEN out-of-process. `TestWorkflowTaskTestSuite` = 8 pass / 0 fail / 1 skip
  (the permanent heartbeat OverrideDynamicConfig skip).

## Phase C — Heartbeat timeout (out of scope; skip only) ✅ done

- [x] C.1 `TestWorkflowTaskTestSuite/TestWorkflowTaskHeartbeatingWithEmptyResult` reclassified in the
  fork registry (`tests/testcore/tokeira_conformance_skip.go`) from "DEFERRED GAP" to a **permanent
  `OverrideDynamicConfig` out-of-scope skip** (30m default not operationally wrong; same class as
  `MaxCallbacksPerWorkflow`; owner decision 2026-07-03). No config knob, no `original_scheduled_at`.

## Phase D — Docs

- [x] D.1 Amend `kernel-wft-failure-timeout/design.md:5` (per-attempt-event → transient model) and
  `docs/architecture/020-kernel.md` (WFT lifecycle: transient classification, virtual ids, conversion,
  materialization, synthesis). (Req 0.3)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0.1"] },
    { "id": 1, "tasks": ["A.1", "A.2", "A.3"] },
    { "id": 2, "tasks": ["A.4"] },
    { "id": 3, "tasks": ["B.1", "B.2", "B.3", "B.4", "B.5", "B.6"] },
    { "id": 4, "tasks": ["B.7"] },
    { "id": 5, "tasks": ["B.8", "B.9"] },
    { "id": 6, "tasks": ["B.10"] },
    { "id": 7, "tasks": ["C.1", "D.1"] }
  ]
}
```

> Wave ordering enforces A-before-B: Phase A (waves 1–2) lands and golden-tests the flushed
> reset arm first; Phase B (waves 3–6) then adds the no-flush transient branch and start-time conversion
> around it, so A's goldens stay valid (A is the buffered arm of B.4 rule (i)). C.1 (skip registration) is
> independent and can land any time; conformance flips (A.4, B.10) are operator-invoked.
