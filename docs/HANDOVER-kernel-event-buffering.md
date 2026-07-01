# Hand-over — kernel event buffering + force-close WFT ordering

**Author:** Kiro · **Date:** 2026-07-01 · **For:** Claude Code (functional-conformance drive)
**Repos:** this workspace (`tokeira`, Rust — the kernel feature) and the pinned fork (`../temporal`,
skip registry).

> **TL;DR.** The conformance leaf `TestTerminateWorkflowOnMessageTooLargeFailure` is not an
> edge/runtime fix. Making it green requires **event buffering** in `tokeira-kernel`, which **reverses a
> documented deliberate deviation** (`state.rs:187`, `020-kernel.md:389`). That is an *Architectural*
> change (AGENTS classification), raised as a spec — `.kiro/specs/kernel-event-buffering/` — not patched
> inline.
>
> **Decision (2026-07-01, owner):** Requirement 0 is **accepted for Phase 1**. Implement **Phase 1
> only** (signals/cancel-requested buffering + flush-on-close + terminate force-close). **Phase 2**
> (completion-class buffering + `reorderBuffer`) is **deferred** — it does not surface anywhere in the
> current suite (`TestWorkflowTestSuite`) and is picked up only when a leaf actually buffers an
> activity/child/Nexus completion during a started WFT (see §5). The leaf stays a classified skip until
> Phase 1 lands.

---

## 1. What was adjudicated (the four raised items)

| # | Item | Disposition |
|---|------|-------------|
| 1 | `TestTerminateWorkflowOnMessageTooLargeFailure` | **Kernel feature, spec'd.** See §2. Genuine kernel state-machine work; pure (no I/O), so it does **not** violate §2 kernel purity. Classify-skip now; spec + owner sign-off, then implement. |
| 2 | `InternalTaskQueue/multiOp` | **Feature spec (Update-with-Start / MultiOperation).** `ExecuteMultiOperation` returns `unimplemented`; the honest fix is the feature, not a hollow error stub. There is already an `api-conformance-multi-operation` spec — route it there. Classify-skip until it lands. |
| 3 | `OnConflictOptions/failed_max_callbacks_per_workflow` | **Systemic Shape-2 harness gap.** `OverrideDynamicConfig(MaxCallbacksPerWorkflow, 1)` cannot reach an out-of-process `tokeirad`; `MaxCallbacksPerWorkflow` is a pinned constant, not a knob. Record the **class** (any `OverrideDynamicConfig`-dependent leaf) as an out-of-scope skip in the registry with a cited reason — not a per-leaf triage. |
| 4 | `TestWorkflowRetry` / `TestWorkflowRetryFailures` | **Confirmed NOT buffering — off this spec.** Both assert plain 5-event per-attempt histories (`WorkflowExecutionStarted, WorkflowTaskScheduled, WorkflowTaskStarted, WorkflowTaskCompleted, WorkflowExecutionFailed/Completed` + ContinuedAsNew run-id links; `tests/workflow_test.go:1440-1520 @ v1.31.0`) — no signal, no completion-during-WFT, no buffered event. The 6-vs-4 delta is the retry-chain / `RespondWorkflowTaskFailed` edge-and-runtime path, **not** the kernel buffering model. Route under `api-conformance-wft-completion` / `edge-unimplemented.md`. |

---

## 2. The kernel feature — what and why

Verified against v1.31.0 (local `../temporal`):

- The target test signals **while a WFT is started**, then `RespondWorkflowTaskFailed` with cause
  `GRPC_MESSAGE_TOO_LARGE`. Expected history (`tests/workflow_test.go:993 @ v1.31.0`):
  ```
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed          (force-close, cause = FORCE_CLOSE_COMMAND)
  5 WorkflowExecutionSignaled   (BUFFERED while WFT started; flushed after the WFT closes)
  6 WorkflowExecutionTerminated
  ```
- `RespondWorkflowTaskFailed` routes `GRPC_MESSAGE_TOO_LARGE` to `TerminateWorkflow`
  (`respondworkflowtaskfailed/api.go:88`), which fails the started WFT first with
  `FORCE_CLOSE_COMMAND`, pins the batch-first id there, then appends the terminated event
  (`workflow/util.go:115`).
- Signals buffer because `bufferEvent` (`historybuilder/event_store.go:263`) returns `true` by default
  for `WorkflowExecutionSignaled`. **Tokeira appends it immediately** (`apply_signal`, `kernel.rs:662`)
  — the documented no-buffering deviation.

So two mechanisms, neither present today:

1. **Event buffering** — hold events admitted during a started WFT; flush after the WFT closes. This is
   the architectural reversal (blast radius: every signal/resolution-during-started-WFT history).
2. **Terminate force-close ordering** — fail the started WFT (`ForceCloseCommand`) before terminate,
   flush buffered between. Tokeira's current `Terminate` (kernel-cancel-terminate Req 3.1) emits only
   `WorkflowExecutionTerminated`.

Plus `WorkflowTaskFailedCause::{ForceCloseCommand, GrpcMessageTooLarge}` (values `17`, `36` in
`failed_cause.proto`).

## 3. The spec

`.kiro/specs/kernel-event-buffering/` — `requirements.md`, `design.md`, `tasks.md`.

- **Requirement 0 (architectural decision) — accepted for Phase 1 (2026-07-01, owner).** It records the
  reversal of the no-buffering deviation and its blast radius. Phase 1 is approved to proceed; the leaf
  stays a classified skip only until Phase 1 lands.
- Phased: **Phase 1 (do this now)** delivers signals/cancel-requested buffering + flush-on-close +
  terminate force-close + new causes + the message-too-large command (the minimum for the raised leaves,
  and sufficient for the entire current suite — see §5). **Phase 2 (deferred)** broadens to
  activity/child/Nexus completion buffering + the `reorderBuffer` rule; pick it up only when a leaf
  demands it.
- `design.md` recommends command shape (a): a dedicated `TerminateOnWorkflowTaskFailed` command so the
  Feature 2 retry path (`apply_workflow_task_failed`) stays unpolluted.

## 4. Way forward — ordered

1. **Implement Phase 1** in `tokeira-kernel` per `tasks.md` (1.1–1.12). Requirement 0 is accepted for
   Phase 1, so no further sign-off is needed to start. Pure kernel; ground-truth every behaviour to
   v1.31.0 and cite in comments (AGENTS §8/§9). Verify:
   `cargo clippy -p tokeira-kernel --all-targets --tests -- -D warnings`, `cargo +nightly fmt`,
   `cargo test -p tokeira-kernel`.
2. **Wire the edge dependency** (1.13): `RespondWorkflowTaskFailed` is a no-op stub today — route
   `GrpcMessageTooLarge` to the new kernel command; every other cause to the Feature 2 retry command.
   Owned under `edge-unimplemented.md` / `api-conformance-wft-completion`.
3. **Remove the skip** (1.14) for `TestTerminateWorkflowOnMessageTooLargeFailure`; confirm green.
4. **Update docs** (1.12): `020-kernel.md` + `state.rs:187` describe the buffered model.
5. **Do NOT start Phase 2.** It is deferred with an explicit trigger (§5); starting it early re-opens the
   "no reorder subsystem" principle for no conformance gain.

## 5. Phase-2 scope boundary (why it's deferred)

`TestWorkflowTestSuite` is exactly `tests/workflow_test.go` (16 tests). The **only** buffered-event test
in it is `TestTerminateWorkflowOnMessageTooLargeFailure` (`:1023`), which buffers a **signal** — pure
Phase 1. No test in the suite buffers an activity/child/Nexus **completion** during a started WFT, so
the `reorderBuffer` rule is never exercised here. Phase 1 clears the whole suite.

**Phase 2 trigger (later):** a leaf in a dedicated activity / child-workflow / Nexus suite where an async
completion arrives while a WFT is in flight and the assertion pins the reordered order (completions after
other buffered events). Only then implement Phase 2 (`tasks.md` 2.1–2.3).

## 6. Guardrails (unchanged, restated)

- **Kernel purity holds.** Buffering/flush/force-close are pure state-machine logic — legitimate kernel
  work, not a §2 violation. The "no kernel additions" conformance rule is a stop-and-raise signal for
  *leaf* fixes; this is the deliberate spec'd version it points to.
- **Ground truth = v1.31.0**, local `../temporal` at tag, cite path + tag. Never web/memory.
- **Skips: registry only, cited reason, never a corpus test body.**
- **tokeira commits:** message via `fsWrite` to `artifacts/cm-*.txt` → `git commit -F` → `rm -rf
  artifacts`; never `git add .`/`-A`; never commit `.claude/` or `runall-results.json`.

## 7. Context map

- `.kiro/specs/kernel-event-buffering/` — this feature (requirements/design/tasks).
- `.kiro/specs/kernel-cancel-terminate/` — current `Terminate` (Req 3.1) that gains the force-close branch.
- `.kiro/specs/kernel-wft-failure-timeout/` — Feature 2 `WorkflowTaskFailed` + `WorkflowTaskFailedCause`
  (extended here).
- `crates/tokeira-kernel/src/{kernel.rs,state.rs}` — `apply_signal:662`, WFT-completed flush point
  `:1497`, buffered-model comment `state.rs:187`.
- `docs/architecture/020-kernel.md:389` — the `Signal` rationale documenting no-buffering.
- `docs/testing/functional-conformance-harness.md`, `docs/HANDOVER-functional-conformance.md` — the
  drive-to-green loop and conventions.
