# Hand-over — workflow retry chain (TestWorkflowRetry / TestWorkflowRetryFailures)

**Author:** Claude (raise, functional-conformance drive) · **Implemented by:** Kiro · **Date:** 2026-07-02 · **For:** the conformance drive (Claude)
**Status:** ✅ IMPLEMENTED (in-repo) — Requirement 0 accepted (2026-07-02, owner); spec
[`.kiro/specs/workflow-retry-chain/`](../.kiro/specs/workflow-retry-chain/requirements.md) Phases 1–5
landed and verified (fmt/clippy/tests/doc green, `tokeirad` builds). **One operator-gated step remains**
— run the functional conformance harness, then remove the two skip entries and flip them to
required-pass. See §4.

**Classification of the two leaves:** currently registry **skips**, reason updated to **real-gap
(implemented; pending harness confirmation)**. They flip to required-pass once a harness run confirms
green. See §4 and "Skip vs FAIL" below.

> **TL;DR.** The two retry leaves need **workflow-retry-on-failure**: a run that fails with a
> `RetryPolicy` must close as Failed carrying `new_execution_run_id` and a successor run (attempt N+1)
> must start. This was raised (two kernel changes → stop-and-raise, not inline-patched), spec'd, accepted,
> and is now **implemented** (kernel + runtime + edge, §3). The only thing left is an operator harness run
> to confirm green and flip the two skips to required-pass (§4). §1–§2 below are the original ground truth;
> read them before touching any assertion detail. A shared edge bug (RunKey leaked as wire run_id) that
> also broke these tests was fixed separately (see `docs/HANDOVER-functional-conformance.md`).

## 1. The tests (ground truth)

`tests/workflow_test.go:1440-1520 @ v1.31.0`, helpers `tests/testcore/functional_test_base.go:588`.

- **TestWorkflowRetry**: start with `RetryPolicy{MaximumAttempts:3, BackoffCoefficient:1, no
  InitialInterval (server defaults 1s)}`; the workflow FailWorkflow-s each attempt. Asserts: 3 runs
  chained by run id; each attempt's 5-event history
  (`WorkflowExecutionStarted{Attempt:N}, WorkflowTaskScheduled, WorkflowTaskStarted,
  WorkflowTaskCompleted, WorkflowExecutionFailed/Completed`); `execution_time` backoff math against
  the 1s default; Describe/History consistency per attempt.
- **TestWorkflowRetryFailures**: retry stops on `MaximumAttempts` and on
  `NonRetryableErrorTypes` (failure type match); asserts the final `WorkflowExecutionFailed`
  has `retry_state` MaximumAttemptsReached / NonRetryableFailure and NO `new_execution_run_id`.

## 2. v1.31.0 mechanics

- FailWorkflow during WFT completion → `AddFailWorkflowEvent(..., retryState, newExecutionRunID)`
  (`workflow_task_completed_handler.go:788-798`): the failed event carries
  `new_execution_run_id` ONLY when retry continues.
- Retry decision + backoff: `service/history/workflow/retry.go:32-116` + `mutable_state_impl.go:1630-1649`
  (non-retryable error types; maximum attempts; execution-expiration cap; backoff =
  initial_interval × coefficient^(attempt-1), capped by maximum_interval).
- Successor start: same workflow id/type/queue/input/policy/timeouts; `attempt = N+1`
  (`retry.go:307-310`), `continued_execution_run_id` = predecessor, inherited
  `first_execution_run_id`/`first_run_started_at`, `continued_failure` = the close failure,
  first-WFT backoff = computed interval.
- Workflow retry-policy defaults applied at StartWorkflowExecution
  (`EnsureDefaults`, `common/retrypolicy/retry_policy.go:32-67` via `workflow_handler.go:6600`).
- `FixFollowEvents` (`service/history/api/get_history_util.go:556-616`): CLOSE_EVENT-filtered
  history for clients without the follows-next-run-id capability rewrites a closing event that
  carries `new_execution_run_id` into a synthetic ContinuedAsNew so `GetWorkflowHistory` follows
  the chain — the corpus helper relies on this.

## 3. What landed (implemented + verified in-repo)

- **Kernel:** `HistoryEventKind::WorkflowExecutionFailed.new_execution_run_id: Option<RunId>`
  (`#[serde(default)]`, mirrors `WorkflowExecutionTimedOut`). New `Command::WorkflowTaskCompletedWithRetry`
  + `RetryContinuation { Retry { new_run_id } | Terminal { retry_state } }`. The `FailWorkflow` arm records
  the runtime's decision (`InProgress` + successor id, or terminal `retry_state` + `None`); cron branch and
  no-continuation fallback preserved. Kernel property tests P1–P4, P6.
- **Runtime** (`runtime/workflow_task.rs`): `retry_continuation_for_completion` evaluates `retry.go`
  semantics (proto-decoding non-retryable check, max-attempts, execution-expiration, exponential backoff)
  — runtime-side, kernel stays proto-free. `start_retry_successor` starts the attempt-N+1 run mirroring the
  continue-as-new successor path (inherited config, `attempt=N+1`, `continued_execution_run_id`, inherited
  first-run identity, `continued_failure`, backoff-delayed first WFT, idempotent). Wired into
  `complete_workflow_task` (cron-first precedence). End-to-end integration tests in `runtime_lane.rs`.
- **Edge:** workflow retry-policy `EnsureDefaults` on `StartWorkflowExecution` + `SignalWithStart`
  (`workflow_retry_policy_with_defaults`). The Failed-event serializer projects `new_execution_run_id`.
- **Docs:** `docs/architecture/020-kernel.md` (Workflow-level retry section) updated to the shipped model.

**Deliberate deferrals (not gaps):**
- ~~`FixFollowEvents` legacy-client rewrite~~ — **SUPERSEDED (2026-07-03, harness confirmation):** the
  deferral rationale was wrong for the corpus. `TestWorkflowRetry` explicitly simulates a pre-2021 Java
  SDK (`headers.SetVersionsForTests(ctx, "1.3.1", ClientNameJavaSDK, ...)`,
  tests/workflow_test.go:1498-1516 @ v1.31.0) and asserts the synthetic ContinuedAsNew on
  CLOSE_EVENT-filtered reads. Implemented during harness confirmation: capability is purely
  header-driven (`supported-features` metadata contains `follows-next-run-id`,
  `version_checker.go:152` — no version-map fallback), and the rewrite mirrors
  `makeFakeContinuedAsNewEvent` (`get_history_util.go:588-640`) in the edge proto layer. Capable
  clients still get the real close event. (Spec Req 3.2 — now shipped.)
- Timeout-retry successor — `WorkflowExecutionTimedOut` already carries `new_execution_run_id`; the
  timeout retry-continuation + successor are the same mechanism, deferred until a timeout-retry case
  demands it.

## 4. Remaining step for the conformance drive (Claude)

The implementation is done and verified in-repo; the only thing left is the **operator-invoked** harness
confirmation (it needs the Go corpus + a running `tokeirad`, per
`docs/testing/functional-conformance-harness.md` — not runnable from the Kiro side). To finish:

1. Build `tokeirad` (it carries all the changes above): `cargo build -p tokeirad`.
2. Remove the two entries from `tests/testcore/tokeira_conformance_skip.go`
   (`TestWorkflowTestSuite/TestWorkflowRetry`, `TestWorkflowTestSuite/TestWorkflowRetryFailures`) — their
   reasons already say "implemented; pending harness confirmation … flip to required-pass once green."
3. Run the harness for those leaves and confirm both GREEN.
4. Flip them to required-pass in the coverage report (real-gap → pass), and check off Phase 4.2 in the
   spec `tasks.md`.

If a leaf is not green, capture the specific assertion and route it back — do **not** re-implement the
chain (it exists and is unit/integration-tested); the likely suspects are conformance-only details
(exact `execution_time`/backoff assertions, per-attempt history shape) rather than the mechanism.

## Skip vs FAIL (reconciliation)

An earlier revision of this doc said the two leaves would "remain honest FAILs (not skips)". That is
**superseded**: they are now registry **skips** in
`tests/testcore/tokeira_conformance_skip.go`, classified **real-gap (DEFERRED GAP, raised)**.

Why the change from FAIL to skip: after the RunKey/run-id edge fix, the leaves no longer fail fast — the
corpus helper (`functional_test_base.go:588`) successfully reads attempt 1's history and then polls
**indefinitely** for the attempt-2 successor run that never starts, hanging until the go-test timeout and
**aborting the parallel suite with 0 outcomes recorded** (observed: `TestWorkflowRetryFailures` hung
2m56s → 8-min timeout, masking ~30 passing tests). A hang-to-timeout leaf erases the suite's signal
entirely, so a cited skip is strictly better than a live FAIL — the same class as the existing
`TestNexusOperationSyncCompletion` suite-abort skip. The skip stays classified real-gap (remove-when-
lands), so it flips to required-pass the moment the spec's Phase 4 runs; it is not out-of-scope and does
not rot.
