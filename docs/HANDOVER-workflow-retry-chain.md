# Hand-over — workflow retry chain (TestWorkflowRetry / TestWorkflowRetryFailures)

**Author:** Claude (functional-conformance drive) · **Date:** 2026-07-02 · **For:** Kiro
**Status:** RAISE — needs kernel work; stop-and-raise per the conformance discipline.

> **TL;DR.** The two retry leaves need **workflow-retry-on-failure**: a run that fails with a
> `RetryPolicy` must close as Failed carrying `new_execution_run_id` and a successor run
> (attempt N+1) must start. tokeira has per-run `retry_policy`/`attempt` state and the cron
> continuation machinery, but no retry continuation. Two of the pieces are kernel changes →
> raised, not inline-patched. A shared edge bug (RunKey leaked as wire run_id) that also
> broke these tests was fixed separately (see `docs/HANDOVER-functional-conformance.md` drive).

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

## 3. What tokeira has / lacks

Has: `WorkflowState.{retry_policy, attempt, first_execution_run_id, original_execution_run_id}`,
cron continuation (`WorkflowTaskCompletedWithCron` + `cron_continuation_for_completion`,
runtime/workflow_task.rs) as the architectural template, and (post-`20388323`) activity retry
EnsureDefaults.

Lacks (kernel — the raise):
1. `HistoryEventKind::WorkflowExecutionFailed.new_execution_run_id: Option<RunId>`
   (`#[serde(default)]`, mirroring `WorkflowExecutionTimedOut`, event.rs:130).
2. A `RetryContinuation { new_run_id, retry_state }` input alongside `cron_continuation` on the
   WFT-completed command; the FailWorkflow arm currently hardcodes
   `retry_state = InProgress if retry_policy.is_some()` (kernel.rs FailWorkflow arm) with no
   successor linkage and no MaximumAttemptsReached/NonRetryableFailure derivation.

Lacks (runtime/edge — implementable once the kernel lands):
3. `retry_continuation_for_completion` (runtime): evaluate retry.go semantics
   (non-retryable types / max attempts / expiration / backoff) and mint the successor run id.
4. Successor start on committed Failed-with-retry (runtime lane, mirroring the ContinuedAsNew
   successor block in lane.rs).
5. Workflow retry-policy EnsureDefaults at start (edge translate; activity-side already landed).
6. `FixFollowEvents` for CLOSE_EVENT-filtered reads (edge get_workflow_execution_history).

## 4. Recommendation

Extend `.kiro/specs/api-conformance-wft-completion/` (or a dedicated `workflow-retry-chain` spec)
with the kernel Requirement (items 1-2) for owner sign-off, then implement 3-6 in
runtime/edge. Items 5-6 are independently landable but do not flip the leaves alone.
Until then the two leaves remain honest FAILs (not skips — the feature is in-claim).
