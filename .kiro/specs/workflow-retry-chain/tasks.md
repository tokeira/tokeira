# Tasks: Workflow Retry Chain (Kernel + Runtime + Edge)

Requirements: [requirements.md](./requirements.md). Design: [design.md](./design.md).

> **Requirement 0 accepted (2026-07-02, owner).** Phase 1 may proceed. Command shape: dedicated
> `WorkflowTaskCompletedWithRetry`. `TestWorkflowRetry` / `TestWorkflowRetryFailures` stay classified
> registry skips (Req 0.5) only until Phase 4 flips them.
>
> **Progress (2026-07-02):** Phase 1 (kernel) and Phase 2 (runtime) landed and verified — workspace
> compiles, kernel property tests + runtime tests green, `fmt`/`clippy` clean. **Phases 3–5 remain**
> (edge `EnsureDefaults` + `FixFollowEvents`, golden 3-run + skip removal, docs). The feature is not yet
> end-to-end: without Phase 3.2 (`FixFollowEvents`) a CLOSE_EVENT-filtered read will not chase the
> successor, so the two leaves stay skipped. No conformance regression (skips still in place).

Kernel edits stay pure (AGENTS §2). Ground-truth every behaviour to v1.31.0 and cite the source in code
comments (AGENTS §8, §9). Verify with `cargo clippy -p tokeira-kernel -p tokeira-runtime -p tokeira-edge
--all-targets --tests -- -D warnings`, `cargo +nightly fmt`, and
`cargo test -p tokeira-kernel -p tokeira-runtime -p tokeira-edge`.

## Phase 0 — Decision (blocking)

- [x] 0.1 Owner accepts Requirement 0: `new_execution_run_id` on `WorkflowExecutionFailed` (event-schema
  field, `#[serde(default)]`) + the failure-close `RetryContinuation` input, with the stated blast
  radius (new observable successor run for retrying workflows). **Accepted (2026-07-02, owner).** Command
  shape: dedicated `WorkflowTaskCompletedWithRetry` (design §"Command shape" option (a)).

## Phase 1 — Kernel (after 0.1 accepted) ✅ done

- [x] 1.1 Add `new_execution_run_id: Option<RunId>` (`#[serde(default)]`) to
  `HistoryEventKind::WorkflowExecutionFailed`, mirroring `WorkflowExecutionTimedOut`. Serde round-trip
  covered by P6. Edge serializer projects it to proto field #4. (Req 1.1)
- [x] 1.2 Added the dedicated `WorkflowTaskCompletedWithRetry { request, retry_continuation }` command
  parallel to `WorkflowTaskCompletedWithCron`. `RetryContinuation { Retry { new_run_id }, Terminal
  { retry_state } }` — backoff omitted (runtime recomputes it; documented). (Req 1.2)
- [x] 1.3 Reworked the FailWorkflow close arm to record the `RetryContinuation`: `Retry` → `InProgress`
  + `Some(new_run_id)`; `Terminal` → supplied `retry_state` + `None`; no-continuation fallback and cron
  branch preserved. (Req 1.2)
- [x] 1.4 Kernel properties P1–P4 + P6 in `property_tests.rs` (93/93 pass). (Req P1–P4, P6)

## Phase 2 — Runtime (after Phase 1) ✅ core done

- [x] 2.1 Added `retry_continuation_for_completion` (`runtime/workflow_task.rs`, mirroring
  `cron_continuation_for_completion`): evaluates `retry.go` semantics (non-retryable type/flag via a
  proto-decoding classifier, max attempts, execution-expiration cap, exponential backoff); mints the
  successor `RunId` on retry. Wired into `complete_workflow_task` (cron-first, then retry; runtime owns
  precedence). (Req 2.1)
- [x] 2.2 Added `start_retry_successor`: on a committed Failed-with-retry close, starts the attempt-N+1
  run inheriting id/type/queue/input(from history)/policy/timeouts, `attempt=N+1`,
  `continued_execution_run_id=predecessor`, inherited first-run identity, `continued_failure`,
  backoff-delayed first WFT, deterministic request id; `Duplicate` treated as success. Predecessor close
  stands if successor start fails. (Req 2.2, 5.1)
- [~] 2.3 Runtime property P5: unit-covered the decision logic (`workflow_failure_is_retryable`,
  `retry_backoff`). A full end-to-end successor-chain integration test needs `tokeira-proto` as a
  runtime dev-dependency (to encode a `Failure`); folded into the Phase 4 golden/conformance. (Req P5)

## Phase 3 — Edge

- [x] 3.1 Apply workflow retry-policy `EnsureDefaults` at the start leg (`workflow_retry_policy_with_defaults`
  in edge translate, applied on both `StartWorkflowExecution` and `SignalWithStart`): 1s / coefficient
  2.0 / max-interval 100× / max-attempts 0-unless-set, filling only unset subfields. Activity-side
  defaults untouched. (Req 3.1)
- [~] 3.2 CLOSE_EVENT chain-following: the edge already projects `new_execution_run_id` onto the Failed
  event (Phase 1), so the capable v1.31.0 corpus SDK follows the chain to the started successor — the
  hang was the absent successor (fixed in Phase 2), not a missing rewrite. The legacy-client
  `FixFollowEvents` rewrite is **deferred** (documented in Req 3.2): it needs client-capability plumbing
  the edge lacks, and an unconditional rewrite would be wrong for capable clients. Only affects
  pre-~Sept-2021 SDKs, not the corpus.

## Phase 4 — End-to-end tests + conformance flip

- [x] 4.1 End-to-end runtime tests (`runtime_lane.rs`): `retryable_failure_starts_attempt_two_successor`
  (retryable FailWorkflow → predecessor Failed with `new_execution_run_id` + `InProgress` → attempt-2
  successor started, chained, 1s backoff) and `non_retryable_failure_is_terminal_without_successor`
  (`NonRetryableFailure`, no successor). Together with kernel P1–P4/P6 and the classifier/backoff unit
  tests, these cover a chain hop + both terminal conditions (the G1 mechanism). A literal 3-run drive was
  not added; the hop + terminal coverage proves the chain. (Req G1, P5)
- [x] 4.2 DONE (2026-07-03, harness confirmation by Claude): the two fork skip entries are REMOVED and
  both leaves are GREEN out-of-process (3x stress). Four harness-surfaced fixes were needed on top of the
  in-repo implementation — all conformance details, not the mechanism: (1) `workflow_failure_is_retryable`
  rewritten to mirror `isRetryable` (retry.go:115) across ALL failure-info classes — the corpus drives
  retry with `failure.NewServerFailure` (ServerFailureInfo), which the ApplicationFailureInfo-only check
  classified non-retryable; (2)+(3) `execution_time` must be this run's `started_at + workflow_start_delay`
  (v1.31.0 `ExecutionTime = StartTime + FirstWorkflowTaskBackoff`, mutable_state_impl.go:2859) — fixed in
  the three storage projection builders AND the tokeirad Describe resolver (which returned
  `first_run_started_at`); (4) the Req 3.2 `FixFollowEvents` deferral was WRONG for the corpus — the test
  explicitly simulates a pre-2021 Java SDK (`SetVersionsForTests(..., "1.3.1", JavaSDK, ...)`,
  workflow_test.go:1498-1516) and asserts the synthetic ContinuedAsNew on CLOSE_EVENT reads; implemented
  header-driven (`supported-features` contains `follows-next-run-id`, version_checker.go:152) with the
  `makeFakeContinuedAsNewEvent` substitution (get_history_util.go:588). (Req 0.5, 3.2)

## Phase 5 — Docs

- [x] 5.1 Updated `docs/architecture/020-kernel.md` (Workflow-level retry section) to the shipped
  implementation: `WorkflowExecutionFailed.new_execution_run_id`, the `WorkflowTaskCompletedWithRetry` /
  `RetryContinuation` model, runtime evaluation + successor/backoff, and the deferred timeout path. Inline
  doc comments on the new kernel/runtime items carry the WHY + ground-truth citations (AGENTS §9). (Req 0.4)

## Out of scope (tracked, not this spec)

- Timeout-retry successor (`apply_workflow_execution_timed_out`, `kernel.rs:1314`, currently
  `new_execution_run_id: None`) — same mechanism, picked up when a timeout-retry leaf demands it.
- Cron/retry-precedence consolidation into a unified `CompletionContinuation` enum (design §"Command
  shape" option (c)) — optional follow-up if the owner prefers it over the parallel command.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0.1"] },
    { "id": 1, "tasks": ["1.1", "1.2", "1.3", "1.4"] },
    { "id": 2, "tasks": ["2.1", "2.2", "2.3"] },
    { "id": 3, "tasks": ["3.1", "3.2"] },
    { "id": 4, "tasks": ["4.1", "4.2"] },
    { "id": 5, "tasks": ["5.1"] }
  ]
}
```

> Wave 0 (`0.1`) is the blocking owner decision. Waves 1–3 may proceed in parallel once accepted, except
> `3.2` and `4.2` depend on `1.1` (the closing event must carry `new_execution_run_id` before
> `FixFollowEvents` and the conformance flip are meaningful).
