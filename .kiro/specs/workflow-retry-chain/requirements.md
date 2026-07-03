# Requirements Document: Workflow Retry Chain (Kernel + Runtime + Edge)

## Introduction

This document captures the requirements for **workflow-retry-on-failure**: when a workflow started with
a `RetryPolicy` fails, v1.31.0 closes the run as `Failed` carrying `new_execution_run_id` and starts an
attempt-N+1 successor run (inherited input/policy/timeouts, run-id-chained, backoff-delayed first
workflow task). It was raised by the functional-conformance drive (see
[docs/HANDOVER-workflow-retry-chain.md](../../../docs/HANDOVER-workflow-retry-chain.md)) as the
load-bearing gap behind `TestWorkflowRetry` and `TestWorkflowRetryFailures`
(`tests/workflow_test.go:1440-1520 @ v1.31.0`).

tokeira has the per-run retry state (`WorkflowState.{retry_policy, attempt, first_execution_run_id,
original_execution_run_id}`) and two architectural templates for close-time continuation — cron
(`Command::WorkflowTaskCompletedWithCron` + `cron_continuation_for_completion`) and continue-as-new
(the successor-start block in `crates/tokeira-runtime/src/lane.rs`) — but **no retry continuation**. The
failure-close arm hardcodes the retry outcome (`kernel.rs:3167`:
`retry_state = InProgress if retry_policy.is_some()`) with no stop-condition derivation, no
`new_execution_run_id`, and no successor.

Two of the required pieces touch `tokeira-kernel` — the pure state machine the conformance campaign
treats as frozen for corpus fixes (`docs/testing/functional-conformance-harness.md`: "No kernel
additions … a fix that seems to need the kernel is a signal to stop and raise it"). Per that discipline
this is **raised as a spec**, not inline-patched. Requirement 0 records the decision for owner sign-off;
nothing below is implemented until it is accepted.

### The behaviour being matched (ground truth, v1.31.0)

- **TestWorkflowRetry** (`tests/workflow_test.go:1440 @ v1.31.0`): start with
  `RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1.0}` (no `InitialInterval` → server default 1s);
  the workflow fails each attempt. Asserts **3 runs chained by run id**; each attempt's **5-event**
  history (`WorkflowExecutionStarted{attempt: N}`, `WorkflowTaskScheduled`, `WorkflowTaskStarted`,
  `WorkflowTaskCompleted`, `WorkflowExecutionFailed`); `execution_time` reflecting the backoff; and
  Describe/History consistency per attempt.
- **TestWorkflowRetryFailures** (`tests/workflow_test.go:~1490 @ v1.31.0`): retry stops on
  `MaximumAttempts` and on a `NonRetryableErrorTypes` match; asserts the final `WorkflowExecutionFailed`
  carries `retry_state` `MaximumAttemptsReached` / `NonRetryableFailure` and **no**
  `new_execution_run_id`.

Server mechanics:

- FailWorkflow during WFT completion → `AddFailWorkflowEvent(..., retryState, newExecutionRunID)`
  (`service/history/workflow/workflow_task_completed_handler.go:788-798 @ v1.31.0`): the failed event
  carries `new_execution_run_id` **only** when retry continues.
- Retry decision + backoff: `service/history/workflow/retry.go:32-116` +
  `mutable_state_impl.go:1630-1649 @ v1.31.0` (non-retryable error-type match; maximum attempts;
  execution-expiration cap; `backoff = initial_interval × coefficient^(attempt-1)`, capped by
  `maximum_interval`).
- Successor start: same workflow id/type/task-queue/input/retry-policy/timeouts, `attempt = N+1`,
  `continued_execution_run_id = predecessor`, inherited `first_execution_run_id` /
  `first_run_started_at`, `continued_failure = close failure`, first-WFT delayed by the backoff
  interval (`retry.go:307-310`).
- Start-time defaulting: workflow retry-policy defaults applied at `StartWorkflowExecution`
  (`EnsureDefaults`, `common/retrypolicy/retry_policy.go:32-67` via `workflow_handler.go:6600`).
- `FixFollowEvents` (`service/history/api/get_history_util.go:556-616 @ v1.31.0`): for a CLOSE_EVENT-
  filtered read by a client without the follows-next-run-id capability, a closing event that carries
  `new_execution_run_id` is rewritten into a synthetic `ContinuedAsNew` so `GetWorkflowExecutionHistory`
  follows the chain. The corpus helper (`tests/testcore/functional_test_base.go:588`) relies on this.

### Architectural decision required (blocking)

Adopting the retry chain adds a **persisted event-schema field** (`new_execution_run_id` on
`WorkflowExecutionFailed`) and a **kernel continuation input** to the failure-close arm, and introduces a
**new observable successor run** on retry. Per the AGENTS change-classification a change to state/event
format is **Architectural** and requires **spec update AND explicit approval**. This document IS that
spec update; it does not presume approval. Requirement 0 records the decision and its blast radius so
the owner can accept it explicitly.

## Glossary

- **Kernel**: the pure deterministic state machine (`tokeira-kernel`) — no I/O, async, storage, metrics
  (AGENTS §2). Retry-state derivation and event authoring are pure and stay within these bounds; the
  retry *evaluation* (backoff/expiration/non-retryable) and *successor start* are runtime concerns.
- **Retry chain**: the sequence of runs sharing one `first_execution_run_id`, each linked to its
  predecessor by `continued_execution_run_id`, produced when a workflow with a `RetryPolicy` fails and
  is retried.
- **RetryContinuation**: the runtime-computed decision passed into the failure-close command — either
  "retry, successor run id R, next backoff B" or "terminal, retry_state S" — mirroring the existing
  `CronContinuation`.
- **Stop condition**: a `RetryState` other than `InProgress` that ends the chain
  (`MaximumAttemptsReached`, `NonRetryableFailure`, `RetryPolicyNotSet`, and — for the timeout path,
  out of scope here — `Timeout`).
- **Successor run**: the attempt-N+1 run started by the runtime after a Failed-with-retry close commits,
  with a backoff-delayed first workflow task.
- **CLOSE_EVENT-filtered read**: `GetWorkflowExecutionHistory` with
  `HistoryEventFilterType = CLOSE_EVENT`, which the corpus helper uses to follow the chain and which
  `FixFollowEvents` makes chase the successor run.

## Requirements

---

## Requirement 0: Architectural Decision — Adopt the Workflow Retry Chain

> **ACCEPTED (2026-07-02, owner).** The retry-chain's event-schema field and failure-close
> `RetryContinuation` input are approved. Command shape: a **dedicated `WorkflowTaskCompletedWithRetry`**
> command (design §"Command shape" option (a)). Phase 1 may proceed; the two conformance leaves stay
> classified skips only until Phase 4 flips them.

**User Story:** As the Tokeira owner, I want the retry-chain's event-schema and kernel-continuation
additions recorded explicitly with their blast radius, so that adopting them is a deliberate accepted
decision and not a silent state-format/behaviour change.

#### Acceptance Criteria

1. THE decision SHALL record the two kernel-touching changes: (a) adding
   `new_execution_run_id: Option<RunId>` to `HistoryEventKind::WorkflowExecutionFailed`
   (`crates/tokeira-kernel/src/event.rs`), mirroring the field already present on
   `WorkflowExecutionTimedOut` (`event.rs:130`); and (b) replacing the hardcoded failure-close
   `retry_state` (`kernel.rs:3167`) with a runtime-supplied `RetryContinuation` input, mirroring the
   existing `CronContinuation` path.
2. THE decision record SHALL state the blast radius: the `WorkflowExecutionFailed` event gains an
   optional field (old transition-log records remain readable via `#[serde(default)]`); the
   failure-close transition conditionally links a successor; and a retrying workflow now produces a
   **new successor run** — an observable behaviour change for every workflow started with a retry policy
   that fails.
3. THE change SHALL be feasible without a breaking transition-log migration: the new event field is
   `Option<RunId>` with `#[serde(default)]`, so it round-trips and prior records deserialize with
   `None`.
4. WHEN Requirement 0 is accepted, THE architecture/state docs SHALL be updated to describe the retry
   chain (the `WorkflowExecutionFailed` field and the continuation model), documentation being part of
   the deliverable (AGENTS §9).
5. UNTIL Requirement 0 is accepted, THE conformance leaves it unblocks (`TestWorkflowRetry`,
   `TestWorkflowRetryFailures`) SHALL remain classified registry skips with a cited reason (per the
   no-kernel-additions discipline), not force-passed. As live FAILs they hang the corpus helper's
   successor-history poll to the go-test timeout and abort the parallel suite, so the skip is required
   to preserve the rest of the suite's signal (same class as the `TestNexusOperationSyncCompletion`
   suite-abort skip).

---

## New Types and State

### Requirement 1.1: `WorkflowExecutionFailed` Carries `new_execution_run_id`

**User Story:** As a Tokeira developer, I want the failed event to carry the successor run id, so that
history readers (and `FixFollowEvents`) can follow a retry chain exactly as v1.31.0.

#### Acceptance Criteria

1. THE `HistoryEventKind::WorkflowExecutionFailed` variant SHALL gain a field
   `new_execution_run_id: Option<RunId>` annotated `#[serde(default)]`, mirroring
   `WorkflowExecutionTimedOut` (`event.rs:130`).
2. `new_execution_run_id` SHALL be `Some(run_id)` if and only if retry continues (the run is Failed and
   an attempt-N+1 successor is being started); it SHALL be `None` on terminal failure.
   (`workflow_task_completed_handler.go:788 @ v1.31.0`.)
3. THE field SHALL serialize and deserialize without loss, and prior records lacking the field SHALL
   deserialize to `None` (round-trip property).

### Requirement 1.2: `RetryContinuation` Input to the Failure-Close Arm

**User Story:** As a Tokeira developer, I want the kernel's failure-close to record a runtime-computed
retry decision rather than hardcoding it, so the retry stop-condition and successor linkage are correct
and the retry *evaluation* stays out of the pure kernel.

#### Acceptance Criteria

1. THE failure-close path SHALL accept a `RetryContinuation` describing either "retry" (successor
   `RunId`, and the backoff already applied to the successor's first WFT by the runtime) or "terminal"
   (the derived `RetryState`).
2. WHEN the `RetryContinuation` is "retry", THE Kernel SHALL emit `WorkflowExecutionFailed` with
   `retry_state = InProgress` and `new_execution_run_id = Some(successor)`.
3. WHEN the `RetryContinuation` is "terminal", THE Kernel SHALL emit `WorkflowExecutionFailed` with the
   supplied terminal `RetryState` (`MaximumAttemptsReached`, `NonRetryableFailure`, or
   `RetryPolicyNotSet`) and `new_execution_run_id = None`.
4. THE existing behaviour for a run with **no** retry policy SHALL be preserved exactly: terminal,
   `retry_state = RetryPolicyNotSet`, `new_execution_run_id = None`.
5. THE `RetryContinuation` is carried on a **dedicated `WorkflowTaskCompletedWithRetry` command**
   (decided 2026-07-02; design.md §"Command shape" option (a)), parallel to the existing
   `WorkflowTaskCompletedWithCron`; the runtime owns retry-vs-cron precedence. The kernel contract is
   the resulting transition. All `RetryState` variants required already exist in `command.rs`
   (`InProgress`, `NonRetryableFailure`, `MaximumAttemptsReached`, `RetryPolicyNotSet`); no new enum
   variant is introduced.

---

## Retry Evaluation and Successor Start (Runtime)

### Requirement 2.1: Retry Evaluation Mirrors `retry.go`

**User Story:** As a Tokeira developer, I want the retry decision computed in the runtime with v1.31.0
semantics, so the kernel stays pure and the decision is correct.

#### Acceptance Criteria

1. THE runtime SHALL compute the `RetryContinuation` in a `retry_continuation_for_completion` helper
   mirroring `cron_continuation_for_completion` (`crates/tokeira-runtime/src/runtime/workflow_task.rs`),
   invoked on a FailWorkflow completion.
2. THE evaluation SHALL apply, per `retry.go:32-116` and `mutable_state_impl.go:1630-1649 @ v1.31.0`:
   non-retryable error-type match → `NonRetryableFailure`; `attempt >= MaximumAttempts` (when
   `MaximumAttempts > 0`) → `MaximumAttemptsReached`; workflow-execution-expiration exceeded → terminal;
   otherwise retry with `backoff = InitialInterval × BackoffCoefficient^(attempt-1)`, capped by
   `MaximumInterval`.
3. WHEN retry applies, THE runtime SHALL mint the successor `RunId` and pass it (with the computed
   backoff) into the failure-close command so the kernel records the linkage.

### Requirement 2.2: Successor Run Start

**User Story:** As a Tokeira developer, I want the attempt-N+1 run started after the failed run commits,
mirroring the continue-as-new successor path, so the chain is durable and observable.

#### Acceptance Criteria

1. WHEN a Failed-with-retry close commits, THE runtime lane SHALL start the successor run, mirroring the
   continue-as-new / cron successor block (`crates/tokeira-runtime/src/lane.rs`), with: same
   `workflow_id`, `workflow_type`, `task_queue`, `input`, `retry_policy`, and timeouts;
   `attempt = predecessor.attempt + 1`; `continued_execution_run_id = Some(predecessor run id)`;
   inherited `first_execution_run_id` and `first_run_started_at`; the predecessor's close failure carried
   as the successor's `continued_failure`.
2. THE successor's first workflow task SHALL be delayed by the computed backoff interval (the successor
   starts in a backoff state, first WFT scheduled after the delay), matching v1.31.0.
3. IF the successor start fails, THE predecessor's committed Failed close SHALL still be returned (the
   successor start is a derived effect after the authoritative close, mirroring the existing
   continue-as-new "predecessor commit returned even when successor start fails" behaviour in
   `lane.rs`).

---

## Edge Behaviour

### Requirement 3.1: Start-Time Retry-Policy Defaults

**User Story:** As a Tokeira developer, I want workflow retry-policy defaults applied at start, so the
backoff math matches v1.31.0 (the tests assert the 1s `InitialInterval` default).

#### Acceptance Criteria

1. WHEN `StartWorkflowExecution` (and the start leg of any start-like RPC) supplies a `RetryPolicy`
   with unset fields, THE Edge SHALL apply the v1.31.0 workflow retry-policy defaults
   (`EnsureDefaults`, `retry_policy.go:32-67 @ v1.31.0`): `InitialInterval` 1s, `BackoffCoefficient` 2.0,
   `MaximumInterval` 100×`InitialInterval`, `MaximumAttempts` 0 (unlimited) unless set.
2. THE activity-side `EnsureDefaults` already landed SHALL NOT be regressed; this requirement adds the
   workflow-start-side application.

### Requirement 3.2: CLOSE_EVENT-Filtered Reads Follow the Chain

**User Story:** As an SDK client (and the corpus helper), I want a CLOSE_EVENT-filtered history read to
follow the retry chain, so that reading the "final" history chases successor runs instead of stopping at
the first failed run.

Ground truth (`get_history_util.go:556-616 @ v1.31.0`): `FixFollowEvents` rewrites the close event into a
synthetic `ContinuedAsNew` **only** for clients that lack the `FollowsNextRunID` capability
(pre-~Sept-2021 SDKs). A capable client instead reads `new_execution_run_id` off the real
`WorkflowExecutionFailed` event and follows the chain itself.

#### Acceptance Criteria

1. WHEN `GetWorkflowExecutionHistory` returns a `WorkflowExecutionFailed` close event for a retrying run,
   THE Edge SHALL populate its `new_execution_run_id` (Req 1.1 / edge projection), so a capability-aware
   client — including the v1.31.0 conformance corpus's Go SDK — follows the chain to the (now started,
   Req 2.2) successor. This is what unblocks the corpus helper's chain-following poll
   (`functional_test_base.go:588`); the earlier hang was the *absent successor*, not a missing rewrite.
2. THE legacy-client rewrite (synthetic `ContinuedAsNew` for a non-capable client) is **deferred**:
   tokeira's edge does not yet parse client `supported-features`, so the `!FollowsNextRunID` guard cannot
   be evaluated, and an unconditional rewrite would be **wrong** for capable clients (it would turn a
   `Failed` close into a `ContinuedAsNew`). It only affects pre-~Sept-2021 SDKs, which the pinned corpus
   is not. Tracked as a follow-up gated on adding client-capability plumbing to the edge; called out here
   so it is not mistaken for an oversight.

---

## Retry Stop Conditions and History Shape

### Requirement 4.1: Stop Conditions (TestWorkflowRetryFailures)

**User Story:** As an SDK client, I want retry to stop exactly when v1.31.0 stops, so terminal failures
are reported with the right `retry_state` and no dangling successor.

#### Acceptance Criteria

1. WHEN `attempt` reaches `MaximumAttempts`, THE final `WorkflowExecutionFailed` SHALL carry
   `retry_state = MaximumAttemptsReached` and no `new_execution_run_id`; no successor SHALL start.
2. WHEN the failure's type matches a `NonRetryableErrorTypes` entry, THE final
   `WorkflowExecutionFailed` SHALL carry `retry_state = NonRetryableFailure` and no
   `new_execution_run_id`; no successor SHALL start.

### Requirement 4.2: Per-Attempt History Shape

**User Story:** As an SDK client, I want each attempt's history to match v1.31.0 exactly, so run-by-run
inspection is conformant.

#### Acceptance Criteria

1. Each attempt's history SHALL be the 5-event sequence `WorkflowExecutionStarted{attempt: N}`,
   `WorkflowTaskScheduled`, `WorkflowTaskStarted`, `WorkflowTaskCompleted`, `WorkflowExecutionFailed`.
2. Successive runs SHALL be linked: run N's `WorkflowExecutionFailed.new_execution_run_id` equals run
   N+1's run id, and run N+1's `WorkflowExecutionStarted` carries `attempt = N+1`,
   `continued_execution_run_id = run N`, and the inherited `first_execution_run_id`.
3. THE successor `WorkflowExecutionStarted` `attempt` and the run's `execution_time` SHALL reflect the
   applied backoff (the 1s default for TestWorkflowRetry's `BackoffCoefficient: 1.0`).

---

## Structural Invariants

### Requirement 5.1: Chain Integrity

#### Acceptance Criteria

1. FOR ALL retry chains, every run except the first SHALL carry
   `continued_execution_run_id = Some(predecessor)` and all runs SHALL share one
   `first_execution_run_id`.
2. FOR ALL terminal failures (`MaximumAttemptsReached`, `NonRetryableFailure`, `RetryPolicyNotSet`),
   `WorkflowExecutionFailed.new_execution_run_id` SHALL be `None`.
3. FOR ALL Failed-with-retry closes, exactly one successor run SHALL be started (no duplicate successors
   on retry of the close command — the successor start is idempotent on the predecessor's committed
   `new_execution_run_id`).

---

## Property Tests

### Requirement P1: Retry-Eligible Failure Links a Successor
1. FOR ALL open WorkflowState with a retry-eligible `RetryPolicy` (attempt below max, retryable
   failure) and FOR ALL FailWorkflow completions, WHEN the failure-close is applied with a "retry"
   `RetryContinuation`, THE emitted `WorkflowExecutionFailed` SHALL carry `retry_state = InProgress` and
   `new_execution_run_id = Some(_)`.
   `// Feature: workflow-retry-chain, Property 1`

### Requirement P2: Maximum-Attempts Failure Is Terminal
1. FOR ALL open WorkflowState at `attempt == MaximumAttempts`, WHEN the failure-close is applied with a
   "terminal(MaximumAttemptsReached)" `RetryContinuation`, THE emitted `WorkflowExecutionFailed` SHALL
   carry `retry_state = MaximumAttemptsReached` and `new_execution_run_id = None`.
   `// Feature: workflow-retry-chain, Property 2`

### Requirement P3: Non-Retryable Failure Is Terminal
1. FOR ALL open WorkflowState with a `RetryPolicy` whose `NonRetryableErrorTypes` matches the failure,
   WHEN the failure-close is applied with a "terminal(NonRetryableFailure)" `RetryContinuation`, THE
   emitted `WorkflowExecutionFailed` SHALL carry `retry_state = NonRetryableFailure` and
   `new_execution_run_id = None`.
   `// Feature: workflow-retry-chain, Property 3`

### Requirement P4: No Retry Policy Preserves Existing Behaviour
1. FOR ALL open WorkflowState with `retry_policy == None`, WHEN the failure-close is applied, THE emitted
   `WorkflowExecutionFailed` SHALL carry `retry_state = RetryPolicyNotSet` and
   `new_execution_run_id = None` (unchanged from today).
   `// Feature: workflow-retry-chain, Property 4`

### Requirement P5: Successor Chain Fields
1. FOR ALL Failed-with-retry closes, the started successor `StartRequest` SHALL carry
   `attempt = predecessor.attempt + 1`, `continued_execution_run_id = Some(predecessor)`, the inherited
   `first_execution_run_id` / `first_run_started_at`, and a backoff-delayed first WFT.
   `// Feature: workflow-retry-chain, Property 5`

### Requirement P6: Event Round-Trip
1. FOR ALL `WorkflowExecutionFailed` events (with and without `new_execution_run_id`), serialization
   SHALL round-trip without loss, and a record lacking the field SHALL deserialize to `None`.
   `// Feature: workflow-retry-chain, Property 6`

---

## Golden Transition Test

### Requirement G1: Three-Run Retry Chain
1. WHEN a workflow with `RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1.0}` fails on each
   attempt, THE assembled histories SHALL match the v1.31.0 corpus: three runs, each with the 5-event
   attempt history, runs 1→2 and 2→3 linked by `new_execution_run_id`/`continued_execution_run_id`, and
   run 3's `WorkflowExecutionFailed` carrying `retry_state = MaximumAttemptsReached` and no
   `new_execution_run_id` (`tests/workflow_test.go:1440 @ v1.31.0`).

---

## Out of Scope / Dependencies

- **Timeout-retry successor (adjacent, deferred).** `apply_workflow_execution_timed_out`
  (`kernel.rs:1314`) already emits `WorkflowExecutionTimedOut` with `new_execution_run_id: None`
  hardcoded and starts no successor — the **same** gap as failure-retry, served by the **same**
  `RetryContinuation` + successor mechanism. It is **not** exercised by `TestWorkflowRetry` /
  `TestWorkflowRetryFailures` (both fail via FailWorkflow, not timeout), so it is out of scope here and
  picked up when a timeout-retry leaf demands it. Called out so it is not mistaken for an oversight.
- **Cron/retry precedence (design concern).** A workflow may carry both a cron schedule and a retry
  policy. The runtime owns the precedence decision (retry evaluated before cron) before selecting the
  continuation; see design.md.
- **Reset-with-retry, activity-retry** (already landed) are out of scope.
- **Edge dependency ordering.** Requirements 3.1 and 3.2 are edge/runtime work; 3.2 (`FixFollowEvents`)
  is what actually stops the corpus hang and flips the leaves, and depends on Requirement 1.1 landing so
  the closing event carries the successor id.
