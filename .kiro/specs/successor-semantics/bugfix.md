# Successor semantics — bugfix requirements

Status: requirements approved 31 October 2026; not an implementation claim. Baseline: Tokeira
`60972ea02b0eebac0a143b9374731ea94ea27050`. Behaviour authority: Temporal server
**v1.31.0**; wire authority: vendored Temporal API **v1.62.11**.

## Summary and scope

The close and successor now share a transaction under
[atomic-successors](../atomic-successors/bugfix.md). Their contents and the decision
to create a cron successor still differ from the targeted server. This change fixes
the following observable successor semantics:

- Retry and cron inherit header, memo, search attributes and links from the
  predecessor's **started event**, rather than dropping them or reading modified
  current values. Explicit continue-as-new also inherits the started event's links.
- A retry preserves the cron schedule so that completing or exhausting that retry
  can resume the schedule.
- A run-timeout retry carries the timeout failure and the started event's last
  completion result.
- A workflow-task failure considers cron after retry is declined, including when a
  retry policy exists, and records the actual retry decision on the failed event.
- Retry and cron apply the same minimum interval between executions as the target.

Explicit continue-as-new already carries its command header and enforces the
default minimum interval. Those are preservation cases, not missing functionality.
This specification does not replace atomic commit, add delivery rows, repair old
chains, or redesign reset, worker versioning, callbacks, or all retry-policy rules.
It does not claim those other surfaces fully conform.

## Glossary

- **Predecessor:** the run whose close creates the successor.
- **Started event:** that predecessor's first `WorkflowExecutionStarted` history
  event, including its event-level links. Tokeira has legacy and V2 internal
  variants; both represent the same public contract.
- **Current state:** the predecessor's state after mutations such as memo updates,
  search-attribute upserts and attachment of later links.
- **Retry decision:** the backoff/`RetryState` result of evaluating failure against
  the retry policy. Declining retry does not by itself decline cron.
- **Execution time:** predecessor start time plus its first-workflow-task delay.
- **Selected backoff:** the retry-policy delay or next-cron delay before applying
  the minimum interval.

## Verified evidence

All Temporal paths below refer to **v1.31.0**, read from the pinned local reference.
Source inspection confirms the mechanisms; the reproductions below have not yet
been executed as regression tests for this specification.

| Surface | Current Tokeira source at the baseline | Targeted behaviour source |
|---|---|---|
| Retry/cron metadata | `crates/tokeira-runtime/src/runtime/workflow_task.rs`, `build_retry_successor_start` and `build_cron_successor_start`: `header: None`, empty links, memo/search attributes from state | `service/history/workflow/retry.go:156-351`, `SetupNewWorkflowForRetryOrCron`: `Header`, `Memo`, `SearchAttributes`, `Input`, `CronSchedule` from started attributes; `Links` from `startLinks` |
| Source of links | `crates/tokeira-runtime/src/successor.rs`, `build_continue_as_new_start`: empty links; workflow-task and timeout plans retain only selected start fields | `service/history/workflow/mutable_state_impl.go:5738-5754`, `AddContinueAsNewEvent`: passes `startEvent.Links`; `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1328-1427`, `handleRetry`/`handleCron`: same source |
| Retry loses cron | `build_retry_successor_start`: `cron_schedule: None` | `SetupNewWorkflowForRetryOrCron`: `CronSchedule: startAttr.CronSchedule`, for both initiators |
| Timeout retry outcome | `crates/tokeira-runtime/src/timeout.rs`, `submit_workflow_timeout`: snapshots pre-close `close_failure`/`close_result`; `successor.rs`, `build_successor_start`: overwrites the successor with those values | `service/history/timer_queue_active_task_executor.go:683-754`: supplies a start-to-close timeout failure and `startAttr.LastCompletionResult` to either retry or cron |
| Workflow-task failure selection | `workflow_task.rs`, `cron_continuation_for_completion`: declines cron whenever a failure has a retry policy; `crates/tokeira-kernel/src/kernel.rs`, `FailWorkflow` arm: terminal retry excludes cron, while cron-only failure hardcodes `InProgress` | `workflow_task_completed_handler.go:779-809`: retry first, cron when no retry backoff; the failed event retains the evaluated `retryState` even when cron follows |
| Minimum delay | Retry/cron builders use the selected backoff directly; `kernel.rs`, `continue_as_new_min_backoff`, already handles explicit continue-as-new | `service/history/workflow/retry.go:323-330`; `service/history/workflow/mutable_state_impl.go:2675-2695`, `ContinueAsNewMinBackoff`; `common/dynamicconfig/constants.go:2785-2789`: default one second |

Wire fields are in `proto/upstream/temporal/api/history/v1/message.proto`:
`WorkflowExecutionStartedEventAttributes` (header 25, memo 22, search attributes 23,
cron schedule 20, continued failure 12, last completion result 13, first-workflow-task
backoff 21), `WorkflowExecutionFailedEventAttributes` (`retry_state` and
`new_execution_run_id`), and `HistoryEvent.links` (302). Explicit continue-as-new's
command header is field 12 of `ContinueAsNewWorkflowExecutionCommandAttributes` in
`proto/upstream/temporal/api/command/v1/message.proto`.

### Outcome distinctions that must survive the fix

Temporal's workflow-task **retry** handler supplies no last completion result
(`handleRetry`, `workflow_task_completed_handler.go:1354-1364`). Its workflow-task
**cron** handler supplies the successful close result, or, on failure, the started
event's carried result (`handleCron:1383-1425`). The **timeout** handler supplies the
started event's carried result for both retry and cron
(`timer_queue_active_task_executor.go:743-754`). A single rule such as “always carry
the last result” would be wrong.

## Minimal reproductions

Use the real in-memory repository and runtime with explicit command/scanner times;
observe persisted history after commit, not only a builder's return value. No test
requires wall-clock sleeps. Repeat applicable cases with each internal started-event
variant and through the edge's history serializer.

1. **Original metadata.** Start with header `H`, links `[L]`, memo `M0` and search
   attributes `S0`. Change memo/search attributes to `M1`/`S1` and attach another
   link. Fail with a retry available, or complete/fail/time out into cron. The new
   started event must contain `H`, `[L]`, `M0`, `S0`. Today it loses header/links and
   carries `M1`/`S1`. Separately, explicit continue-as-new must retain `[L]` while
   using its own command header/memo/search attributes.
2. **Cron through a retry.** Start a cron workflow with a retry policy; fail once
   while retry is available. Complete the retry. The retry's started event must
   retain the schedule, and its completion must create a cron successor. Today the
   retry's started event has no schedule and the chain ends on completion.
3. **Timeout result and error.** Start a run whose started event carries result `R`
   and whose retry policy permits another attempt. Trigger its run timeout with
   the execution deadline still in the future. The retry must carry the timeout
   failure and `R`; pre-close close-result/failure fields are not the source.
4. **Retry exhaustion to cron.** Start a cron workflow with maximum attempts one,
   then fail its first attempt. The failed event must report
   `MaximumAttemptsReached` and link a cron successor with attempt one. Today it
   reports that retry state but has no successor. Repeat with a non-retryable
   failure; include no-policy cron as a retry-state preservation/correction case.
5. **Short executions.** At execution age 100 ms, select a retry delay 100 ms.
   With the default minimum interval one second, the successor must have a
   first-workflow-task delay 900 ms. Today it gets 100 ms. For cron, generate the
   selected delay through the real schedule helper: v1.31.0 rounds it up to whole
   seconds, so 100 ms is not a valid selected cron delay. Exercise an exact-fire
   zero delay where the execution age is below one second, plus ordinary positive
   delays, equality, and a predecessor whose own start was delayed
   (`common/backoff/cron.go:33-63 @ v1.31.0`).

These are independent assertions against the target contract. Comparing the new
builder to a copy of the old builder would preserve the bugs.

## Bug condition C(X)

Let `X` contain a valid predecessor state, its complete started event `E`, a
supported close cause, deterministic evaluation time, retry decision and cron
schedule. Let `O(X)` be the committed close plus successor history (or absence of a
successor). Restrict observation to the fields and selection rules in the policy
table below; unrelated fields are outside this predicate.

`C(X)` holds exactly when at least one of these mismatches occurs:

1. A retry/cron successor's header, memo, search attributes or links differs from
   the corresponding value in `E`, or explicit continue-as-new's links differ from
   `E.links`.
2. A retry successor's cron schedule differs from `E.cron_schedule`.
3. A run-timeout retry's continued failure is not the target timeout failure, or
   its last completion result differs from `E.last_completion_result`.
4. A valid cron continuation exists after retry is declined for a workflow-task
   failure, but no cron successor is committed; or the close's retry state is
   replaced by `InProgress` merely because a cron successor exists.
5. A retry/cron successor's first-workflow-task backoff differs from `B` below.

For execution age `L = evaluation_time - predecessor_execution_time`, selected
backoff `b`, and default minimum interval `m = 1 second`:

```text
B = m - L    if L + b < m
    b        otherwise
```

This is the behaviour of `ContinueAsNewMinBackoff @ v1.31.0`, not a new retry
eligibility test. Minimum-delay adjustment follows selection. Use the execution
time anchor, not the predecessor's wall-clock start. The fixed default is
consistent with the existing
[conformance override contract](../conformance-config-override/requirements.md),
which classifies `history.workflowIdReuseMinimalInterval` as kernel-excluded;
making that setting mutable is not part of this change.

## Field and decision policy

The table enumerates this change's contract surface, not the entire start RPC.
Values come from already admitted commands/history; this change introduces no new
client validation errors or public status codes. Storage/read failures retain
their existing error path and must not produce a partial normal pair commit.

| Field or decision | Explicit continue-as-new | Retry | Cron | Durable effect |
|---|---|---|---|---|
| Header | Command/continued event; already correct | `E.header` | `E.header` | Successor started attributes |
| Links | `E.links` | `E.links` | `E.links` | Successor history event links; exclude links attached later |
| Memo | Command/continued event; preserve | `E.memo` | `E.memo` | Successor started attributes and initial state |
| Search attributes | Command/continued event; preserve | `E.search_attributes` | `E.search_attributes` | Successor started attributes and initial state |
| Cron schedule | Existing command handling is outside this change | `E.cron_schedule` | `E.cron_schedule` | Successor started attributes; future continuation discovery |
| Continued failure after workflow-task failure | Existing command handling outside this change | The failing command's failure | The failing command's failure | Successor started attributes |
| Continued failure after run timeout | Not applicable | Start-to-close timeout failure | Start-to-close timeout failure | Successor started attributes |
| Last completion result after workflow-task failure | Existing command handling outside this change | Absent | `E.last_completion_result` | Successor started attributes |
| Last completion result after workflow-task success | Existing command handling outside this change | Not applicable | This close's result | Successor started attributes |
| Last completion result after run timeout | Not applicable | `E.last_completion_result` | `E.last_completion_result` | Successor started attributes |
| Failure continuation selection | Not applicable | Prefer an available retry | Evaluate if retry is declined | Exactly one successor, or none when neither applies |
| Failed event retry state | Not applicable | Evaluated retry state | Evaluated retry state, including a terminal/no-policy result | Closing `WorkflowExecutionFailed.retry_state` |
| Successor initiator/attempt | Preserve | Retry / predecessor attempt plus one | CronSchedule / one | Successor started attributes |
| First-workflow-task delay | Preserve existing supported minimum-delay behaviour | `B` | `B` | Successor execution time and delayed first-task eligibility |

For successful cron completion, continued failure remains absent. For run timeout,
the target timeout failure is `failure.NewTimeoutFailure("workflow timeout",
TIMEOUT_TYPE_START_TO_CLOSE)` from
`service/history/timer_queue_active_task_executor.go:683 @ v1.31.0`.

## Expected behaviour

### Requirement 1: Started-event metadata

**User story:** As an SDK user, I want a successor to receive the same original
execution metadata as Temporal, so retries and scheduled runs preserve context.

1. WHEN a retry or cron successor is prepared, THE runtime SHALL source its header
   from the predecessor's started event.
2. WHEN any in-scope successor is prepared, THE runtime SHALL source its links
   from the predecessor's started event.
3. WHEN a retry or cron successor is prepared, THE runtime SHALL source its memo
   from the predecessor's started event.
4. WHEN a retry or cron successor is prepared, THE runtime SHALL source its search
   attributes from the predecessor's started event.
5. WHEN a retry successor is prepared, THE runtime SHALL preserve the started
   event's cron schedule.
6. WHEN reading a legacy or V2 internal started event, THE runtime SHALL apply the
   same field-source policy.
7. WHEN metadata is absent or empty in the started event, THE runtime SHALL
   preserve that absence or emptiness in the successor.

### Requirement 2: Outcome-specific continuation data

**User story:** As an SDK user, I want the previous error and result to match the
cause of this successor, so workflow decisions do not consume fabricated data.

1. WHEN a run timeout creates a retry successor, THE runtime SHALL set its
   continued failure to the target start-to-close timeout failure.
2. WHEN a run timeout creates a retry successor, THE runtime SHALL set its last
   completion result from the predecessor's started event.
3. WHEN a workflow-task failure creates a retry successor, THE runtime SHALL keep
   its last completion result absent.
4. WHEN a workflow-task failure creates a cron successor, THE runtime SHALL carry
   the started event's last completion result.
5. WHEN a workflow-task success creates a cron successor, THE runtime SHALL carry
   that close's result as the last completion result.
6. WHEN a run timeout creates a cron successor, THE runtime SHALL carry the
   started event's last completion result.
7. WHEN a workflow-task failure creates a retry or cron successor, THE runtime
   SHALL carry that command's failure as the continued failure.
8. WHEN a run timeout creates a cron successor, THE runtime SHALL carry the target
   start-to-close timeout failure as the continued failure.
9. WHEN a workflow-task success creates a cron successor, THE runtime SHALL leave
   its continued failure absent.

### Requirement 3: Retry then cron

**User story:** As a cron workflow operator, I want the schedule to resume when a
retry is exhausted or disallowed, so the chain does not silently stop.

1. WHEN a workflow-task failure has an available retry continuation, THE runtime
   SHALL select that retry rather than a cron continuation.
2. WHEN retry is declined for a workflow-task failure and a valid cron backoff
   exists, THE runtime SHALL select a cron continuation.
3. WHEN a failed close selects cron, THE kernel SHALL record the evaluated retry
   state on the failed event.
4. WHEN a failed close selects a successor, THE kernel SHALL record that
   successor's run id on the failed event.
5. WHEN neither retry nor cron selects a successor, THE kernel SHALL leave the
   failed event's successor run id absent.
6. WHEN cron is selected after a retry, THE runtime SHALL start its attempt at one.
7. WHEN retry is selected, THE runtime SHALL set the successor's attempt to the
   predecessor's attempt plus one.
8. WHEN either retry or cron is selected, THE runtime SHALL set the successor's
   initiator to the selected kind.

### Requirement 4: Minimum interval

**User story:** As an operator, I want short retry/cron runs to respect the target's
minimum interval, so rapid chains cannot bypass the execution throttle.

1. WHEN a retry or cron successor is prepared, THE runtime SHALL apply the
   backoff `B` defined above to its first workflow task.
2. WHEN computing `B`, THE runtime SHALL measure predecessor lifetime from its
   execution time, including its own first-workflow-task delay.
3. IF the selected delay already meets the minimum interval, THEN THE runtime
   SHALL preserve the selected delay.
4. WHILE the first-workflow-task delay has not elapsed, THE runtime SHALL keep
   that successor's first workflow task ineligible for dispatch.

## Preservation set

### Requirement 5: Preserve existing correct paths

1. WHEN a normal close creates a successor, THE runtime SHALL retain the paired
   fenced commit required by `atomic-successors`.
2. WHEN the storage size fallback is used, THE runtime SHALL use the same corrected
   successor fields as the normal pair path.
3. WHEN a close is retried after an OCC conflict, THE runtime SHALL retain the
   selected successor identity across that retry.
4. WHEN explicit continue-as-new carries a header, memo or search attributes, THE
   runtime SHALL preserve its existing command-sourced values.
5. WHEN explicit continue-as-new has a short execution lifetime, THE kernel SHALL
   preserve its supported default minimum-delay behaviour.
6. WHEN a close has no successor, THE runtime SHALL retain its single-run commit.
7. WHEN a continuation is retried or replayed, THE runtime SHALL preserve the
   existing one-successor idempotency guarantee.
8. WHEN a workflow reaches its execution timeout, THE runtime SHALL preserve the
   existing prohibition on continuing that expired chain.
9. WHEN this change is implemented, THE kernel SHALL remain a deterministic
   single-run state machine without I/O or mutable configuration reads.

The preservation domain includes empty metadata, metadata unchanged since start,
retry-only workflows, cron-only completion/failure/timeout, retry-preferred
cron workflows, delayed starts, parent/child linkage, current-run CAS, history
notifications, and failure before pair commit. Preservation is scoped to correct
existing behaviour; it does not require keeping the mismatches described by C(X).

## Required reconciliation in design and implementation

- `atomic-successors` bugfix 2.4/3.3 and design Property 3 deliberately freeze the
  old builders. Amend those preservation statements and their legacy-equivalence
  test to admit these approved corrections while retaining atomicity coverage.
- `workflow-retry-chain` requirements 1.1.2, 1.2.3–4, 5.1.2 and Properties P2–P4
  currently equate “retry terminal” with “no successor.” Qualify them for absence
  of cron. Update the design and test plan's corresponding predicates together.
- `runtime-continue-as-new` retains command-sourced fields; add the started-event
  link rule without changing its atomic persistence contract.
- `edge-history-parent-chain` requirements 3–4 need cause-specific clarification:
  a timeout has a continued failure, and a workflow-task retry does not always
  carry a historical successful result. Reconcile dependent design properties.
- Keep the minimum-interval constant's existing conformance override disposition.
  Any proposed configuration or persistent-format change needs a separate explicit
  design decision; neither is required by these behavioural requirements.

## Review and verification boundary

The requirements were approved before authoring [design.md](design.md) and
[tasks.md](tasks.md). The first implementation task must be a property-based exploration test
that asserts absence of C(X) and fails on the unfixed baseline, with one independent
counterexample for each bug family. If it unexpectedly passes, investigate before
implementing a fix. Subsequent tests must cover both correction and preservation,
including persisted edge-visible histories and the pinned continue-as-new, retry
and cron corpus suites.

Runtime code is unchanged by this document. No new dependency, migration or
production configuration is proposed.
