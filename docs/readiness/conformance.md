# Conformance Readiness — Temporal v1.31.0

> Sibling of [`delivery.md`](./delivery.md). **This is the single status surface for how far tokeira
> has progressed toward Temporal v1.31.0 compliance.** It is the *numerator*. The *denominator* — what
> full v1.31.0 compliance **means** — is defined in [`../conformance/v1.31.0/`](../conformance/v1.31.0/README.md).
>
> Supersedes the status tables previously scattered across `temporal-functional-conformance/reference/
> FINDINGS.md` (its Status ledger) and `api-conformance-tracker/tracker.md` (its progress counts). Those
> remain as **detail-by-reference**: FINDINGS for the deep per-cluster investigation + Implementer
> Mandate; tracker for the RPC→spec index.

**Target:** `TEMPORAL_SERVER_COMPAT = 1.31.0`, proto `v1.62.11` (`crates/tokeira-build-info/src/pinned.rs`).
**Last updated:** 2026-06-22 · **Single contributor for status:** Kiro.

## How to read this

Every area carries one of three honest states — we do **not** assert compliance we have not measured:

- ✅ **Verified** — exercised and passing (unit/property/golden, or the Tier-2 functional corpus, or a
  cited end-to-end run). Evidence noted.
- 🟡 **Implemented, unverified** — code exists and the happy path works, but the full conformance
  surface has not been measured against the corpus.
- ⬜ **Outstanding** — not implemented / not started.
- ⏸ **Deferred** · ⛔ **Out of public scope** (depends on an internal surface tokeira does not front).

A green check in code is not compliance; a measured pass against v1.31.0 ground truth is. (See the
Implementer Mandate in FINDINGS — "a wrong guess behind a green check bakes in non-conformance.")

## The three conformance tiers

| Tier | What it proves | Owning spec | State |
|------|----------------|-------------|:-----:|
| Compatibility surface/metadata | The claim is explicit, queryable, pinned | `temporal-compatibility`, `temporal-compatibility-surface` | 🟡 |
| Tier-1 in-process oracle | Responses + history match v1.31.0, RPC coverage gate | `conformance-harness` (`tokeira-conformance` crate) | ⬜ not built |
| Tier-2 functional corpus | Temporal's own Go suites pass over gRPC | `temporal-functional-conformance` | 🟡 partial |

## Tier-2 functional corpus — cluster status

From the canonical run (corpus @ v1.31.0; 100 entrypoints; 1501 per-test outcomes: 1194 fail / 267
unfinished / 19 pass / 21 skip at the 2026-06-09 baseline, then targeted fixes below).

| Cluster | Area | State | Measured | What remains |
|---------|------|:-----:|:--------:|--------------|
| C4a | Nexus endpoint admin CRUD | ✅ | impl; 13 edge tests | Operator-measure the ~17 admin tests; author proptests P1–P4. |
| C5a | Completion-callback admission validation | ✅ | done | `allowedAddresses` (2 sub-cases) deferred (deployment policy). |
| C5b | Other admission validators (links, …) | ✅ | done | Residuals driven by a corpus re-run. |
| C6 | Over-rejection (cron, nil/empty SA+memo) | ✅ | done | Full corpus re-run pending. |
| C1 | Standalone / first-class activity RPCs | 🟡 | **1 pass / 31 fail** | SA admission validation, Describe fidelity, count-by-id; chasm-activity timeout/retry (~20 tests) needs a spec. Owned by `activity-executions-first-class`. |
| C2 | Worker deployment / versioning | 🟡 | 19/19 (1 suite) | **Untriaged: `TestVersioningFunctionalSuite` (406), `TestDeploymentVersionSuite` (68), `TestVersioning3FunctionalSuite` (13), `TestWorkerRegistryTestSuite` (7).** Legacy v0.x version-sets = deliberate deviation. |
| C3 | Visibility list/query + search attributes | 🟡 | — | Run `TestAdvancedVisibilitySuite`/`…Legacy` for residual query surface (ORDER BY / BETWEEN / STARTS_WITH / keyword IN / null close-time). |
| C4b | Nexus operation execution / task transport | 🟡 | 2 suites unmeasured | Async completion-callback delivery (`nexus-async-completion`, in progress); permanent e2e gRPC round-trip test; **measure `TestNexusApiTestSuiteWith{TemporalFailures,LegacyErrorPaths}` (40+40, never run) + `TestNexusWorkflowTestSuite` (2).** |
| C7 | Lifecycle / describe `NotFound` | ⏸ | — | Re-triage after C1–C4 (many are cascades). |
| C9 | `unfinished` panic siblings | ⬜ | 0/267 | Fix the entrypoint panic; 267 siblings then resolve to real pass/fail. |
| C8 | Internal-surface / admin-service tests | ⛔ | — | Out of public scope by construction. |

**Biggest unknowns (unmeasured denominator):** C2 versioning (~494 tests untriaged), C4b Nexus
op-execution (~82 unmeasured), C9 (267 unfinished). Until these are measured, the overall conformance
percentage is **not** known — establishing real denominators is the next audit priority.

**Order of attack:** C1 → C3 (cheap re-run) → C2 (triage) → C9 (panic fix, reclassifies 267) → C7
(re-triage) → C4b (async-completion + e2e + measure 82). Done: C4a, C5a, C5b, C6.

## Drive-to-green ledger (suite-by-suite, per `functional-test-order.md`)

Fix-to-green campaign (`docs/HANDOVER-functional-conformance.md`): a suite is **clean** when every
test is green or a classified skip with a cited registry reason — zero unclassified non-pass.

| Tier | Suite | Result | Date | Notes |
|------|-------|--------|------|-------|
| 1.7 | `TestWorkflowFailuresTestSuite` | ✅ **CLEAN — 3 pass / 0 fail / 0 skips** (3× stress; Tiers 1.1–1.6 re-verified unregressed) | 2026-07-04 | Baseline 0/3 (harness panic + missing validation + visibility parse error). Landed four fixes. (1) **Partial-history license**: `is_sticky_match` now requires sticky-QUEUE dispatch (dispatch queue ≠ run queue), not worker-identity affinity — the degraded empty-queue hint armed on every WFT start made a transient retry's normal-queue redelivery attach suffix-only history, and the harness panicked slicing `events[PreviousStartedEventId:]` (13 vs 7 attached); v1.31.0 trims only when the run's sticky queue is set (`setHistoryForRecordWfTaskStartedResp`, recordworkflowtaskstarted/api.go:272-278; `IsStickyTaskQueueSet`, api.go:418). The reserved-start sync-match path also stopped claiming sticky. (2) **Req B.6 timestamp fidelity**: late-materialized transient Scheduled/Started events stamp the task's ACTUAL schedule/start times via new `emit_at` — the leaf asserts the persisted started time equals the poll-observed one, ≥1s before the close event (`workflowTask.ScheduledTime/StartedTime` in `AddWorkflowTaskCompletedEvent`, workflow_task_state_machine.go:768-800). (3) **RecordMarker validation + wire-message model**: empty MarkerName fails the WFT with new `BAD_RECORD_MARKER_ATTRIBUTES` (`ValidateRecordMarkerAttributes`, command_attr_validator.go:194-211); kernel `Reject::InvalidCommandAttributes.message` became `Option<String>` mirroring v1.31.0's nullable `causeErr`, the runtime composes the wire message (`"{cause}: {causeErr}"` / bare cause per `workflowTaskFailedCause.Message()`) and persists it as the WFT-failed event's `ServerFailure` — the corpus asserts `"BadRecordMarkerAttributes: MarkerName is not set on RecordMarkerCommand."` verbatim while Tier 1.6's bare `"UnhandledCommand"` still holds. (4) **Visibility filter conjunction**: `split_top_level` is now quote- and BETWEEN-aware — the legacy list-closed conversion emits `CloseTime BETWEEN 'a' AND 'b' AND WorkflowId = 'x'` and the old `split_once(" AND ")` ate the range connective ("unsupported filter expression"). Sticky S4 (fail clears sticky, no attempt increment — workflow_task_state_machine.go:1010-1015) located but NOT demanded: these leaves never arm a real sticky queue; stays open. |
| 1.6 | `TestTransientTaskSuite` | ✅ **CLEAN — 2 pass / 0 fail / 1 classified skip** (3× stress; Tiers 1.1–1.5 re-verified unregressed) | 2026-07-04 | Baseline 1/2. Landed the **UnhandledCommand contract**: a close command (Complete/Fail/Cancel/ContinueAsNew) with buffered events REJECTS the completion; the runtime persists the `WorkflowTaskFailed(UNHANDLED_COMMAND)` transition (unless a transient attempt keeps failing on another cause — dropped to time out) and errors the call with `INVALID_ARGUMENT("UnhandledCommand")` — mirroring v1.31.0's handler/API split (`hasBufferedEventsOrMessages` close guards + respondworkflowtaskcompleted/api.go:455-485,739-742). This REVISED the K4 seam: the kernel no longer converts internally — it rejects with the cause, the lane boundary now preserves typed rejects (`KernelRejected` wrapper, display-compatible with every existing string matcher), and `complete_workflow_task` runs the persist-then-error contract; the invalid-cancel goldens moved to the reject contract and buffering property P5 now models worker-close-past-buffer as impossible. The buffered flush resets the retry to a real attempt-1 task (Tier-1.2 rule (i)) — the leaf's `WithExpectedAttemptCount(1)` pins it. Skip: `TestTransientWorkflowTaskHistorySize` — requires `OverrideDynamicConfig(HistorySizeSuggestContinueAsNew=20KB)`; established OverrideDynamicConfig-class registry skip (fork). |
| 1.5 | `TestUserTimersTestSuite` + `TestWorkflowTimerTestSuite` | ✅ **CLEAN — 1 pass / 0 fail + 2 pass / 0 fail, 0 skips** (3× stress each; Tiers 1.1–1.4 re-verified unregressed) | 2026-07-04 | Baseline: UserTimers already clean; WorkflowTimer 1/1 — `TestCancelTimer_CancelFiredAndBuffered` died on `kernel rejected command: unknown timer` (the timer fired mid-WFT, was appended immediately and removed from state, so the same WFT's CancelTimer found nothing). Landed the **event-buffering Phase 2 timer slice** (spec `kernel-event-buffering`; the deferral trigger — "until a completion-during-started-WFT leaf demands it" — met by this leaf): `TimerFired` now buffers during a started WFT (`should_buffer`), and `CancelTimer` on a fired-and-buffered timer DELETES the buffered `TimerFired` so only `TimerCanceled` reaches history (`GetAndRemoveTimerFireEvent` in `AddTimerCanceledEvent`, mutable_state_impl.go @ v1.31.0); with the buffer emptied the close-flush emits nothing and no spurious follow-up WFT is scheduled. A truly-unknown CancelTimer now FAILS THE WORKFLOW TASK with the new `BAD_CANCEL_TIMER_ATTRIBUTES` cause via the K4 invalid-command seam (previously a gRPC-level command reject). Two golden pins added (buffered-fired cancel spine; unknown-cancel WFT failure). |
| 1.4 | `TestStickyTqTestSuite` | ✅ **CLEAN — 2 pass / 0 fail / 0 skips** (3× stress; Tiers 1.1–1.3 re-verified unregressed) | 2026-07-04 | Baseline 0/2 (both leaves waited for a `WorkflowTaskTimedOut {SCHEDULE_TO_START}` event tokeira could never write). Landed the **sticky task-queue model** (raise `docs/HANDOVER-sticky-taskqueue.md` S1–S3+S5 as one gating unit per Kiro's in-doc §7; S4 deferred to Tier 1.6): `StickyAffinity` carries the sticky queue + per-dispatch schedule-to-start timeout (the edge validated the queue name and discarded it); sticky-held WFTs dispatch ONTO the sticky queue with `PendingWorkflowTask.schedule_to_start_deadline` (attempt-1-guarded; empty-name pre-S1 encodings degrade to the sync-match hint); `WorkflowTaskTimeoutType::ScheduleToStart` accepted on an UNSTARTED task — real timed-out event, sticky cleared, attempt NOT incremented (`ApplyWorkflowTaskTimedOutEvent`, workflow_task_state_machine.go:263-268), fresh attempt-1 reschedule on the normal queue; `Command::ResetSticky` (real `ResetStickyTaskQueue` RPC) clears affinity while the dispatched task's deadline survives (leaf 2's exact pin). Runtime: the WFT timeout scanner gained a `ScheduleToStart` kind (lane post-commit tracks unstarted deadlines; recovery unchanged semantics). Two integration bugs found by the bar: `commit.rs` removed unstarted pending tasks from timeout tracking (start-to-close-era logic erasing the S2S entry), and the `ReturnNewWorkflowTask` eager-return claimed from the normal queue while the WFT sat on the sticky queue (broke Tier 1.2's heartbeating leaf until fixed — claims now follow the actual dispatch queue). |
| 1.3 | `TestActivityTestSuite` + `TestActivityClientTestSuite` | ✅ **CLEAN — 10 pass / 0 fail / 0 skips + 6 pass / 0 fail / 0 skips** (3× stress each; Tiers 1.1 + 1.2 re-verified unregressed) | 2026-07-03 | Baseline 2/8 + 0/6. Landed in two waves. **Non-kernel** (`e44b0b20`): present-zero SDK timeouts are UNSET not insta-deadlines (the dominant failure mode; `timer_sequence.go:268-271`, sdk `internal_event_handlers.go:616`) + `validateAndNormalizeTimeouts` derivation; retry backoff-delayed dispatch + retry-expiration classification (`retry.go:70-113`); durable `last_failure` bookkeeping (`activity.go:82`); scanner retry-on-timeout with `TemporalTimeout:<Type>` classification + terminal ScheduleToClose conversion (`timer_queue_active_task_executor.go:281-362`); typed `ErrActivityTaskNotFound` with the exact `consts/const.go:44-45` message; by-id force-completion (`respondactivitytaskcompleted/api.go:89-105`); Describe `heartbeat_details`. **Kernel raise K1–K4** (`docs/HANDOVER-activity-kernel-gaps.md`, Req 0 accepted by Kiro in-doc; landed incrementally `c2758f33`/`6616144d`/`61cdf155`/`9e4d46cf`): resolutions carry `retry_state`; `ActivityTaskTimedOut` carries the timeout failure incl. `last_heartbeat_details`; durable worker identity (start/heartbeat/retry) + Describe `LastWorkerIdentity` fallback (`workflow/activity.go:159-166`); `RequestCancelActivity` keyed by `scheduled_event_id` with durable `cancel_requested`, not-started immediate cancel (`ACTIVITY_ID_NOT_STARTED`), and the **fail-WFT-with-cause error-model seam** (`Reject::InvalidCommandAttributes` → WFT failure, `failWorkflowTaskOnInvalidArgument` @ v1.31.0). K3 verification also surfaced+fixed a worker-flagged non-retryable failure that retried (marker-string bug; `isRetryable` flag short-circuit, retry.go:139-147). Zero registry skips — both suites fully green. |
| 1.2 | `TestWorkflowTaskTestSuite` | ✅ **CLEAN — 8 pass / 0 fail / 1 classified skip** (3× stress; Tier 1.1 re-verified unregressed 3×) | 2026-07-03 | Baseline 4/5/0, no hangs (Tier-1.1 groundwork carried the regular-WFT + raw-history cases). Landed: the **transient-WFT model** (spec `transient-wft`, Req 0 accepted — supersedes Feature-2's per-attempt-event design): attempt>1 WFTs live off-history with virtual ids (`mutable_state_impl.go:2250`, `workflow_task_state_machine.go:322-597`), only attempt-1 fail/timeout persists its event, transient force-close writes nothing (`workflow/util.go:118-120`), buffered-flush + new-events-by-start conversions reset to attempt-1 real events, success materializes Scheduled+Started late, and poll/GetHistory synthesize the virtual suffix (CLI/UI excluded, `get_history_util.go:427`). Phase A (buffered-flush-resets-retry) by Kiro; Phase B kernel completion + runtime/edge synthesis by Claude. Remaining skip: `TestWorkflowTaskHeartbeatingWithEmptyResult` — permanent `OverrideDynamicConfig` out-of-scope (WFT heartbeat timeout; owner decision, no knob). |
| 1.1 | `TestWorkflowTestSuite` | ✅ **CLEAN — 32 pass / 0 fail / 2 classified skips** (3× stress) | 2026-07-03 | Session-start baseline: suite could not finish (history-pagination infinite loop read as a hang), then 5 pass / 29 fail. Landed: history empty-page-token (corpus-wide unblock), start-response status/link fidelity, request-id dedup + terminate-on-conflict, per-NS-TQ rejection, activity retry defaults, incremental eager-WFT history (`20388323`, `d3fd2b7a`, `2d534260`), kernel event-buffering Phase 1 + force-close ordering (`4f70f6dc`, spec `kernel-event-buffering`), OnConflictOptions attach history fidelity, RunKey/run-id wire split, search-attribute wire codec + banned-predefined admission. 2026-07-03: the workflow retry chain landed (spec `workflow-retry-chain`, implemented by Kiro; harness confirmation surfaced 4 conformance fixes — ServerFailureInfo retryability per retry.go:115, execution_time = start + first-WFT backoff in projection AND Describe, and the FixFollowEvents legacy-client rewrite the corpus exercises via a simulated 1.3.1 Java SDK) — `TestWorkflowRetry`/`TestWorkflowRetryFailures` unskipped and GREEN. Remaining skips: `multiOp` (MultiOperation deferred → `api-conformance-multi-operation`), `OnConflictOptions_failed_max_callbacks` (OverrideDynamicConfig class). |

## Tier-1 + compatibility infra — outstanding

- **`conformance-harness`**: the `tokeira-conformance` crate is **not yet built** — TestCluster
  fixture, WorkerPoller, ExpectedHistory matcher, the 121-RPC coverage manifest + gate, three-way
  reconciliation, report, CLI, CI wiring.
- **`temporal-compatibility`** (tasks.md, 9 top-level tasks open): matrix-completeness properties (3),
  kernel `cfg_feature!` adoption (4), edge `dispatch_rpc` adoption (5), the Buffa/connect-rust
  compatibility service (7), `tkr compat` (8) and `tkr ci` (9) CLIs, the Dagger compatibility module
  (10) + versioned build/lockfile policy (11), final verification (14). Release-process aspects feed
  [`infra.md`](./infra.md).
- **`temporal-compatibility-surface`**: ✅ complete — the queryable matrix spine both tiers consume.

## Active conformance specs (detail-by-reference)

- [`functional-test-order.md`](./functional-test-order.md) — the order to drive Temporal's functional
  suites (Tier-2) against `tokeirad`, in/deferred/out partition, plus the in-process metrics-capture
  analysis.
- [`edge-unimplemented.md`](./edge-unimplemented.md) — current flat indicator of public-edge RPCs
  answering `UNIMPLEMENTED`, split into in-scope gaps vs intentional. Generated from the edge handlers.
- `nexus-async-completion` — async Nexus completion (C4b blocker for Odori's durable path); Wave 0 done,
  Wave 1 handed to Claude (`docs/HANDOVER-nexus-async-completion.md`).
- `workflow-id-conflict-policy-concurrency` — done (conflict-policy concurrency; `TestNexusWorkflowTestSuite`).
- `temporal-functional-conformance/reference/FINDINGS.md` — deep per-cluster investigation, ground-truth
  citations, and the Implementer Mandate. **Status lives here; investigation detail lives there.**
