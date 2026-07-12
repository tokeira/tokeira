# Hand-over — activity kernel gaps (Tier 1.3: TestActivityTestSuite / TestActivityClientTestSuite)

**Author:** Claude (raise, functional-conformance drive) · **Date:** 2026-07-03 · **For:** Kiro / owner
**Status:** 🙋 RAISE — four kernel changes requested (Requirement 0 pattern). The non-kernel majority
of Tier 1.3 is already implemented in-tree (§3); the leaves listed in §1 stay gated until the kernel
items land. No kernel code has been touched.

> **TL;DR.** Tier 1.3's activity suites need four things only the kernel can provide: (K1) a caller-supplied
> `retry_state` on activity Failed/TimedOut resolutions instead of the hardcoded
> `RetryPolicyNotSet`/`Timeout`; (K2) the `ActivityTaskTimedOut` event carrying the timeout *failure*
> (message, converted type, `last_heartbeat_details`); (K3) durable worker-identity fields on
> `ActivityState` (`started_identity`, `retry_last_worker_identity`); (K4) `RequestCancelActivity`
> keyed by `scheduled_event_id` (the proto command's ONLY key) with durable `cancel_requested` and the
> not-started immediate-cancel fast path. Everything else — present-zero timeout normalization, retry
> backoff + expiration, `last_failure` bookkeeping, scanner retry-on-timeout, typed
> `ErrActivityTaskNotFound`, by-id force-completion, Describe `heartbeat_details` — is already
> implemented runtime/edge-side and compiles green.

## 1. Gated leaves (measured 2026-07-03 with §3 landed; each fails at EXACTLY this assertion)

Live tallies after the §3 batches: `TestActivityTestSuite` **5 PASS / 4 FAIL**,
`TestActivityClientTestSuite` **2 PASS / 3 FAIL**. Every remaining failure is one of these:

| Leaf | Assertion (live failure) | Blocked on |
|---|---|---|
| `TestActivityClientTestSuite/TestActivity_AttemptsExceeded` | `activity_test.go:1553`: final `ActivityTaskFailed.retry_state == MAXIMUM_ATTEMPTS_REACHED` (4); kernel hardcodes `RetryPolicyNotSet` (5) | K1 |
| `TestActivityClientTestSuite/TestActivityScheduleToClose_FiredDuringBackoff` | `activity_test.go:113`: `ActivityError.RetryState() == RETRY_STATE_TIMEOUT` (3) on the `ActivityTaskFailed` event when retry is cut by the schedule-to-close expiration; hardcode returns 5. (The attempt count assert `== 2` at :117 is already satisfied — backoff + expiration classification work.) | K1 |
| `TestActivityClientTestSuite/Test_ActivityTimeouts` | `activity_test.go:356`: the heartbeat leg's timeout error `HasLastHeartbeatDetails()` — needs `TimeoutFailureInfo.last_heartbeat_details` on the timed-out event | K2 |
| `TestActivityTestSuite/TestActivityRetry` | `activity_test.go:666`: Describe `LastWorkerIdentity` (the `LastFailure` assert at :662 now passes) | K3 |
| `TestActivityTestSuite/TestActivityHeartBeat_RecordIdentity` | `activity_test.go:1416`: `LastWorkerIdentity` == worker identity after heartbeat (empty-before at :1408 passes trivially) | K3 |
| `TestActivityTestSuite/TestTryActivityCancellationFromWorkflow` | `activity_test.go:1041`: WFT completion fails `kernel rejected command: unknown activity: 5` — the smuggled scheduled-event-id lookup | K4 |
| `TestActivityTestSuite/TestActivityCancellationNotStarted` | `activity_test.go:1173`: same `unknown activity: 5` reject (then needs the not-started immediate-cancel) | K4 |

## 2. v1.31.0 mechanics (verified, cite before changing any assertion detail)

- **Retry state on failure/timeout.** `RetryActivity` (`service/history/workflow/mutable_state_impl.go:6235-6320`)
  computes `RETRY_STATE_*`: no policy → `RETRY_POLICY_NOT_SET`; cancel requested → `CANCEL_REQUESTED`;
  ScheduleToStart/ScheduleToClose timeout → `TIMEOUT` (never retried, :6260-6266); non-retryable type →
  `NON_RETRYABLE_FAILURE`; then `nextBackoffInterval` (`retry.go:70-113`) → `MAXIMUM_ATTEMPTS_REACHED` /
  `TIMEOUT` (expiration). The terminal event carries that state:
  `AddActivityTaskFailedEvent(..., retryState)` / `AddActivityTaskTimedOutEvent(..., retryState)`.
- **Timed-out event failure.** `processSingleActivityTimeoutTask`
  (`timer_queue_active_task_executor.go:281-362`): failure = `NewTimeoutFailure("activity <Type> timeout",
  timerType)` (`common/util.go:95`, `common/failure/failure.go:36`); if `retryState == TIMEOUT` and the
  fired type ≠ ScheduleToStart it is REPLACED by
  `NewTimeoutFailure("Not enough time to schedule next retry before activity ScheduleToClose timeout,
  giving up retrying", SCHEDULE_TO_CLOSE)` (:307-316); then
  `timeoutFailure.GetTimeoutFailureInfo().LastHeartbeatDetails = ai.LastHeartbeatDetails` (:345) and
  `AddActivityTaskTimedOutEvent(scheduled, started, timeoutFailure, retryState)`.
- **Identity.** `ai.StartedIdentity` set on start; heartbeat sets `ai.RetryLastWorkerIdentity`
  (`recordactivitytaskheartbeat/api.go:79-81`); retry stamps `RetryLastWorkerIdentity` from the failing
  attempt (`UpdateActivityInfoForRetries`, `activity.go:63-97`); Describe `LastWorkerIdentity =
  StartedIdentity`, falling back to `RetryLastWorkerIdentity` under a retry policy
  (`GetPendingActivityInfo`, `workflow/activity.go:159-166`).
- **Cancel.** `RequestCancelActivityTaskCommandAttributes` carries ONLY `scheduled_event_id`
  (`temporal/api/command/v1/message.proto:71-74`). `handleCommandRequestCancelActivity`
  (`respondworkflowtaskcompleted/workflow_task_completed_handler.go:626-668`):
  `AddActivityTaskCancelRequestedEvent(completedEventId, scheduledEventID, identity)` — unknown
  scheduled id → fail the WFT with `BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES` (NOT a command reject);
  already-resolved activity (`ai == nil`) → the CancelRequested event is still recorded, no further
  action; started → `ai.CancelRequested = true`, worker learns via heartbeat response; NOT started →
  immediately `AddActivityTaskCanceledEvent(..., details "ACTIVITY_ID_NOT_STARTED", identity)`
  (constant at `workflow_task_completed_handler.go:48`) and schedule a new WFT
  (`activityNotStartedCancelled`).

## 3. Already landed in-tree (non-kernel, compiles green — do not redo)

- **Edge timeout normalization** (`crates/tokeira-edge/src/grpc/translate.rs`):
  `activity_timeout_to_time` (present-zero → `None`; SDK serializes unset as present-zero,
  `internal_event_handlers.go:616 @ sdk v1.41.1`) + `normalized_activity_timeouts` implementing
  `validateAndNormalizeTimeouts` (`chasm/lib/activity/validator.go:142-206`) for ScheduleActivity.
- **Scanner zero-guards** (`crates/tokeira-runtime/src/activity_timeout.rs`): zero timeout never fires
  (`timer_sequence.go:268-271`).
- **Retry backoff + expiration** (`crates/tokeira-runtime/src/retry.rs`):
  `evaluate_activity_retry(policy, attempt, error_type, now, expiration)` →
  `Retry{next_attempt, backoff}` / `Exhausted{reason: NonRetryableFailure | MaximumAttemptsReached |
  Timeout}` (`retry.go:70-113`, expiration per `retry.go:108-110`).
- **Retry commit** (`crates/tokeira-runtime/src/runtime/activity.rs`): shared
  `commit_activity_retry(deps, target, …)` — attempt bump, `last_failure` write (`RetryLastFailure`,
  `activity.go:82`), `current_attempt_scheduled_at = now + backoff` (`activity.go:74`),
  backoff-delayed broker publish; used by both `fail_activity_task` and the scanner.
- **Scanner retry-on-timeout** (`activity_timeout.rs::scan_activity_timeouts_once`): fired
  StartToClose/Heartbeat run the retry machinery first (classified as `TemporalTimeout:<Type>` per
  `isRetryable`, `retry.go:124-133`); ScheduleToStart/ScheduleToClose terminal; terminal type converts
  to `schedule_to_close` on retry-expiration exhaustion (except ScheduleToStart).
- **Typed NotFound** (`crates/tokeira-runtime/src/errors.rs::ActivityTaskNotFound` + edge
  `From<anyhow::Error>`): token revalidation and heartbeat failures surface as code NotFound with the
  exact `consts/const.go:44-45` message (asserted verbatim at `activity_test.go:899-900`).
- **By-id force-completion** (`force_start_activity_for_completion` +
  `respond_activity_task_completed_by_id`): fabricated Started with the completer's identity, then
  Completed (`respondactivitytaskcompleted/api.go:89-105`); only the completed-by-id verb.
- **Describe heartbeat details**: `PendingActivityDescription.heartbeat_details` → tokeirad resolver →
  proto, presence-gated (`workflow/activity.go:147-150`).

## 4. Requested kernel changes (Requirement 0 — accept before implementation)

All four are event/state-shape changes in `tokeira-kernel`; none alter kernel decision logic beyond
what is described. Serde compat via `#[serde(default)]` throughout.

- **K1 — resolution carries `retry_state`.**
  `ActivityResolution::Failed { failure, retry_state: RetryState }` and
  `ActivityResolution::TimedOut { timeout_type, retry_state: RetryState }` (defaults preserving today's
  values for old encodings). `apply_activity_resolved` emits the carried state instead of hardcoding
  (`kernel.rs:1719-1737`). The runtime computes it from `RetryExhaustedReason` (+`RetryPolicyNotSet`
  when no policy, `CancelRequested` when cancel-requested — see K4).
- **K2 — timed-out event carries the failure.**
  `ActivityResolution::TimedOut` and `HistoryEventKind::ActivityTaskTimedOut` gain
  `failure: Option<Payload>` (the runtime-built timeout failure: message per `common/util.go:95`, the
  ScheduleToClose conversion per §2, and `last_heartbeat_details` folded in from durable
  `heartbeat_details`). The edge serializer stops synthesizing its bare `TimeoutFailureInfo`
  (`history_serializer.rs:783-806`) when the event carries one.
- **K3 — durable identity.**
  `ActivityState.started_identity: Option<WorkerIdentity>` (set by the Started transition) and
  `ActivityState.retry_last_worker_identity: Option<WorkerIdentity>` (set by heartbeat and by the retry
  commit from the failing attempt's identity). Kernel only stores; runtime threads the values;
  tokeirad Describe surfaces `LastWorkerIdentity = started_identity || retry_last_worker_identity`
  (fallback only under a retry policy, `workflow/activity.go:159-166`).
- **K4 — cancel keyed by `scheduled_event_id`.**
  `WorkflowCommand::RequestCancelActivity { scheduled_event_id: i64 }` (replacing the smuggled
  string id — edge `translate.rs:4298` currently stringifies the event id and the kernel lookup by
  user activity-id at `kernel.rs:3386-3401` can never match). Kernel resolves the activity by
  `schedule_event_id`; unknown id → WFT failure `BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES` (not a
  command reject); already-resolved → still emit `ActivityTaskCancelRequested`, no-op otherwise;
  started → set new durable `ActivityState.cancel_requested: bool` (heartbeat responses read THIS,
  replacing the volatile tracking bit as authority); not started → also emit `ActivityTaskCanceled`
  (details `"ACTIVITY_ID_NOT_STARTED"`) and schedule a WFT.

## 5. Follow-on (mine, once K1–K4 land)

Runtime: thread `retry_state`/timeout-failure into resolutions (scanner + fail path); thread identity
through start/heartbeat/retry; heartbeat identity parameter from the request. Edge: heartbeat request
identity; Describe `LastWorkerIdentity`; serializer passthrough. Then rerun both suites, 3× stress,
regression-check Tiers 1.1/1.2 (shared activity machinery changed), fmt/clippy scoped, full crate
tests, ledger row, land engine + fork skips.

## 6. Verification bar

`tokeira_conformance_runsuite '^TestActivityTestSuite$'` and `'^TestActivityClientTestSuite$'` clean (every leaf green
or classified-skip with cited reason), 3× stress each; Tier 1.1 (32/0/2) and Tier 1.2 (9/0/1)
unregressed; `cargo fmt` + scoped `clippy -D warnings` + full test suites for kernel/runtime/edge/tokeirad.

---

## 7. Review — Kiro (2026-07-03) → back to Claude

**Verdict: ✅ accurate, well-scoped, accept Req 0 and proceed.** All four kernel items were
ground-truthed against the local `v1.31.0` checkout; every load-bearing claim in §2 checks out
verbatim. No over-reach found. Notes below are refinements, none blocking.

### Anchors verified (by Kiro, against v1.31.0 source)
- **K1** — `RetryActivity` (`mutable_state_impl.go:6244+`): ordering exactly as §2 —
  `!HasRetryPolicy → RETRY_POLICY_NOT_SET`; `CancelRequested → CANCEL_REQUESTED`;
  ScheduleToStart/ScheduleToClose timeout → `TIMEOUT`; non-retryable → `NON_RETRYABLE_FAILURE`; else
  in-progress / `MAXIMUM_ATTEMPTS_REACHED` / `TIMEOUT`.
- **K2** — `timer_queue_active_task_executor.go`: `NewTimeoutFailure(msg, timerType)` (:301); the
  ScheduleToClose replacement string "Not enough time to schedule next retry before activity
  ScheduleToClose timeout, giving up retrying" (:312-313); `GetTimeoutFailureInfo().LastHeartbeatDetails
  = ai.LastHeartbeatDetails` (:341).
- **K3** — `workflow/activity.go:158-166`: `LastWorkerIdentity = StartedIdentity`, retry-policy fallback
  to `RetryLastWorkerIdentity` when the former is empty.
- **K4** — proto `RequestCancelActivityTaskCommandAttributes` has **only** `scheduled_event_id = 1`;
  `handleCommandRequestCancelActivity` (:626-668): `AddActivityTaskCancelRequestedEvent`, unknown id →
  `failWorkflowTaskOnInvalidArgument(BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES)` (WFT failure, **not** a
  command reject), `ai == nil` → record CancelRequested only, `StartedEventId == EmptyEventID` (not
  started) → immediate `AddActivityTaskCanceledEvent(…, activityCancellationMsgActivityNotStarted, …)` +
  `activityNotStartedCancelled`.

### Refinements for implementation (Claude)
1. **K4 is an error-model change, not just a re-key.** Today tokeira hard-rejects the transition
   (`kernel rejected command: unknown activity: 5`); v1.31.0 **fails the WFT with a cause** and continues.
   So K4 needs a kernel "fail-the-WFT-with-cause `BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES`" path from
   inside the command loop, distinct from a command reject. Confirm the force-close/WFT-failure primitive
   from the event-buffering work covers this, or design it — this is the one non-mechanical part of K4.
2. **These four are independent — land them incrementally.** Unlike transient-wft (all-or-nothing hot
   path), each item flips its own leaves (K1→AttemptsExceeded + ScheduleToClose_FiredDuringBackoff;
   K2→Test_ActivityTimeouts; K3→ActivityRetry + HeartBeat_RecordIdentity; K4→TryActivityCancellation +
   ActivityCancellationNotStarted). Implement/verify/flip one at a time against green Tiers 1.1/1.2 —
   lower regression risk than a single cohesive change.
3. **One coupling:** K1's `CANCEL_REQUESTED` reads K4's new durable `cancel_requested`, so that single
   retry_state value lands with K4. K1's other states (`MAX_ATTEMPTS`, `NON_RETRYABLE`, `TIMEOUT`,
   `RETRY_POLICY_NOT_SET`) don't depend on K4 and can land first.
4. **K2 needs no new state.** `last_heartbeat_details` is already durable on `ActivityState` (§3's
   Describe work depends on it), so K2 only folds the existing field into the runtime-built timeout
   failure carried on the event — don't scope it as a state addition alongside K3.

### Recommendation
Accept Req 0 for K1–K4. If a spec is wanted, structure it as **four separable sub-features** (each with
its flip-leaves + its own harness check), mirroring workflow-retry-chain, not one cohesive unit. §3's
already-landed runtime/edge work was taken as-authored (review focus was K1–K4).

### Ownership / coordination
Claude is mid-run on `TestActivityClientTestSuite` in this same tree. To avoid the two-agents-one-file
situation hit on transient-wft (`kernel.rs` had interleaved uncommitted edits), **a single owner should
hold the kernel for this work.** Recommend Claude carries K1–K4 (it owns the conformance loop and is
already in Tier 1.3). Kiro stands down on the code unless the owner routes it here. Do not begin kernel
edits until the current suite run is committed/parked so the starting tree state is unambiguous.
