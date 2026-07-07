# Tasks: Speculative Workflow-Task Model (Kernel + Runtime + Edge)

Requirements: [requirements.md](./requirements.md). Design: [design.md](./design.md).

> **Requirement 0 is PROPOSED — blocking.** The kernel is frozen; Phases K-F may not start until
> the owner explicitly accepts Requirement 0 (existence bit, drop/materialize completion, appended
> `WorkflowTaskFailedCause` + event variants, follow-up WFT). Phase S (fork-side skip hygiene) is
> independent and should land first — the unregistered nil-panic leaves abort the whole parallel
> suite and mask every other result.

All kernel edits stay pure (AGENTS §2). Ground-truth every behaviour to v1.31.0 and cite in code
comments (AGENTS §8, §9). Verify with `cargo clippy -p tokeira-kernel -p tokeira-runtime -p
tokeira-edge --all-targets -- -D warnings`, `cargo +nightly fmt`, and — because this spec appends
postcard-persisted enum/event variants — `cargo test --workspace` in every bar (postcard lesson).

## Phase 0 — Decision (blocking)

- [x] 0.1 Owner accepts Requirement 0 **(DONE 2026-07-06 — accepted via owner-review.md; amendments F1-F5 recorded in requirements.md)** (speculative model + rider kernel changes). Record the
  acceptance date and any scope amendments in requirements.md before starting Phase K.

## Phase S — Harness skip hygiene (fork; independent, land before first full run)

- [x] S.1 Register the missing harness-class skips **(DONE — fork commit 9a9bd7bcb, all five entries plus the AdminService pair)** in
  `../temporal/tests/testcore/tokeira_conformance_skip.go`: CloseShard-class
  `TestScheduledSpeculativeWorkflowTask_LostUpdate` + `TestStartedSpeculativeWorkflowTask_LostUpdate`
  (nil-panic today); AdminClient-class `TestStartedSpeculativeWorkflowTask_TerminateWorkflow` +
  `TestScheduledSpeculativeWorkflowTask_TerminateWorkflow` (DescribeMutableState nil-panic);
  OverrideDynamicConfig-class `TestContinueAsNew_Suggestion` (non-default values, inert over the
  wire). Cited reasons per entry. (Req 0.3)

## Phase K — Kernel (raised; requires 0.1)

- [x] K.1 `PendingWorkflowTask.task_type **(DONE — state.rs WorkflowTaskType + threaded)**: WorkflowTaskType { Normal, Speculative }` with
  `#[serde(default)]` (`state.rs:411-433`); thread through schedule/start/complete and the
  three-mode predicate (normal / transient / speculative, Invariant I.2). (Req 1.1)
- [x] K.2 `apply_update` speculative arm **(DONE — schedule_speculative_workflow_task + F3 WorkflowTaskStateInconsistent guard)** (`kernel.rs:727-770`): fires on no pending WFT AND no
  buffered events → speculative task, no `WorkflowTaskScheduled`, virtual ids via the transient
  arm of `schedule_workflow_task` (`kernel.rs:4375-4446`); dispatch op marked speculative.
  **Owner amendment (F3): buffered events with NO pending WFT at the update call site is an
  INCONSISTENCY ERROR, not a graceful normal-WFT fallback — v1.31.0 treats a non-speculative
  result here as `ErrWorkflowTaskStateInconsistent` (`updateworkflow/api.go:180-186`); the
  general buffered→normal downgrade rule applies only at the other schedule sites.** Cites `updateworkflow/api.go:171-186` +
  `workflow_task_state_machine.go:309-410 @ v1.31.0`. (Req 1.1-1.3)
- [x] K.3 Completion drop-vs-materialize **(DONE — drop branch + events-window + capability threaded via WorkflowTaskCompletedRequest; buffered-events also block the drop)** in `apply_workflow_task_completed`: rejection-only +
  events-window (Req 3.3, capability `client_discards_speculative_with_events`, pinned max 10) →
  drop (zero events, pending task cleared, waiter outcomes still resolved); otherwise materialize
  Scheduled+Started late (reuse transient materialization) then Completed. Wire-message model:
  the completion command carries the message taxonomy so the kernel can see "only rejections".
  Cites `workflow_task_state_machine.go:676-748, 750-819 @ v1.31.0`. (Req 3.1, 3.3, 3.4)
- [x] K.4 Conversions: signal-while-scheduled **(DONE — materialize_scheduled_speculative helper on signal/cancel/timer paths + both timeout shapes; started-task fail/force-close also late-materialize)** → materialize Scheduled before the signal (Scheduled,
  Signaled, Started); signal-buffered-while-started → persist + convert at completion; timeout
  shapes per Req 5 (start-to-close: persist Scheduled/Started/TimedOut + attempt-2 transient;
  schedule-to-start: persist Scheduled/TimedOut + attempt reset 1 + real reschedule). Cites
  `workflow_task_state_machine.go:270-306, 934-990, 1466-1530 @ v1.31.0`. (Req 4.1, 4.2, 5)
- [x] K.5 `WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage` **(DONE — appended last; Reject::BadUpdateMessage{not_found} + runtime seam + NotFound edge mapping; resurrect via TryResurrect parity)** appended LAST
  (`command.rs:225-228` discipline); ProtocolMessage rejects (`UnknownUpdate`,
  `DuplicateUpdateId`, wrong-state) convert to K4-seam WFT failures instead of transition aborts.
  (Req 6.1, 6.2)
- [x] K.6 Event fidelity **(DONE — sequencing id validated (zero + F5 bounds) and recorded; WorkflowExecutionUpdateCompletedV2 appended, old variant decode-only; serializer emits {update_id}/request + Meta.UpdateId)** **(+ owner amendment F5: bounds-check the worker-provided sequencing
  id; out-of-range → bad-update-message failure via K.5)**: `UpdateProtocolBody::Accepted`
  carries the worker sequencing id (drop
  the hardcoded 0 at `kernel.rs:4129`); append the failure-capable
  `WorkflowExecutionUpdateCompleted` outcome variant (old variant decode-only). (Req 7.1, 7.2)
- [x] K.7 Follow-up WFT **(DONE — speculative successor on the normal completion tail and inside the drop branch)**: `apply_workflow_task_completed` schedules a speculative successor when
  `admitted_updates` is non-empty after a non-failing, non-close completion (normal if buffered
  events/heartbeat). Cites `respondworkflowtaskcompleted/api.go:512-541 @ v1.31.0`. (Req 8)

## Phase R — Runtime

- [x] R.1 Direct dispatch **(DONE d1f49d7f — sticky-first with immediate normal-queue fallback when no sticky poller waits (publisher probes waiter counts; normal queue carried on the op); empty sticky worker_task_queue tolerated as clear-stickiness; broker grace-demotion exemption deferred — the precise in-memory timer reclaims a lost task before the 5s grace window)**: speculative dispatch op bypasses durable backlog, publishes straight to
  the broker — sticky first, normal-queue fallback on sticky-unavailable, other errors logged
  only; zero-message dispatch suppressed. Cites `updateworkflow/api.go:218-252 @ v1.31.0`.
  (Req 1.4, 1.5)
- [x] R.2 In-memory timers **(DONE 87d2ce05 — precise per-run tokio timers (SpeculativeTimerSet): 5s pinned normal-queue STS stamped by the kernel, sticky STS, start-to-close; armed/disarmed by the lane post-commit hook (absolute deadlines, idempotent re-arm = stale guard); poll discards superseded broker entries and re-polls (I.1); stale completions surface "Workflow task not found."; F2 re-arm-on-load DEFERRED — needs sweep queries to carry task_type/deadlines, not exercised by the single-node corpus, tracked for T.2)**: 5s pinned normal-queue schedule-to-start, sticky schedule-to-start,
  **plus re-arm-on-load (owner amendment F2): shard sweep/load re-derives the timer from a
  persisted pending speculative task (STS if scheduled, start-to-close if started),**
  start-to-close for the started speculative task; stale-guard invalidation when the tracked task
  changes; **rollback bookkeeping** — on kernel drop, disarm timers, clear broker in-flight,
  keep sticky consistent (Invariant I.1). (Req 3.5, 5.3)
- [x] R.3 Waiter semantics **(DONE 87d2ce05 — abort_sent_for_wft_failure on both server-decided failure arms (bad-message + invalid-command), entry retained for redelivery (durable admitted set); absent-message-id routed through the kernel BadUpdateMessage seam via UpdateProtocolBody::UnresolvedMessage so the abort is uniform; TestValidateWorkerMessages 10/0/0)**: bad-message WFT failure aborts in-flight waiters with WorkflowNotReady
  (exact string); explicit RespondWorkflowTaskFailed keeps the update admitted + redelivers;
  resurrect-from-AcceptedRequest. (Req 6.3, 6.4)
- [x] R.4 RejectUnprocessed **(DONE 87d2ce05 — runtime stamps delivered_update_ids pre-commit; kernel prunes them from admitted_updates before the K7 follow-up decision; runtime resolves waiters with RejectedUnprocessed (unprocessedUpdateFailure authored at the edge); admission-order drain via admitted_seq)**: after a successful non-heartbeat completion, auto-reject Sent-state
  updates with the exact `unprocessedUpdateFailure`; no redelivery; a second update admitted
  mid-WFT gets its own fresh speculative WFT. (Req 9)

## Phase E — Edge

- [x] E.1 Poll synthesis **(DONE — suffix predicate widened to any virtual started id (transient OR speculative); GetHistory append_transient_suffix widened)**: widen the transient-suffix predicate to speculative
  (`from_internal.rs:47-76`; `append_transient_suffix`, `workflow_service.rs:5414`); anchor
  `Messages[].SequencingId.EventId` at the virtual scheduled id; `NextEventId` = virtual scheduled
  id; `PreviousStartedEventId` = last persisted WFT-started. (Req 2)
- [x] E.2 Completion wire **(DONE — reset_history_event_id on the completion DTO/proto, drop detected via token-vs-last_event_id; capability consumed into the Req 3.3 window)**: surface `ResetHistoryEventId` on the RespondWorkflowTaskCompleted
  response (drop → `LastCompletedWorkflowTaskStartedEventId`, else 0); consume
  `client_discards_speculative_with_events` (`grpc/translate.rs:2312-2314`) into the Req 3.3
  window. (Req 3.2, 3.3)
- [x] E.3 Message plumbing **(DONE — unreferenced-message splice landed in Tier 2.12 wave A; Response-with-Failure now decodes Completed-with-failure; absent message id -> exact InvalidArgument; serializer fixes with K.6)**: thread command-unreferenced messages through
  `to_internal::workflow_task_completed_request` (rejections + no-commands success shape); decode
  update `Response`-with-Failure as Completed-with-failure (fix `grpc/translate.rs:2342-2367`);
  serialize `accepted_request_message_id = "{update_id}/request"` and `Meta.UpdateId`
  (`history_serializer.rs:1400-1459`). (Req 6.5, 7.1, 7.2)

## Phase M — Metrics

- [x] M.1 **(DONE 3d9109a9 — kernel emits RecordSpeculativeOutcome (commit on materialize / rollback on drop) and RecordSpeculativeTimeout dispatch ops; publisher resolves the namespace NAME and counts namespace-labelled commits/rollbacks + timer task_requests/start_to_close_timeout tagged TimerActiveTaskSpeculativeWorkflowTaskTimeout.)** **(F4 resolved 2026-07-06: the four metric-asserting leaves count samples only —
  `speculativeWorkflowTaskOutcomes` iterates `capture.Metric(name)` with no tag inspection
  (update_workflow_test.go:35-45); reason tags NOT required. The start-to-close leaf's
  `operation == TimerActiveTaskSpeculativeWorkflowTaskTimeout` tag assertion
  (update_workflow_test.go:2647-2661) stays in M.2 scope.)** Emit namespace-labelled
  `speculative_workflow_task_commits` / `_rollbacks` at the
  completion seam; timer-task metrics tagged `TimerActiveTaskSpeculativeWorkflowTaskTimeout` on
  the start-to-close firing. (Req 10.1, 10.2)
- [x] M.2 Fork bridge **(DONE fork b4befe147 — four rename entries)**: add the `tokeiraMetricRename` entries
  (`../temporal/tests/testcore/tokeira_metrics_bridge.go:41`). (Req 10.3)

## Phase T — Required tests

> Progress note (2026-07-06): Phase-K landing included goldens (a)-(e) plus buffered-signal,
> both timeout shapes, V2-failure-outcome, and explicit-fail-materializes in
> `tests/golden_tests.rs`; T.1's P1-P6/G1/G2 coverage audit and T.2 remain open.

- [ ] T.1 Kernel properties/goldens P1-P6, G1, G2 (`// Feature: speculative-wft, Property N`):
  no-persist schedule/start; drop-without-trace + waiter outcomes; late materialization ids;
  conversion orderings; timeout shapes; follow-up + RejectUnprocessed. (Req 1-5, 8, 9)
- [ ] T.2 Runtime/edge tests: direct dispatch (no backlog row) + sticky fallback; **a
  simulated reload with a pending speculative task re-arms the timer and still
  dispatches/times-out correctly (owner amendment F2);** rollback
  bookkeeping (re-dispatch works after drop); poll suffix + anchoring; `ResetHistoryEventId`;
  validation-taxonomy wire errors (exact strings); metrics emission. (Req 2, 3, 6, 10, I.1)

## Phase C — Conformance checkpoint (operator-invoked)

- [x] C.1 **(DONE 2026-07-07 — all gated leaves flipped: TestWorkflowUpdateSuite 69/0/11 CLEAN, TestUpdateWorkflowSdkSuite 6/0/0 CLEAN, TestUpdateWithStartSuite 37/0/5 CLEAN; QueryFailureClearsWFContext closed via the consistent-query buffer capacity 1 + ErrConsistentQueryBufferExceeded (10710ec1), AbortUpdates via rejection-wins-over-close-abort pre-claim; workspace tests + 3x stress + all-tier regression in the Tier 2.12 land bar)** Run `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`,
  `cargo test --workspace`. Then drive `TestWorkflowUpdateSuite` against a running `tokeirad`
  (skips from S.1 in place) and flip the ~36 gated leaves:
  - **Cluster 3 (accept/complete + anchoring, 13):** TestEmptySpeculativeWorkflowTask_AcceptComplete
    (×2), TestNotEmptySpeculativeWorkflowTask_AcceptComplete (×2),
    TestFirstNormalScheduledWorkflowTask_AcceptComplete (×2),
    TestNormalScheduledWorkflowTask_AcceptComplete (×2),
    TestStickySpeculativeWorkflowTask_AcceptComplete (×2),
    TestStickySpeculativeWorkflowTask_AcceptComplete_StickyWorkerUnavailable,
    TestWaitAccepted_GotCompleted, TestUpdatesAreSentToWorkerInOrderOfAdmission.
  - **Cluster 4 (rollback / convert / ResetHistoryEventId, 8):**
    TestFirstNormalScheduledWorkflowTask_Reject, TestEmptySpeculativeWorkflowTask_Reject,
    TestNotEmptySpeculativeWorkflowTask_Reject,
    TestRunningWorkflowTask_NewEmptySpeculativeWorkflowTask_Rejected,
    TestRunningWorkflowTask_NewNotEmptySpeculativeWorkflowTask_Rejected,
    TestStartedSpeculativeWorkflowTask_ConvertToNormalBecauseOfBufferedSignal,
    TestScheduledSpeculativeWorkflowTask_ConvertToNormalBecauseOfSignal,
    TestSpeculativeWorkflowTask_Heartbeat.
  - **Cluster 5 (validation taxonomy, 10):** TestValidateWorkerMessages (8 table leaves),
    TestSpeculativeWorkflowTask_Fail,
    TestSpeculativeWorkflowTask_WorkerSkippedProcessing_RejectByServer.
  - **Cluster 7 (multi-update ordering, +2):** Test1stAccept_2ndAccept_2ndComplete_1stComplete,
    Test1stAccept_2ndReject_1stComplete (ordering leaf shared with cluster 3).
  - **Cluster 8 (timeouts, 3):** TestSpeculativeWorkflowTask_StartToCloseTimeout,
    TestSpeculativeWorkflowTask_ScheduleToStartTimeout,
    TestSpeculativeWorkflowTask_ScheduleToStartTimeoutOnNormalTaskQueue.

  Metric-asserting members (TestEmptySpeculativeWorkflowTask_AcceptComplete ×2,
  TestRunningWorkflowTask_NewEmptySpeculativeWorkflowTask_Rejected,
  TestSpeculativeWorkflowTask_StartToCloseTimeout) require M.1+M.2 to pass. Registered skips
  (S.1 + pre-existing) stay skipped; TestSpeculativeWorkflowTask_QueryFailureClearsWFContext is
  tracked under the Tier 2.10 query-buffer work, not gated here.

## Phase D — Docs

- [ ] D.1 Amend `docs/architecture/020-kernel.md` (three WFT modes, drop/materialize, conversion
  table) and `docs/readiness/command-surface.md` (appended cause + event variant, wire-message
  model). (AGENTS §9)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["0.1", "S.1"] },
    { "id": 1, "tasks": ["K.1"] },
    { "id": 2, "tasks": ["K.2", "K.5", "K.6"] },
    { "id": 3, "tasks": ["K.3", "K.4", "K.7"] },
    { "id": 4, "tasks": ["R.1", "R.2", "R.3", "R.4"] },
    { "id": 5, "tasks": ["E.1", "E.2", "E.3"] },
    { "id": 6, "tasks": ["M.1", "M.2", "T.1", "T.2"] },
    { "id": 7, "tasks": ["C.1"] },
    { "id": 8, "tasks": ["D.1"] }
  ]
}
```

> Wave ordering: 0.1 gates everything kernel-ward; S.1 is independent but wave-0 so the first full
> suite run cannot be masked by harness panics. K.1 (the existence bit) precedes every other kernel
> arm; K.3/K.4/K.7 build on K.2's scheduling arm. Runtime dispatch/timers (wave 4) need the kernel
> shapes; edge wire (wave 5) needs both. C.1 is operator-invoked and flips the gated leaves; D.1
> records the landed model.
