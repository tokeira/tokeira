# Implementation Plan

Phases A–D are Codex slices on `compat/temporal-1.32`, dispatched in order after this
spec is approved. Every task cites the requirement it implements; every design
property has a required property-based test task. Corpus checkpoints run the named
suites on `tokeira/conformance-v1.32.0` three times against `tokeirad` built with
`--features conformance`.

- [x] 1. Phase A — execution lineage and response fidelity
  - [x] 1.1 Runtime start results carry the chain head
    - `StartWorkflowResult` and `SignalWithStartResult` gain `first_execution_run_id`
      (and `start_request_id` on the rejected variant), read from the resolved run's
      state; edge DTOs and `EdgeError::WorkflowStartRejected` carry them.
    - _Requirements: 1.1, 1.2, 1.3, 1.6, 1.7_
  - [x] 1.2 Emit the chain head on the three proto paths
    - Start response, signal-with-start response, and `workflow_already_started_status`
      (also `start_request_id`); multi-operation start leg reuses the DTO.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_
  - [x] 1.3 Property test: Property 1 — chain head propagates to every start-path outcome
    - Generated lineages (retry, cron, continue-as-new, reset) and start requests;
      reference model = event 1 of the resolved run.
    - Tag: `// Feature: v132-lifecycle-fidelity, Property 1: chain head propagates to every start-path outcome`
    - _Requirements: 1.1–1.6_
  - [x] 1.4 Continue-as-new backoff field
    - `WorkflowCommand::ContinueAsNew.backoff_start_interval` (serde default, last);
      edge fills it; kernel preserves it in the close event; runtime calls the pure
      `continue_as_new_min_backoff` helper when constructing the successor delay.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_
  - [x] 1.5 Property test: Property 2 — continue-as-new backoff arithmetic
    - Tag: `// Feature: v132-lifecycle-fidelity, Property 2: continue-as-new backoff arithmetic`
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 1.6 Total-updates limit at admission
    - Distinct-update counter per run against the `history.maxTotalUpdates` consult
      site; exact message; retried requests do not count; multi-operation error shape.
    - _Requirements: 3.1, 3.2, 3.6_
  - [x] 1.7 Update-with-start retry-once and running-workflow leg
    - Re-execute once on a closing abort with no started run; `ABORTED` on the second;
      running leg `started = false`, `status = RUNNING`, Started_Event_Ref_Link.
    - _Requirements: 3.3, 3.4, 3.5_
  - [x] 1.8 Property tests: Property 3, Property 4, Property 5
    - Tags: `// Feature: v132-lifecycle-fidelity, Property 3: total-updates limit at admission`,
      `… Property 4: update-with-start re-executes once on a closing abort`,
      `… Property 5: running-workflow start leg`
    - _Requirements: 3.1–3.6_
  - [x] 1.9 Link constructors and response links
    - `started_event_ref_link`, `request_id_ref_link`; update response link by
      outcome; signal and signal-with-start links; query link stays unset.
    - _Requirements: 4.1, 4.2, 4.3, 5.1, 5.2, 5.3_
  - [x] 1.10 Request links onto produced events
    - Start, signal, cancel-requested, terminated events carry the request's `links`.
    - _Requirements: 5.4_
  - [x] 1.11 Signal request-id infos behind the backlinks override
    - Runtime records signal request ids (buffered → event id) when
      `history.enableCHASMSignalBacklinks` is on; reset rebuilds them; the bridge
      wires the key; ledger amended (task 5.2).
    - _Requirements: 5.5, 5.6, 5.7_
  - [x] 1.12 Property tests: Property 6, Property 7, Property 8, Property 9
    - Tags: `… Property 6: update response link by outcome`, `… Property 7: signal links are unconditional and idempotent`,
      `… Property 8: request links land on the produced event`, `… Property 9: signal request-id infos follow the override`
    - _Requirements: 4.1, 4.2, 5.1–5.7_
  - [x] 1.13 Nil-map omission and predicate alignment
    - `is_temporal_nil_payload` per `isNilPayload`; `None` for empty filtered maps on
      start, continue-as-new, child-start events and describe.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_
  - [x] 1.14 Property test: Property 10 — nil-map omission
    - Tag: `… Property 10: nil-map omission`
    - _Requirements: 6.1–6.5_
  - [x] 1.15 Update callbacks precondition
    - Exact `INVALID_ARGUMENT` message; callbacks otherwise ignored; reassign the SDK
      dedup leaf to D5 in the findings owner table.
    - _Requirements: 7.1, 7.2, 7.3_
  - [x] 1.16 Eager dispatch guards
    - Remove the per-response cap; paused-workflow and versioned-routing guards;
      retry policy on the inline task.
    - _Requirements: 8.1, 8.2, 8.3_
  - [x] 1.17 Status truncation
    - UTF-8-safe truncation at 4000 bytes with the `"... <truncated>"` suffix.
    - _Requirements: 9.1, 9.2_
  - [x] 1.18 Property tests: Property 11, Property 12, Property 20
    - Tags: `… Property 11: callback precondition`, `… Property 12: status message truncation`,
      `… Property 20: eager dispatch guards`
    - _Requirements: 7.1, 7.2, 9.1, 9.2, 8.1–8.3_
  - [x] 1.19 Checkpoint: bar green; corpus `TestWorkflowTestSuite`,
    `TestContinueAsNewTestSuite`, `TestUpdateWithStartSuite`,
    `TestUpdateWorkflowSdkSuite` (minus the D5 leaf), `TestNilSearchAttributeSuite`,
    and the D6 leaves of `TestLinksTestSuite` clean three times; Ledger rows updated.
    - _Requirements: 18.4_

- [ ] 2. Phase B — workflow pause
  - [ ] 2.1 Gate and validation order
    - `WorkflowPausePolicy` (default `false`) consulted first by pause; bridge wires
      `frontend.WorkflowPauseEnabled`; length checks in the documented order for pause
      and unpause before resolution.
    - _Requirements: 10.1, 10.2, 10.3, 10.4_
  - [ ] 2.2 Property test: Property 13 — pause gate and validation order
    - Tag: `… Property 13: pause gate and validation order`
    - _Requirements: 10.1–10.4_
  - [ ] 2.3 `TemporalPauseInfo` on describe
    - Shared `pause_info_entries`; describe merges system attributes over user
      attributes; removal on unpause.
    - _Requirements: 11.1, 11.2_
  - [ ] 2.4 Property test: Property 14 — pause search-attribute lifecycle
    - Tag: `… Property 14: pause search-attribute lifecycle`
    - _Requirements: 11.1, 11.2_
  - [ ] 2.5 Drive `TestPauseWorkflowExecutionSuite` to green
    - Diagnose the describe timeouts and the no-scheduled-task-between-events rule
      against `1.32.0` before each fix; record every finding in this file's Notes.
    - _Requirements: 11.3, 11.4, 11.5_
  - [ ] 2.6 Checkpoint: bar green; the pause suite clean three times with the override
    on and the stock rejection verified with it off; Ledger row added.
    - _Requirements: 18.4_

- [ ] 3. Phase C — schedules
  - [ ] 3.1 Manual actions and paused reconciliation
    - `manual` flag through due and buffered actions; gates and decrement bypassed;
      reconcile every schedule; inclusive backfill start.
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_
  - [ ] 3.2 Property test: Property 15 — manual schedule actions bypass gates
    - Tag: `… Property 15: manual schedule actions bypass gates`
    - _Requirements: 12.1–12.5_
  - [ ] 3.3 Validation order and list caps
    - Request-id length first; create requires a request id; payload size before the
      memo rejection using the `limit.blobSize.*` consult sites (verified `Wired`,
      task 5.3); duration validation with exact messages; list caps of 5.
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_
  - [ ] 3.4 Property test: Property 16 — schedule list caps and validation order
    - Tag: `… Property 16: schedule list caps and validation order`
    - _Requirements: 13.1–13.5_
  - [ ] 3.5 Schedule-id operators and schedule search attributes in the filter
    - _Requirements: 14.1, 14.2, 14.3_
  - [ ] 3.6 Property test: Property 17 — schedule-id filter algebra
    - Tag: `… Property 17: schedule-id filter algebra`
    - _Requirements: 14.1–14.3_
  - [ ] 3.7 Describe fidelity
    - Pause-on-failure on `FAILED`/`TIMED_OUT` with the V1 note (verify the CHASM
      note against the CHASM leaves); resolved catchup window; `state_size_bytes`;
      successor tracking for continue-as-new and reset runs.
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5_
  - [ ] 3.8 Property test: Property 18 — schedule describe resolution
    - Tag: `… Property 18: schedule describe resolution`
    - _Requirements: 15.1, 15.3, 15.4_
  - [ ] 3.9 Checkpoint: bar green; `TestScheduleV1`, `TestScheduleCHASM`, the two
    pause-interaction suites (with the pause override on), `TestScheduleCountsVisibility`,
    `TestScheduleNextActionTimeVisibility`, `TestScheduleManyCalendars`,
    `TestScheduleFarFutureActionTimes` clean or classified three times; Ledger rows.
    - _Requirements: 18.4_

- [ ] 4. Phase D — reset
  - [ ] 4.1 `reset_run_id` on extended info; deprecated override fields on describe
    - _Requirements: 16.1, 16.5_
  - [ ] 4.2 Chain reapply and missing-current tolerance
    - Walk the continue-as-new chain from the base successor; stop at a deleted run;
      new run becomes current.
    - _Requirements: 16.2, 16.3_
  - [ ] 4.3 Batch reset carries post-reset operations
    - _Requirements: 16.4_
  - [ ] 4.4 Property test: Property 19 — reset linkage and chain reapply
    - Tag: `… Property 19: reset linkage and chain reapply`
    - _Requirements: 16.1–16.3_
  - [ ] 4.5 Checkpoint: bar green; `TestResetWorkflowTestSuite`,
    `TestWorkflowResetTestSuite` (minus the metric leaf) clean three times; Ledger rows.
    - _Requirements: 18.4_

- [ ] 5. Phase E — registry, ledger, kept-clean suites
  - [ ] 5.1 Registry skip for the in-process metric leaf, with the cited reason
    - _Requirements: 18.1_
  - [ ] 5.2 Ledger amendments: `frontend.WorkflowPauseEnabled` and
    `history.enableCHASMSignalBacklinks` to conformance-only overrides, `Wired`,
    D6 owner, `v1.32.0` evidence; `check-temporal` clean.
    - _Requirements: 18.2_
  - [ ] 5.3 Verify `limit.blobSize.error` / `limit.blobSize.warn` are `Wired`; wire them
    if not, with a ledger amendment.
    - _Requirements: 18.3_
  - [ ] 5.4 Re-run `TestWorkflowTypeEncodingSuite`, `TestPrematureEosTestSuite`,
    `TestRelayTaskTestSuite`, and `TestHttpApiTestSuite` (with the fork's synchronized
    read); confirm the HTTP metric bridge emits a namespace-tagged sample; record the
    header leaf as `expected-until-flip`.
    - _Requirements: 17.1, 17.2, 17.3_
  - [ ] 5.5 Checkpoint: bar green; all D6 Ledger rows clean or classified.
    - _Requirements: 18.4_

## Task Dependency Graph

```json
{
  "1.1": [], "1.2": ["1.1"], "1.3": ["1.2"], "1.4": [], "1.5": ["1.4"],
  "1.6": [], "1.7": ["1.6"], "1.8": ["1.7"], "1.9": [], "1.10": ["1.9"],
  "1.11": ["1.9"], "1.12": ["1.10", "1.11"], "1.13": [], "1.14": ["1.13"],
  "1.15": [], "1.16": [], "1.17": [], "1.18": ["1.15", "1.16", "1.17"],
  "1.19": ["1.3", "1.5", "1.8", "1.12", "1.14", "1.18"],
  "2.1": ["1.19"], "2.2": ["2.1"], "2.3": ["2.1"], "2.4": ["2.3"], "2.5": ["2.3"], "2.6": ["2.2", "2.4", "2.5"],
  "3.1": ["1.19"], "3.2": ["3.1"], "3.3": ["5.3"], "3.4": ["3.3"], "3.5": [], "3.6": ["3.5"],
  "3.7": ["2.6"], "3.8": ["3.7"], "3.9": ["3.2", "3.4", "3.6", "3.8"],
  "4.1": ["1.19"], "4.2": ["4.1"], "4.3": ["4.1"], "4.4": ["4.2"], "4.5": ["4.3", "4.4"],
  "5.1": [], "5.2": ["1.11", "2.1"], "5.3": [], "5.4": ["1.19"], "5.5": ["2.6", "3.9", "4.5", "5.1", "5.2", "5.4"]
}
```

## Notes

- Slices: Phase A = tasks 1.x; Phase B = 2.x; Phase C = 3.x plus 5.3; Phase D+E =
  4.x plus 5.1, 5.2, 5.4, 5.5. Phase A carries the only kernel change (1.4).
- Task 3.7's successor tracking depends on the pause override (Phase B) only for the
  pause-interaction suites; the describe fidelity itself does not.
- The `TestHTTPAPIHeaders` leaf is not a D6 defect; it is re-verified at the claim
  flip by the umbrella spec.
- Diagnoses recorded during task 2.5 belong here, each with a `path:line @ v1.32.0`
  citation, before the corresponding fix lands.

### Phase A validation and review checkpoint — 2026-09-15

- Requirements 1–9 are implemented, with Properties 1–12 and 20 exercised at
  100 or more cases per property. The approved amendments preserve zero as an
  unlimited update budget, require the complete interpolated limit message, and
  bound truncated status text to at most 4000 bytes without splitting a code point.
- Task 1.4 is complete. The wording correction was approved on 2026-09-29 and
  applied to A2 and the task description: Requirement 2.1 and
  `service/history/historybuilder/event_factory.go:476-499 @ v1.32.0` preserve
  the requested backoff in the close event; the successor's minimum delay belongs
  at runtime construction (`service/history/workflow/mutable_state_impl.go:2778-2794,
  2868-2894 @ v1.32.0`). This aligns the design with the existing implementation.
- Task 1.19 completed on 2026-09-29. The target suites' measured outcomes and build
  validation
  are recorded in the [readiness ledger](../../../docs/readiness/conformance.md#temporal-v1320-successor-verification).
  Update-with-start had deadlocked in the fork's metrics capture shim after
  the correct total-update-limit rejection: the corpus opens a second capture
  while retaining the failed request's first capture until cleanup
  (`tests/update_workflow_test.go:4982, 5805-5807 @ v1.32.0`). The separately approved
  shim correction retains isolation between clusters while supporting overlapping
  windows within one cluster, with once-only freezing and lock release. Four
  regression tests each passed 75 times under the race detector at 1, 4, and 8
  logical CPUs; full shim race tests, focused Go vet, and the fork's
  `make lint-code` also passed. The shim change is fork commit
  `5d6ceeed1e560a4e2c6f39a7d0377ea3a95fcc58`. Three fresh
  update-with-start runs each produced 26 pass / 0 fail / 1 existing skip, with
  identical leaf outcomes. No corpus body or skip registry was changed.
- Devbox validation is green: nightly formatting verification, `cargo lint --locked`,
  `cargo check --workspace --locked`, `cargo nextest run --workspace --locked --no-fail-fast`
  (3,392 passed, 2 skipped), `cargo test --workspace --doc --locked`, and
  `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.
  The full gRPC property module additionally passes 35 properties with
  `PROPTEST_CASES=1024`; focused conformance-feature properties also pass.
- `cargo run -p compatibility-docs --locked -- check-temporal` confirms the
  configuration ledger and generated documentation agree. The offline link check
  passes for tracked Markdown. The broad local glob also scans ignored build and
  old evidence files and reports three pre-existing missing local links there.
- Regression fixtures now reflect nil-map omission, nil memo upserts as deletion,
  and the already-started failure's start request id and chain head. The wire
  golden changes only those two fields on its three transport surfaces; status
  codes, text, and other metadata are unchanged.
- Admission counts distinct completed ids from durable history under the loaded
  snapshot boundary, alongside admitted/pending ids. This preserves correctness
  after legacy completions and cold replay without a second kernel state change;
  the tradeoff is a history scan for each new update id.
