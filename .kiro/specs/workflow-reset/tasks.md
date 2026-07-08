# Tasks: Workflow Reset

Sliced per `design.md`. Each slice ends with: rebuild `tokeirad`, run the slice's reset leaves,
`cargo test --workspace`, regress Tiers 3.14/3.15/3.16 (shared successor path), commit.
Target after all in-scope slices: **28 pass / 0 fail / 8 skip**.

Leaf shorthand: `RW/*` = `TestResetWorkflowTestSuite`, `WR/*` = `TestWorkflowResetTestSuite`,
`WRC/*` = `TestWorkflowResetWithChildTestSuite`.

## Slice 0 — Plumbing & resolution (prereq)

- [ ] 0.1 (CM1) Add `reset_reapply_type`, `reset_reapply_exclude_types`, inert `post_reset_operations`
      to Edge `ResetWorkflowExecutionRequest` (`translate/mod.rs`); read them in `reset_request_to_edge`
      (`grpc/translate.rs:4011`); carry through `to_internal::reset_request` (`to_internal.rs:543`)
      onto the kernel `ResetRequest`. Compute the exclude set (ALL/UNSPEC→{}, SIGNAL→{UPDATE},
      NONE→{SIGNAL,UPDATE}; union explicit) at the edge. No behaviour.
- [ ] 0.2 (CM2) Broaden `validate_reset_target` (`workflow_service.rs:5723`) to `finish ∈ [2,
      NextEventID-1]` + enclosing-pending-WFT resolution; keep prefix boundary `[1..finish-1]`.
      Populate `fork_event_version` from the base version history.
- [ ] 0.3 (CM9) Reset dedup: `current.create_request_id == request.request_id` → return
      `{RunId: currentRunId}` no-op.

## Slice 1 — Closed-run + base/current + successor WFTFailed (NO reapply) → ~16/28

- [ ] 1.1 (CM4) Drop `expect_open` from the reset path (`kernel.rs:1302`); ensure
      `materialize_reset_successor` (memory + DSQL + trait) loads a CLOSED base's full history.
- [ ] 1.2 (CM5) Resolve base (by run_id, default current) + current (chain tip) independently at the
      edge (replace `resolve_execution_run_key` for reset); new run becomes chain current.
- [ ] 1.3 (CM3) Rework the kernel/lane reset applier: author `WorkflowTaskFailed{RESET, BaseRunId,
      NewRunId, ForkEventVersion}` on the SUCCESSOR inline (remove the lane.rs:694-722 spawn),
      synthesize WFTStarted if scheduled-not-started, attempt→1; do NOT terminate the base or run
      parent-close-policy on the fork; force-fail in-flight activities non-retryable, reset
      not-started activity ScheduledTime. Reject if no pending WFT at fork.
- [ ] 1.4 (CM5) Terminate the CURRENT run iff running (identity=resetter, force-fail started WFT,
      mark WorkflowWasReset); skip if already closed.
- [ ] 1.5 (CM5) Linking: add `reset_run_id` (base→new) + `base_execution` (new→base) to execution
      info; surface `ResetRunId`/`OriginalExecutionRunId` in Describe; fix `first_run_id` to the true
      first run (translate.rs:7696). Serde-default + DSQL mirror.
- [ ] 1.6 (CM5) Storage atomicity: 2-run atomic when base==current; 3-run atomic conflict-resolve
      when base≠current (memory single-lock; DSQL multi-row txn).
- [ ] 1.7 (CM7-accept) Accept a CONTINUED_AS_NEW base (no walk yet).
- [ ] 1.8 (CM10) Recompute external-payload stats on the successor from the copied prefix only.
- [ ] 1.9 (F11 verify) Confirm the successor re-advertises replayed-pending activities for polling
      (`RW/TestResetWorkflow` drives 2 remaining activities to completion).
- [ ] 1.10 Verify Slice-1 leaves green: `WR/{NoBaseCurrentClosed,NoBaseCurrentRunning,
      SameBaseCurrentClosed,SameBaseCurrentRunning,DifferentBaseCurrentClosed,
      DifferentBaseCurrentRunning,RepeatedResets,WithMoreClosedRuns,OriginalExecutionRunId}`,
      `RW/{TestResetWorkflow,TestResetWorkflowAfterTimeout,WorkflowTask_Schedule,
      WorkflowTask_ScheduleToStart,WorkflowTask_Start,ResetAfterContinueAsNew,
      ResetWorkflowWithExternalPayloads}`. `cargo test --workspace` + regress 3.14/3.15/3.16. Commit.

## Slice 2 — Reapply engine + type/exclude mapping → 25/28

- [ ] 2.1 (CM6) Add `WorkflowExecutionUpdateAdmitted` to `HistoryEventKind` (event.rs), serde-default;
      mirror in `history_serializer.rs` + DSQL codec; serde round-trip test.
- [ ] 2.2 (CM6) Reapply walk `[fork+1..end]` (memory + DSQL) gated by the exclude set: SIGNALED
      (unless excl SIGNAL); UPDATE_ACCEPTED/ADMITTED → UpdateAdmitted (unless excl UPDATE;
      accepted-with-no-request skipped); OPTIONS_UPDATED always; skip CANCEL_REQUESTED + TERMINATED.
- [ ] 2.3 (CM6) Schedule a WFT re-delivering each reapplied admitted update on the new run (Tier 2.12
      update state machine: admitted → open update re-driven).
- [ ] 2.4 (CM7) CaN-chain walk: follow `WORKFLOW_EXECUTION_CONTINUED_AS_NEW.NewExecutionRunId` across
      runs, reapplying post-fork events from later generations.
- [ ] 2.5 Verify: `RW/{ExcludeNoneReapplyDefault,ExcludeNoneReapplyAll,ExcludeNoneReapplySignal,
      ExcludeNoneReapplyNone,ExcludeSignalReapplyAll,ExcludeSignalReapplySignal,
      ExcludeSignalReapplyNone}` (+ repeated-reset/dedup branches in Default/All). Kernel golden for
      the reapply spine. `cargo test --workspace` + regress. Commit.

## Slice 3 — Buffered-signal reapply → 27/28

- [ ] 3.1 (F8) Treat a signal buffered during the in-flight fork-point WFT as a post-fork
      reapply-eligible event; reapply under SIGNAL-eligible, drop under reapply-NONE.
- [ ] 3.2 Verify: `RW/{TestBufferedSignalIsReappliedOnReset,TestBufferedSignalIsDroppedOnReset}`.
      `cargo test --workspace` + regress. Commit.

## Slice 4 — Reset-with-child reconnection → 28/28

- [ ] 4.1 (CM8) Reapply child non-init resolution events onto the new run only when the child's
      `InitiatedEventId` still resolves to a live child (initiated before the fork); parent-close-policy
      suppression already delivered by CM3.
- [ ] 4.2 Verify: `WRC/{TestResetWithChild_AfterStartingChild,TestResetWithChild_AfterChildCompletes,
      TestResetWithChild_AfterChildTerminated}` (the 3 non-phase-2 leaves). 3× stress all three reset
      suites → **28/0/8**. `cargo test --workspace` + regress 3.14/3.15/3.16. Ledger row + memory +
      commit + push.

## Deferred (stay skipped)

- [ ] 5.1 (CM11, F14) PostResetOperations (UpdateWorkflowOptions / versioning override) on the new run.
- [ ] 5.2 (CM11, F15) Batch reset: BuildId target, reapply/current_run_only.
      (Both consumers are the 2 worker-deployment-versioning skips; the 6 phase-2 child leaves remain
      upstream `t.Skip`.)
