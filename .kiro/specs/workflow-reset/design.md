# Design: Workflow Reset

This design realizes the reset model of `requirements.md` across edge → kernel → runtime → storage.
It reworks tokeira's current **structural fork-and-replay** (which terminates the reset run and
copies only the prefix) into v1.31.0's **base/current fork + terminal-WFT + reapply + relink** model.

The work is organized as **11 core model changes (CM1–CM11)** grouped into **5 slices**. Slice 1
(closed-run + base/current + successor WFTFailed, no reapply) is the high-leverage first landing.

---

## Model overview

```
ResetWorkflowExecution(base_run_id?, workflow_task_finish_event_id, reason, request_id,
                       reset_reapply_type?, reset_reapply_exclude_types?)
  │
  ├─ edge: validate finish ∈ [2, NextEventID-1]; resolve enclosing pending WFT (CM2)
  │        compute exclude_set from type+excludes (CM1); dedup on request_id (CM9)
  │        resolve base (by run_id) + current (chain tip) (CM5)
  │
  ├─ kernel/runtime reset engine:
  │     1. load base (OPEN OR CLOSED) full history          (CM4)
  │     2. copy prefix [1 .. finish-1]; replay into new run (existing, kept)
  │     3. author WorkflowTaskFailed(RESET) on the NEW run  (CM3)
  │        (+ synthesized WFTStarted; attempt→1; fail in-flight activities)
  │     4. link base→new (reset_run_id), new→base (base_execution) (CM5)
  │     5. reapply [finish .. end] across the CaN chain, filtered by exclude_set (CM6, CM7)
  │            SIGNALED / UPDATE→UpdateAdmitted / OPTIONS_UPDATED(always) / child-resolutions
  │            skip CANCEL_REQUESTED, TERMINATED
  │     6. terminate current run iff running (CM5)
  │     7. schedule fresh NORMAL WFT on the new run
  │
  └─ storage: 2-run atomic (base==current) or 3-run atomic (base≠current) (CM5)
             new run = chain current
```

The **new run's history** after the shared prefix is:
`WorkflowTaskFailed(RESET)` [+ synthesized WFTStarted] → activity-failed events → reapplied tail
(Signaled / UpdateAdmitted / OptionsUpdated / child resolutions) → fresh `WorkflowTaskScheduled`.

---

## CM1 — Edge `ResetRequest` fields + exclude-set (S) — Slice 0

- Add to Edge `ResetWorkflowExecutionRequest` (`translate/mod.rs:891`): `reset_reapply_type:
  ResetReapplyType`, `reset_reapply_exclude_types: Vec<ResetReapplyExcludeType>`, and a plumbed-inert
  `post_reset_operations` (F14, unused until Slice 5).
- Read them in `reset_request_to_edge` (`grpc/translate.rs:4011`).
- Carry them through `to_internal::reset_request` (`to_internal.rs:543`) into the kernel `ResetRequest`.
- **Compute the exclude set at the edge** per api.go:199-219: `ALL_ELIGIBLE`/`UNSPECIFIED`→`{}`,
  `SIGNAL`→`{UPDATE}`, `NONE`→`{SIGNAL,UPDATE}`; union explicit excludes. Enum: SIGNAL=1, UPDATE=2,
  NEXUS=3. Store a `Vec<ResetReapplyExcludeType>` (or a small bitset) on the kernel `ResetRequest`.
- No behaviour yet — pure plumbing (the kernel ignores the set until CM6).

## CM2 — Reset-point resolution + validator broadening (M) — Slice 0/1

- Replace the edge type-whitelist (`validate_reset_target`, `workflow_service.rs:5723-5745`) with
  the range check `finish ∈ [2, NextEventID-1]` and resolution of the **enclosing pending WFT**: a
  finish id that lands on a non-WFT event (e.g. `Signaled`) resolves to the WFT whose
  `[Scheduled+1, Started+1]` range contains it (workflow_resetter.go:526).
- Keep the prefix boundary `[1 .. finish-1]` (equivalent to today).
- Populate `fork_event_version` from the base version history (currently hard-None at kernel.rs:1342)
  for `BaseExecutionInfo`/event fidelity. (tokeira's version histories are trivial single-branch;
  the version is the run's failover version — plumb it rather than invent it.)

## CM3 — Kernel reset applier redesign (L) — Slice 1

The current applier is inverted: it appends `WFTFailed{RESET}` to the **base** and TERMINATES the
base (kernel.rs:1333-1345), then re-emits the successor WFTFailed off-lane via `tokio::spawn`
(lane.rs:694-722). Rework to v1.31.0's shape:

- **Author `WorkflowTaskFailed{cause=ResetWorkflow, BaseRunId, NewRunId, ForkEventVersion}` on the
  SUCCESSOR** at the boundary, inline in materialization (removes the lane.rs:694-722 spawn race).
  Synthesize a `WorkflowTaskStarted` first when the pending WFT is scheduled-not-started
  (workflow_resetter.go:532-548). **Reset WFT attempt→1** (workflow_task_state_machine.go:918-922).
- **Do NOT append WFTFailed to the base**, and **do NOT run `apply_parent_close_policy()`** as part
  of the fork (kernel.rs:1359 removed from the reset path).
- **Force-fail in-flight started activities** `RETRY_STATE_NON_RETRYABLE_FAILURE` with
  `NewResetWorkflowFailure(reason, lastHeartbeatDetails)`; not-started activities reset their
  `ScheduledTime` to now (F11, workflow_resetter.go:564-598).
- Reject when the fork point has no pending WFT (workflow_resetter.go:520-529).

The `WorkflowTaskFailed` event variant already carries `base_run_id/new_run_id/fork_event_id/
fork_event_version` (kernel.rs:1340-1343) — no new event fields, but `fork_event_version` must be
populated (CM2) and the event must now be authored on the successor, not the base.

## CM4 — Closed-run reset (M) — Slice 1

- Remove the `expect_open` gate from the reset path (kernel.rs:1302). Reset must load a **closed**
  base's full history and fork it (F1). The base's status is irrelevant to the fork; only the
  current run's running-ness matters (CM5).
- `materialize_reset_successor` (memory.rs:706-808, DSQL mirror at dsql/run_repository/mod.rs:483,
  trait at api.rs:537/:1262) must accept a closed base — it already loads base state + history, so
  the change is dropping the open-assertion and ensuring closed-run history is fully readable.
- The two baseline errors ("already completed"/"not found") disappear here.

## CM5 — Base/current-run model + linking + persistence (L) — Slice 1

- **Resolve base and current independently** (api.go:73-105): base = requested run_id (default
  current); current = chain tip (`GetCurrentWorkflowRunID`). Replace the single
  `resolve_execution_run_key` (workflow_service.rs:3833) with a base+current pair.
- **Terminate current iff running** (decoupled from the fork): author
  `WorkflowExecutionTerminated` (identity=resetter), force-failing any started WFT first
  (workflow_resetter.go:140-156). If current already closed, skip.
- **New run = chain current** after reset.
- **Linking:** add a `reset_run_id: Option<RunId>` field to the base run's execution info
  (base→new) and a `base_execution: Option<BaseExecutionInfo>` to the new run (new→base). Surface
  `ResetRunId`/`OriginalExecutionRunId` in Describe; **fix `first_run_id`** to trace the true first
  run instead of self (translate.rs:7696). Reuse the existing `original_execution_run_id`
  propagation (kernel.rs:447).
- **Persistence atomicity (new for tokeira storage):**
  - `base == current` → **2-run atomic** update (base/current mutation + new snapshot).
  - `base != current` → **3-run atomic** conflict-resolve (base `reset_run_id` link + new snapshot +
    current termination) (workflow_resetter.go:358-430). tokeira has no existing 3-run atomic
    primitive — this is the highest-risk storage work. In-memory is a single-lock write; DSQL needs
    a multi-row transaction.
- Handles `RepeatedResets` (pointer overwrite + prior-new-run termination) and `WithMoreClosedRuns`
  (middle runs untouched).

## CM6 — Event-reapply engine (L) — Slice 2

- Add a forward reapply walk over `[baseRebuildLastEventID+1 .. baseNextEventID]` (memory + DSQL),
  gated by the exclude set from CM1. Per-type rules (F6a–d):
  - `WorkflowExecutionSignaled` → re-emit (name/input/identity/header/links) unless excl SIGNAL.
  - `UpdateAccepted`/`UpdateAdmitted` → re-emit as **`WorkflowExecutionUpdateAdmitted`** unless excl
    UPDATE; accepted-with-no-embedded-request → skip.
  - `WorkflowExecutionOptionsUpdated` → always reapply (F6c).
  - `WorkflowExecutionCancelRequested`, `WorkflowExecutionTerminated` → skip (F6d).
  - child non-init resolutions → reconnect if child still live (CM8).
- **New persisted event kind (sub-dependency):** add `WorkflowExecutionUpdateAdmitted` to
  `HistoryEventKind` (event.rs — currently only Accepted/Completed/Rejected/CompletedV2). Additive,
  serde-default, mirrored in `history_serializer.rs` + DSQL. Plus the machinery to schedule a WFT
  that re-delivers the admitted update on the new run (touches the Tier 2.12 update state machine —
  an admitted update leaves an open update to be re-driven).
- After reapply, `ScheduleWorkflowTask` on the new run.

## CM7 — CaN-chain reapply walk + reset-after-CaN acceptance (M, folds into CM6) — Slice 1(accept)/2(walk)

- Accept a `CONTINUED_AS_NEW` base (F10) — Slice 1 needs only acceptance (no post-fork events in the
  `ResetAfterContinueAsNew` leaf beyond the CaN close).
- The reapply walk follows `WORKFLOW_EXECUTION_CONTINUED_AS_NEW.NewExecutionRunId` across runs
  (workflow_resetter.go:822-823); `close_continues_into_successor` (lane.rs:1607) already detects the
  CaN close and can seed the walk.

## CM8 — Reset-with-child reconnection (M) — Slice 4

- Parent-close-policy suppression is already delivered by CM3 (removing `apply_parent_close_policy`
  from the reset path).
- Reapply child non-init resolution events only when `GetChildExecutionInfo(InitiatedEventId)` still
  resolves (workflow_resetter.go:1039); the pending-children gate stays off (phase-2 population is
  intentionally absent upstream, so the 6 phase-2 leaves remain skipped).

## CM9 — Reset dedup on RequestId (S) — Slice 0

- Return `{RunId: currentRunId}` no-op when `current.create_request_id == request.request_id`
  (api.go:108-116). tokeira pushes a `RequestDedupeOp` today but has no idempotent-return path for
  reset — add it at the edge/runtime boundary before the fork.

## CM10 — External-payload recompute (S) — Slice 1

- Recompute `ExternalPayloadCount/SizeBytes` on the successor from the copied prefix events only
  (F12) — the serde-tree fold already used by Describe (Tier 1.8), applied to the new run's history
  rather than copying the base stats. `ResetWorkflowWithExternalPayloads` asserts 2/3072 → 1/1024.

## CM11 — PostResetOperations + batch reset (M) — DEFERRED (Slice 5)

- F14/F15; both consumers are the 2 versioning skips. Plumb the `post_reset_operations` field (CM1)
  but leave it inert. Batch reset target resolution is already partially wired (batch_engine.rs);
  its `BuildId` gap and reapply/current_run_only support are deferred with the versioning leaves.

---

## Cross-cutting risks (from the gap analysis)

1. **Successor materialization shares the lane commit path with CaN/cron/retry.** `lane.rs:598-603`
   selects the successor branch by a metadata marker; CM3 moves WFTFailed authoring inline, changing
   that selection. Regress-test Tiers 3.14/3.15/3.16 after every reset slice.
2. **Postcard event-enum evolution.** `WorkflowExecutionUpdateAdmitted` is a new persisted
   `HistoryEventKind` — must be additive/serde-default, mirrored in DSQL + `history_serializer.rs`.
   `reset_run_id`/`base_execution` are new persisted execution-info fields (DSQL schema; in-memory
   free). The `WorkflowTaskFailed` variant needs no new fields.
3. **Two-store fidelity + 3-run atomicity.** Closed-run load + prefix copy + reapply + relink must
   work in memory AND DSQL, including the 3-run atomic conflict-resolve (CM5) — tokeira's
   highest-risk new storage primitive.
4. **K4 seam / WFT accounting.** The successor's `WorkflowTaskFailed(ResetWorkflow)` must NOT feed
   WFT-failure backoff or the K4 invalid-command error model; the following fresh NORMAL WFT must
   dispatch cleanly; attempt→1 interacts with the retry/attempt bookkeeping (Tiers 1.2/1.7).
5. **Pending-activity re-dispatch (F11).** Re-seeding timeout tracking (lane.rs:654-667) ≠
   re-enqueuing the activity for polling. `TestResetWorkflow` requires the 2 replayed-pending
   activities to be pollable on the successor — verify the rebuild re-advertises them.
6. **Reapply-type NONE ≠ full skip.** OptionsUpdated + child reconnection + Nexus still reapply under
   NONE; only {SIGNAL,UPDATE,NEXUS}-all-excluded short-circuits. `ExcludeNoneReapplyNone` asserts ev5
   OptionsUpdated survives.
7. **Update reapply is UpdateAdmitted, not Accepted** — replays as *pending admitted* (an open
   update to be re-driven), touching the Tier 2.12 update state machine, not just an event append.
8. **CANCEL_REQUESTED/TERMINATED skip is unconditional on reset** (not exclude-gated); ensure the
   reset's own current-run terminate (identity=resetter) is never reapplied onto the new run
   (workflow_resetter.go:981) — else a base≠current-running reset self-terminates the new run.

## Verification strategy

- Per-slice: rebuild `tokeirad`, run the affected reset leaves, then `cargo test --workspace` +
  regression on Tiers 3.14/3.15/3.16 (shared successor path) before committing.
- Golden/property tests: add kernel goldens for the reset spine (prefix + successor WFTFailed +
  reapply tail) and a serde round-trip for the new `WorkflowExecutionUpdateAdmitted` kind.
- Final: 3× stress the three reset suites; confirm **28 pass / 0 fail / 8 skip**.
