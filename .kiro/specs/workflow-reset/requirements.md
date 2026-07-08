# Requirements Document: Workflow Reset (Kernel + Runtime + Edge + Storage)

## Introduction

This spec adopts Temporal's **workflow reset** model (`ResetWorkflowExecution`) into
`tokeira-kernel` and the derived runtime/edge/storage paths. It was raised by the Tier 3.17
reset-suite map (`docs/readiness/functional-test-order.md` item 17), which covers three suites —
`TestResetWorkflowTestSuite` (16 leaves), `TestWorkflowResetTestSuite` (11 leaves),
`TestWorkflowResetWithChildTestSuite` (9 leaves) — **36 leaves, 28 in scope for green**
(8 out-of-scope skips: 6 upstream `t.Skip("reset phase 2")` child leaves + 2 tokeira
worker-deployment-versioning leaves already classified in the conformance skip registry).

Reset is the operation that **forks a run's history at an earlier point into a brand-new run**,
re-drives it from that boundary, and re-applies the eligible events (signals, updates, options,
child resolutions) that happened after the fork so no external input is lost. It is one of the
most cross-cutting engine features: it reads a (possibly **closed**) run's full history, rebuilds
a successor from a prefix, authors a terminal `WorkflowTaskFailed(RESET_WORKFLOW)` at the boundary,
re-applies a filtered event tail, re-links two-or-three runs atomically, and re-points the run
chain's current execution.

tokeira today has a **structural fork-and-replay** of the prefix only (landed incidentally by Tier
1.8's `TestGetHistoryReverse_MultipleBranches`): it TERMINATES the run being reset, copies
`history[..fork_event]`, replays it, and fails the dangling fork-point WFT. It works **only on open
runs**, performs **no event reapply**, drops **reapply type / exclude types** at the proto boundary,
has **no base-vs-current-run model or continue-as-new walk**, and **mis-handles children** by
running parent-close-policy on the terminated run. The baseline is therefore **0 pass / 14 fail /
8 skip** with two gating errors: `expect_open` (kernel.rs:1302) rejects a closed base as
"workflow execution already completed", and an absent base surfaces "execution not found".

### Ground truth (v1.31.0, agent-verified)

Two files carry the algorithm in the pinned fork (`v1.31.0-26-g86a27eb76`):

- **RPC entry / point resolution:** `service/history/api/resetworkflow/api.go`
- **Reset engine:** `service/history/ndc/workflow_resetter.go`
  (NOT `service/history/workflow/workflow_resetter.go`, which does not exist; and NOT
  `ndc/resetter.go`, which is the replication/conflict-resolution resetter, not the RPC path.)

Supporting: `ndc/state_rebuilder.go` (replay), `workflow/workflow_task_state_machine.go` +
`historybuilder/event_factory.go` (terminal WFT event), `workflow/util.go` (terminate),
`workflow/mutable_state_impl.go` (link bookkeeping).

**Entry, lease, dedup (api.go:25-116).**
- Fork-point validation: `WorkflowTaskFinishEventId ∈ [2, base.NextEventID-1]`; else
  `InvalidArgument("Workflow task finish ID must be > 1 && <= workflow last event ID.")`
  (api.go:61-64).
- **Base vs current:** the base run is the request's RunId (defaults to current if omitted);
  `currentRunID = GetCurrentWorkflowRunID(workflowID)` is resolved independently (api.go:73-105).
  Both leases held for the whole operation.
- **Reset dedup:** if `current.CreateRequestId == request.RequestId`, return `{RunId: currentRunID}`
  as a no-op (api.go:108-116).

**Fork point (api.go:118-129).** `resetRunID = uuid.New()`;
`baseRebuildLastEventID = WorkflowTaskFinishEventId - 1` (last event replayed onto the new run);
`baseRebuildLastEventVersion` from the base version history; the new run retains base events
`[1 .. baseRebuildLastEventID]` inclusive, diverging at `baseRebuildLastEventID+1`.

**Rebuild + terminal WFT (workflow_resetter.go:107-560).**
- Link base→new: `UpdateResetRunID(resetRunID)` sets base `ExecutionInfo.ResetRunId`
  (mutable_state_impl.go:1010).
- Fork branch at node `baseRebuildLastEventID+1`; replay `[1..baseRebuildLastEventID]` into a fresh
  MutableState keyed by `resetRunID`, **reusing the original start-request-id** (state_rebuilder.go:404).
- Link new→base: `SetBaseWorkflow(baseRunID, baseRebuildLastEventID, baseRebuildLastEventVersion)`
  → `BaseExecutionInfo` (mutable_state_impl.go:998).
- **Author `WorkflowTaskFailed(cause=RESET_WORKFLOW, failure=NewResetWorkflowFailure(reason),
  identity=IdentityHistoryService, BaseRunId, NewRunId=resetRunID, ForkEventVersion)` on the NEW
  run** (workflow_resetter.go:550-560); if the pending WFT was scheduled-not-started, synthesize a
  `WorkflowTaskStarted` first (workflow_resetter.go:532-548). **WFT attempt resets to 1** for the
  RESET cause (workflow_task_state_machine.go:918-922).
- Reject if there is no pending WFT at the fork point (workflow_resetter.go:520-529), or pending
  children with the namespace gate off (workflow_resetter.go:314-316).
- Force-fail in-flight started activities `RETRY_STATE_NON_RETRYABLE_FAILURE`; not-started
  activities just reset their `ScheduledTime` to now (workflow_resetter.go:564-598).

**Reapply loop (workflow_resetter.go:644-1026).** Walk forward from `baseRebuildLastEventID+1`
across the base branch AND the whole continue-as-new chain (following
`WORKFLOW_EXECUTION_CONTINUED_AS_NEW.NewExecutionRunId`, workflow_resetter.go:732-759,822-823):
- `SIGNALED` → re-emit `WorkflowExecutionSignaled` unless `excludeSignal`.
- `UPDATE_ADMITTED` → re-emit unless `excludeUpdate`.
- `UPDATE_ACCEPTED` → unless `excludeUpdate`: if it embeds no `AcceptedRequest` it is **skipped**;
  else re-emitted as **`WorkflowExecutionUpdateAdmitted`** (an in-flight accepted update replays as
  *pending admitted*, not accepted) (workflow_resetter.go:898-923).
- `OPTIONS_UPDATED` → always reapplied (not gated by SIGNAL/UPDATE), survives even under type=NONE
  (workflow_resetter.go:948-975).
- `CANCEL_REQUESTED` and `TERMINATED` → **skipped on reset** (`isReset` continue,
  workflow_resetter.go:924-925,976-981); the reset's own terminate (identity Resetter/HistoryService)
  is never reapplied onto the new run.
- Child non-init resolution events (`CHILD_*_STARTED/COMPLETED/FAILED/…`) → reconnected only if the
  child's `InitiatedEventId` still resolves to a live child (workflow_resetter.go:1039).
- default → HSM `CherryPick` (this is where **NEXUS** exclusion is enforced).

**Reapply-type → exclude-set mapping (api.go:199-219).** The engine consumes only the exclude set;
the deprecated include-type maps to it: `ALL_ELIGIBLE`/`UNSPECIFIED` → `{}`; `SIGNAL` → `{UPDATE}`;
`NONE` → `{SIGNAL, UPDATE}` (crucially **not** NEXUS — child/nexus/options still reapply under NONE).
Explicit `reset_reapply_exclude_types` are unioned on top. Full short-circuit
(`shouldExcludeAllReapplyEvents`, workflow_resetter.go:1167) requires SIGNAL, UPDATE, AND NEXUS all
excluded. Enum values: `UNSPECIFIED=0, SIGNAL=1, UPDATE=2, NEXUS=3`.

**Closed runs & current-run termination (workflow_resetter.go:140-156).** Reset works on **closed
base runs** — nothing requires the base to be running. Only the **current** run's running-ness
matters: if running, set `WorkflowWasReset=true`, `TerminateWorkflow(identity=IdentityResetter)`
(force-failing any started WFT with FORCE_CLOSE_COMMAND first), pin `resetWorkflowVersion` to the
current run's version, close it as a mutation. If already closed, the terminate step is skipped.

**Persistence (workflow_resetter.go:340-430).** `base == current` → 2-run atomic
`UpdateWorkflowExecution(UpdateCurrent)`; `base != current` → **3-run atomic**
`ConflictResolveWorkflowExecution(UpdateCurrent)` (base link + reset snapshot + current
termination). The new run becomes the chain's current.

### tokeira seams (agent-verified)

- `crates/tokeira-kernel/src/kernel.rs:1301` (`apply_reset`), `:1302` (`expect_open` — the
  closed-run gate), `:1333-1345` (WFTFailed on the predecessor + `close(Terminated)`), `:1359`
  (`apply_parent_close_policy`), `:266` (`replay_history_prefix`), `:447`
  (`original_execution_run_id` propagation).
- `crates/tokeira-kernel/src/event.rs:540-641` (update event kinds — **no `UpdateAdmitted`**),
  `:561` (`WorkflowExecutionOptionsUpdated`), `:84` (`original_execution_run_id`).
- `crates/tokeira-edge/src/translate/mod.rs:891-898` (Edge `ResetWorkflowExecutionRequest` — no
  reapply field); `translate/to_internal.rs:543-559` (drops reapply, verbatim fork id);
  `grpc/translate.rs:4011` (`reset_request_to_edge`), `:5006`/`:7696` (`first_run_id` surfaced/set
  to self).
- `crates/tokeira-edge/src/grpc/workflow_service.rs:3805` (inner reset handler), `:3833`
  (`resolve_execution_run_key` — no base/current split), `:5723-5745` (`validate_reset_target` —
  type whitelist), `:1809-1844` (batch resolve).
- `crates/tokeira-runtime/src/lane.rs:601-724` (successor materialization + off-lane `tokio::spawn`
  WFTFailed), `:1607` (`close_continues_into_successor`), `:1654` (`extract_reset_metadata`).
- `crates/tokeira-storage/src/memory.rs:706-808` (`materialize_reset_successor` prefix copy);
  `dsql/run_repository/mod.rs:483` (DSQL mirror); `api.rs:537,:1262` (trait).

---

## Requirement 0 — Adjudication: classify vs implement

**Every non-skip Tier 3.17 leaf is a real reset behaviour gap, not a harness artifact.** The 8
out-of-scope leaves are classified skips; the remaining 28 require the reset model below.

### 0.1 Out-of-scope skips (already classified)

- **6 upstream child leaves** carry `s.T().Skip("Skipping until reset phase 2 is enabled")` in the
  corpus itself (`tests/workflow_reset_with_child_test.go`): `TestResetWithChild`, `_WithChildID`,
  `_WithChildID_WithRejectDuplicate`, `_RunningChild_RandomWID`, `_RunningChild_SetWID`,
  `_RunningChild_SetWID_WithRejectDuplicate`. These exercise the **phase-2** post-reset
  child-restart tracking that v1.31.0 itself leaves commented out (workflow_resetter.go:800-817).
  No tokeira action; they skip on any Temporal build.
- **2 worker-deployment-versioning leaves** — `TestResetWorkflowWithOptionsUpdate`,
  `TestBatchResetWithOptionsUpdate` — drive a VERSIONED poller and assert task-queue version
  membership via the matching-service RPC `CheckTaskQueueVersionMembership`. tokeira's conformance
  cluster exposes no standalone `MatchingClient` (single edge process), so the versioned-poller
  goroutine SIGSEGVs and aborts the whole binary. Worker deployment versioning is Tier-4+ scope; the
  reset behaviour itself is covered by the non-versioned leaves. Classified as a
  matching-service/versioning-class skip (`tokeira_conformance_skip.go`).

### 0.2 Feature inventory (28 active leaves)

| # | Capability | v1.31.0 anchor | tokeira today | Leaves |
|---|---|---|---|---|
| F1 | Reset a CLOSED / non-open base run | api.go (no open-check); resetter forks regardless | **Missing** (`expect_open`, kernel.rs:1302) | AfterTimeout, NoBaseCurrentClosed, SameBaseCurrentClosed, DifferentBaseCurrent{Running,Closed}, RepeatedResets, WithMoreClosedRuns, ResetAfterContinueAsNew |
| F2 | Base-vs-current run model (resolve both; terminate current iff running; new run = chain current) | api.go:73-105; workflow_resetter.go:140-156,340 | **Missing** (`resolve_execution_run_key` forks the one run and terminates *it*) | all 9 non-versioning `workflow_reset_test.go` leaves |
| F3 | Link + lineage (base→new `ResetRunId`; new→base `BaseExecutionInfo`; `OriginalExecutionRunId`; Describe `FirstRunId` → true first run) | mutable_state_impl.go:1010,998; workflow_resetter.go:132,494 | **Partial** (`original_execution_run_id` exists; `reset_run_id`/`base_execution` do not; `first_run_id` set to self at translate.rs:7696) | NoBase*, SameBase*, DifferentBase*, RepeatedResets, WithMoreClosedRuns, OriginalExecutionRunId, TestResetWorkflow |
| F4 | Reset dedup on RequestId (retried reset → current run id, no-op) | api.go:108-116 | **Missing** | RepeatedResets, ExcludeNoneReapply{Default,All} (dedup branch) |
| F5 | Terminal `WorkflowTaskFailed{RESET}` on the SUCCESSOR at the boundary; synthesize `WorkflowTaskStarted` if scheduled-not-started; reset WFT attempt→1 | workflow_resetter.go:510-560; wf_task_state_machine.go:918-922 | **Partial & inverted** (WFTFailed on predecessor + Terminate; successor WFTFailed re-emitted off-lane only when started id present) | AfterTimeout, all 7 reapply/exclude leaves, both buffered-signal leaves |
| F6 | Event-reapply engine: walk `[fork+1..end]`, reapply per type | workflow_resetter.go:644,841 | **Missing** (copies only prefix) | all reapply/exclude/buffered-signal + child After* leaves |
| F6a | SIGNALED → re-emit unless excl SIGNAL | workflow_resetter.go:865-879 | Missing | ExcludeNoneReapply{Default,All,Signal}, ExcludeSignalReapply* (suppressed), BufferedSignalIsReapplied |
| F6b | UPDATE_ACCEPTED/ADMITTED → re-emit as `WorkflowExecutionUpdateAdmitted` unless excl UPDATE | workflow_resetter.go:880-923 | **Missing + no event kind** | ExcludeNoneReapply{Default,All}, ExcludeSignalReapplyAll |
| F6c | `WorkflowExecutionOptionsUpdated` ALWAYS reapplied (even under type=NONE) | workflow_resetter.go:948-975 | Missing | every reapply/exclude leaf (ev5 OptionsUpdated) |
| F6d | skip CANCEL_REQUESTED and TERMINATED on reset | workflow_resetter.go:924-925,976-981 | N/A (no loop) | negative assertion across reapply leaves |
| F7 | Reapply-type → exclude-set mapping + exclude union; short-circuit only if {SIGNAL,UPDATE,NEXUS} all excluded | api.go:199-219; workflow_resetter.go:1167 | **Missing** (field dropped at proto) | 9 reapply/exclude + 2 buffered-signal leaves |
| F8 | Buffered-signal-at-reset reapplied (SIGNAL) / dropped (NONE) | reapply over post-fork tail | Missing | BufferedSignalIsReapplied, BufferedSignalIsDropped |
| F9 | Reset-point resolution: validate finish ∈ `[2,NextEventID-1]`; accept a non-WFT id inside the pending-WFT range | api.go:61-64,118-129; workflow_resetter.go:520-529 | **Partial** (type whitelist rejects a `Signaled` finish id; fork taken verbatim) | WorkflowTask_{Schedule,ScheduleToStart,Start} |
| F10 | Reset-after-Continue-as-New: accept a CONTINUED_AS_NEW base; reapply follows the CaN chain | workflow_resetter.go:732-759,822-823 | **Missing** | ResetAfterContinueAsNew; DifferentBase*/WithMoreClosedRuns |
| F11 | Pending-activity handling: replayed prefix's pending activities re-dispatched (not-started → ScheduledTime=now); in-flight started → force-fail non-retryable | workflow_resetter.go:564-598 | **Partial/at-risk** (kernel deletes activities; re-dispatch unverified) | TestResetWorkflow |
| F12 | External-payload accounting recomputed from the copied prefix only | Describe stats recomputed post-rebuild | **Missing/unknown** | ResetWorkflowWithExternalPayloads |
| F13 | Reset-with-child reconnection: reconnect children initiated before fork via post-fork resolution reapply; suppress parent-close-policy on reset | workflow_resetter.go:1039,314-316 | **Wrong** (`apply_parent_close_policy` on the Terminated predecessor) | AfterStartingChild, AfterChildCompletes, AfterChildTerminated |
| F14 | PostResetOperations (UpdateWorkflowOptions / versioning override) applied to new run before ScheduleWFT | api.go:67,223; workflow_resetter.go:239,1181 | Missing — **deferred** (versioning skips) | (2 skipped leaves) |
| F15 | Batch reset with `ResetOptions.Target` + PostResetOperations across N workflows | — | **Partial** (BuildId unimpl; reapply/current_run_only unsupported) | (deferred, versioning skip) |

### 0.3 Slicing (ordered by dependency and corpus leverage)

- **Slice 0 — plumbing & resolution** (no leaves alone): edge reapply fields (inert), reset-point
  validator, exclude-set computation, reset dedup. Hard prerequisite for all below.
- **Slice 1 — closed-run + base/current + successor WFTFailed (NO reapply)** — highest leverage,
  greens **~16/28** (all 9 `workflow_reset_test.go` non-versioning leaves + `TestResetWorkflow`,
  `AfterTimeout`, the 3 `WorkflowTask_*` reset-point leaves, `ResetAfterContinueAsNew`,
  `WithExternalPayloads`) because their setups carry **no post-fork signals/updates** (reapply is a
  verified no-op).
- **Slice 2 — reapply engine + type/exclude mapping** → **25/28**: the 9 reapply/exclude leaves.
  Requires the new `WorkflowExecutionUpdateAdmitted` event kind (F6b) and the always-reapply
  OptionsUpdated rule (F6c).
- **Slice 3 — buffered-signal reapply** → **27/28**: the 2 buffered-signal leaves.
- **Slice 4 — reset-with-child reconnection** → **28/28**: the 3 active child leaves.
- **Deferred (stay skipped):** PostResetOperations + batch (F14/F15, the 2 versioning leaves) and
  the 6 upstream phase-2 child leaves.

### 0.4 Acceptance of Requirement 0

Requirement 0 is accepted when the owner confirms: (a) the 8 skips are correctly classified; (b) the
feature inventory F1–F15 is complete and correctly grounded; (c) the slice order is sound and Slice 1
is a legitimate ~16-leaf first landing with no event-reapply engine; (d) the new persisted event kind
`WorkflowExecutionUpdateAdmitted` and the base/current-run + 2-/3-run atomic persistence changes are
sanctioned kernel/storage evolution.

---

## Requirement 1 — Reset accepts a closed base and resolves the base/current run pair

**User story:** As a client resetting a completed workflow, I can reset a **closed** run to an
earlier point, and the engine forks that base while terminating the (possibly different) current run.

**Acceptance criteria:**
1. WHEN `ResetWorkflowExecution` targets a run whose status is closed (Completed/Failed/TimedOut/
   Terminated/ContinuedAsNew) THE kernel SHALL fork it — the `expect_open` gate SHALL NOT reject the
   reset path (F1).
2. WHEN the request omits a RunId THE base SHALL default to the chain's current run; otherwise the
   base is the requested RunId and the current run is resolved independently as the chain tip (F2).
3. WHEN the current run is running THE engine SHALL terminate it (`WorkflowExecutionTerminated`,
   identity = resetter, marking `WorkflowWasReset`), force-failing any started WFT first; WHEN the
   current run is already closed THE terminate step SHALL be skipped (F2, closed-run §).
4. WHEN the base run does not exist THE reset SHALL return a NotFound consistent with v1.31.0
   ("execution not found" is acceptable only for a genuinely absent run, not a closed one).
5. THE new run SHALL become the chain's current execution after reset.

## Requirement 2 — Successor authored with terminal RESET WFT and correct lineage

**Acceptance criteria:**
1. THE new run's history SHALL be the shared prefix `[1..baseRebuildLastEventID]`
   (`baseRebuildLastEventID = WorkflowTaskFinishEventId - 1`) followed by
   `WorkflowTaskFailed(cause=ResetWorkflow, BaseRunId, NewRunId, ForkEventVersion)` (F5).
2. WHEN the pending WFT at the fork point was scheduled-but-not-started THE engine SHALL synthesize a
   `WorkflowTaskStarted` before the failed event (F5).
3. THE reset WFT attempt SHALL be 1 on the successor (F5; `TestResetWorkflowAfterTimeout` asserts
   `WorkflowExecutionStarted{Attempt:1}`).
4. THE base run SHALL carry `ResetRunId → newRunId`; THE new run SHALL carry `BaseExecutionInfo →
   {baseRunId, baseRebuildLastEventID, forkEventVersion}` and preserve `OriginalExecutionRunId`;
   Describe on the new run SHALL report `FirstRunId` = the chain's true first run (F3).
5. WHEN there is no pending WFT at the fork point THE reset SHALL be rejected (F5,
   workflow_resetter.go:520-529).

## Requirement 3 — Reset-point resolution and validation

**Acceptance criteria:**
1. THE edge SHALL validate `workflow_task_finish_event_id ∈ [2, NextEventID-1]` (F9).
2. THE validator SHALL accept a finish id that is not itself a WorkflowTask* event (e.g. a
   `WorkflowExecutionSignaled` id) provided it resolves to an enclosing pending WFT range
   (`TestResetWorkflow_WorkflowTask_{Schedule,ScheduleToStart,Start}`, F9).
3. THE fork-point prefix boundary SHALL remain `[1 .. finish-1]` (arithmetically equivalent to
   today), and `ForkEventVersion` SHALL be populated from the base version history (not hard-None).

## Requirement 4 — Reset dedup and repeated resets

**Acceptance criteria:**
1. WHEN a reset's RequestId equals the current run's create-request-id THE engine SHALL return
   `{RunId: currentRunId}` as an idempotent no-op (F4).
2. A second reset of the same base to the same fork point SHALL produce an identical successor shape,
   overwriting the base→new pointer and superseding the prior new run (`TestRepeatedResets`, F4).
3. Base/current combinations SHALL behave per v1.31.0: `SameBaseCurrent{Running,Closed}`,
   `DifferentBaseCurrent{Running,Closed}`, `NoBaseCurrent{Running,Closed}`, `WithMoreClosedRuns`
   (middle runs untouched) — all assert `ResetRunId`/`OriginalExecutionRunId` linkage and terminal
   status only (F2, F3, F10).

## Requirement 5 — Event-reapply engine with type/exclude filtering

**Acceptance criteria:**
1. THE engine SHALL walk `[baseRebuildLastEventID+1 .. baseNextEventID]` on the base branch and
   continue across the continue-as-new chain (F6, F10).
2. `WorkflowExecutionSignaled` SHALL be re-emitted unless SIGNAL is excluded (F6a).
3. `WorkflowExecutionUpdateAccepted`/`UpdateAdmitted` SHALL be re-emitted as
   `WorkflowExecutionUpdateAdmitted` unless UPDATE is excluded; an accepted event with no embedded
   `AcceptedRequest` is skipped (F6b) — this requires a **new persisted `HistoryEventKind`
   variant `WorkflowExecutionUpdateAdmitted`** plus the machinery to schedule a WFT re-delivering
   the admitted update.
4. `WorkflowExecutionOptionsUpdated` SHALL always be reapplied (not gated by SIGNAL/UPDATE; present
   even under type=NONE) (F6c).
5. `WorkflowExecutionCancelRequested` and `WorkflowExecutionTerminated` SHALL be skipped on reset;
   the reset's own current-run terminate SHALL never be reapplied onto the successor (F6d).
6. THE deprecated `reset_reapply_type` SHALL map to the exclude set (ALL/UNSPEC→{}, SIGNAL→{UPDATE},
   NONE→{SIGNAL,UPDATE}); explicit `reset_reapply_exclude_types` SHALL be unioned; a full
   short-circuit SHALL occur only when SIGNAL, UPDATE, and NEXUS are all excluded (F7).
7. The 9 reapply/exclude leaves SHALL assert the exact post-reset history shapes documented in the
   corpus inventory (e.g. `ExcludeNoneReapplyNone` → `[..4 WFTFailed, 5 OptionsUpdated, 6
   WFTScheduled]` — OptionsUpdated survives NONE).

## Requirement 6 — Buffered-signal reapply

**Acceptance criteria:**
1. A signal buffered during the in-flight WFT at the fork point SHALL be reapplied when SIGNAL is
   eligible (`TestBufferedSignalIsReappliedOnReset`) and dropped under reapply-NONE
   (`TestBufferedSignalIsDroppedOnReset`) (F8) — treated as a post-fork reapply-eligible event.

## Requirement 7 — Reset-with-child reconnection

**Acceptance criteria:**
1. THE reset SHALL NOT run parent-close-policy on the reset (children of the base run are not
   terminated by the fork) (F13).
2. Children initiated **before** the fork point SHALL be reconnected onto the new run by reapplying
   their post-fork resolution events, only when the child's `InitiatedEventId` still resolves to a
   live child (F13): `TestResetWithChild_AfterStartingChild` (reconnect a running child),
   `_AfterChildCompletes` (replay the completion), `_AfterChildTerminated` (retain the terminated
   outcome).

## Requirement 8 — Two-store fidelity and non-regression

**Acceptance criteria:**
1. Closed-run load + prefix copy + reapply + relink SHALL work in **both** the in-memory store and
   the DSQL store, including the 2-run (base==current) and 3-run (base≠current) atomic commits.
2. Adding `WorkflowExecutionUpdateAdmitted` and the `reset_run_id`/`base_execution` execution-info
   fields SHALL be additive and back-compatible in the postcard codec (serde defaults) and mirrored
   in DSQL serialization + `history_serializer.rs`.
3. Reset SHALL NOT regress the continue-as-new / cron / retry successor-materialization path (Tiers
   3.14–3.16) which shares the lane commit branch; `cargo test --workspace` SHALL stay green.
4. The versioning skips (2) and upstream phase-2 skips (6) SHALL remain classified; net Tier 3.17
   result = **28 pass / 0 fail / 8 skip** after all four slices.
