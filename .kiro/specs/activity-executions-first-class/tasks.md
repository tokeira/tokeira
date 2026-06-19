# Implementation Plan: Activity Executions First-Class

## Overview

Closes the remaining `TestStandaloneActivityTestSuite` (C1) gaps on top of `chasm-foundation`
+ commit `5b5faddd` (edge admission validation + NotFound fidelity, already landed). Measured
baseline: **1/31** (`temporal-functional-conformance/reference/FINDINGS.md`). Every task is
ground-truthed to v1.31.0 (`chasm/lib/activity @ v1.31.0`, AGENTS §8) and verified by re-running
the named sub-tests against a statically-SA-enabled `tokeirad` (FINDINGS runbook). No kernel
additions.

Legend: `[ ]` open · `[ ]*` optional test sub-task.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.0"] },
    { "id": 1, "tasks": ["1.1", "1.2"] },
    { "id": 2, "tasks": ["1.3", "1.4", "1.5"] },
    { "id": 3, "tasks": ["1.6", "1.7"] },
    { "id": 4, "tasks": ["2.1", "2.2", "2.3"] },
    { "id": 5, "tasks": ["2.4"] },
    { "id": 6, "tasks": ["3.1", "4.1", "4.2", "4.3"] },
    { "id": 7, "tasks": ["3.2"] },
    { "id": 8, "tasks": ["5.1"] }
  ]
}
```

Stage 1 is foundational — it unblocks every bare-id path. Stage 2 is the highest-leverage follow-on
because it gates setup validation in most sub-tests. Stages 3 and 4 are independent of each other and
may proceed in any order once Stage 1 lands. Stage 5 depends on all.

## Tasks

## Stage 0 — Design gate (RESOLVED)

- [x] 0.1 **Item 1 (current-run index) decisions are resolved and recorded in `design.md`:**
  (a) **Migration shape** — a new additive migration `V056__chasm_current_run.sql` (next free after
  `V055`), modeled on the existing workflow `current_execution` table (`V003`); a distinct table, not
  a fold into `V049__chasm_node`. (b) **Pointer contents** — `CurrentRun { run_id, status, vt_epoch }`
  (carry all three: `status` for the reuse/conflict policy, `vt_epoch` for the fence). (c) **Delete of
  the current run** clears/supersedes the pointer (read-your-write `NotFound`). (d) **Mechanism** —
  authoritative pointer beside the node store, fenced at the Start commit; never a visibility lookup.
  The one **externally-ground-truthed** point — closed-run bare-id resolution and the exact
  pointer-advance fencing in the `chasm/` framework — is confirmed read-source-first in task 1.0
  below (not a blocker; the recorded default is "terminal run remains current until superseded").

## Stage 1 — Current-run index (Requirements 1, 2)

- [x] 1.0 **Ground-truth (read-only) — DONE.** Confirmed against v1.31.0 and recorded in `design.md`:
  `current_executions` persists at terminal, is deleted only on delete, and is fenced by a
  `last_write_version` conditional update. No contradiction with the recorded default.
- [x] 1.1 Current-run pointer on the node-store trait + in-memory store: `current_run(...) ->
  Option<CurrentRun>` + atomic `persist_new_execution` (set-on-start under the node-write lock).
- [x] 1.2 DSQL backing `V056__chasm_current_run.sql` (modeled on `current_execution`/`V003`, DSQL-safe
  spread-key PK); pointer written in the same transaction as the root node; `LifecycleState`↔SMALLINT.
- [x] 1.3 Enforce `IdReusePolicy`/`IdConflictPolicy` in `ChasmEngine::start_execution` — reject a
  conflict against a LIVE current run with `ActivityExecutionAlreadyStarted` (naming `RunId` +
  `StartRequestId`), `USE_EXISTING`/same-request-id return the existing run, and apply the reuse matrix
  (`REJECT_DUPLICATE`, `ALLOW_DUPLICATE_FAILED_ONLY`) against a terminal current run.
  **DONE** in three steps: (1) policy plumbed to the engine (`StartRequest.policy`); (2) the matrix in
  `start_execution` returning `StartOutcome{reference, created}`, bridge skips `Scheduled` and sets
  `started` for UseExisting/dedup; (3) typed `ActivityExecutionAlreadyStarted` at the edge — code
  `AlreadyExists` + `ActivityExecutionAlreadyStartedFailure` detail (RunId/StartRequestId) in the
  `grpc-status-details-bin` trailer, verified to round-trip. Covered by
  `conflict_policy_against_live_run`, `reuse_policy_against_terminal_run`,
  `activity_already_started_status_carries_typed_detail`.
- [x] 1.4 Edge `activity_execution_key` resolves an empty `run_id` via the pointer (`None` →
  `NotFound "activity not found for ID: <id>"`); blanket "run_id required" rejection removed.
- [x] 1.5 `delete_execution` clears the pointer iff it still points at the deleted run (read-your-write).
- [ ] 1.6* Property test: pointer + node never tear under concurrent Starts; bare-id resolution
  matches the most-recent run across reuse/conflict outcomes (`// Feature: activity-executions-first-class, Property 2`).
- [ ] 1.7 Verify: `TestDelete/DeleteNonExistent`, `DeleteActivityNoRunID`, bare-id `Describe`.

## Stage 2 — Describe info fidelity: worker identity + run state (Requirement 3)

- [x] 2.1 `last_worker_identity` on `ActivityState` (prost tag 18, additive); `identity` on
  `ActivityEvent::Started`; set in the state-machine apply; 3 test constructors fixed.
- [x] 2.2 Worker identity threaded `PollActivityTaskQueue.identity` →
  `ActivityBridge::poll_activity_task` → `record_started`.
- [x] 2.3 `worker_identity` carried on `ActivityDescription` (`description_from`) and set on
  `ActivityExecutionInfo.last_worker_identity` in `chasm_activity_info`.
- [x] 2.4 **Verified:** the `:4831` `last_worker_identity` assertion goes **9 failures → 0**; the
  TestComplete/TestDelete/TestDescribe setups now clear the worker-identity gate (and fail downstream
  on token validation / proto fidelity instead). Leaf pass-rate 1 → 6.

## Stage 3 — Task-token validation on responses (Requirement 4)

- [ ] 3.1 On `RespondActivityTaskCompleted`/`Failed`/`Canceled`, validate the decoded token
  (stale attempt stamp, mismatched component ref, namespace mismatch) → v1.31.0 status.
- [ ] 3.2 Verify: `TestComplete/StaleToken`, `StaleAttemptToken`, `MismatchedTokenComponentRef`,
  `MismatchedTokenNamespace`.

## Stage 4 — Describe proto fidelity + long-poll + count (Requirements 5, 6, 7)

- [ ] 4.1 Reconcile the `DescribeActivityExecution` response encoding (retry policy / payload)
  with v1.31.0; verify `TestDescribeActivityExecution_Completed`.
- [ ] 4.2 Honour the caller deadline in the describe long-poll; verify
  `TestDescribeActivityExecution_DeadlineExceeded`.
- [ ] 4.3 Fix `CountActivityExecutions` by `ActivityId`; verify `TestCountActivityExecutions/CountByActivityId`.

## Stage 5 — Checkpoint

- [ ] 5.1 Full `TestStandaloneActivityTestSuite` re-run; record the new pass-rate in the
  conformance FINDINGS (C1). Triage any residual failures into new tasks (or cross-reference
  `runtime-activity-pump` / `runtime-activity-timeouts` for heartbeat/retry/timeout-owned cases).

---

## Notes

**Cross-spec note.** Heartbeat, retry re-dispatch, and timeout semantics are owned by
`runtime-activity-pump` / `runtime-activity-timeouts`; embedded by-id RPCs are
`api-conformance-activity-by-id`. Where a residual C1 failure traces there, cross-reference rather
than duplicate (no spec sprawl).
