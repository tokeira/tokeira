# Implementation Plan: Activity Executions First-Class

Closes the remaining `TestStandaloneActivityTestSuite` (C1) gaps on top of `chasm-foundation`
+ commit `5b5faddd` (edge admission validation + NotFound fidelity, already landed). Measured
baseline: **1/31** (`temporal-functional-conformance/reference/FINDINGS.md`). Every task is
ground-truthed to v1.31.0 (`chasm/lib/activity @ v1.31.0`, AGENTS §8) and verified by re-running
the named sub-tests against a statically-SA-enabled `tokeirad` (FINDINGS runbook). No kernel
additions.

Legend: `[ ]` open · `[ ]*` optional test sub-task.

---

## Stage 0 — Design gate (no code)

- [ ] 0.1 **Agree Item 1 (current-run index) before coding it.** Resolve in design review:
  (a) DSQL migration shape — fold into the node-table base if no baseline is cut, else additive
  `VNNN` (confirm baseline status in `tokeira-storage/src/dsql/`); (b) pointer contents
  (`run_id` only vs `+status/+epoch`); (c) `delete` of the current run → pointer effect
  (read-your-write `NotFound`); (d) closed-run bare-id resolution, pinned to the
  `chasm/` framework current-run ground-truth callout in `design.md`. **Ground-truth** the
  pointer-advance fencing + closed-run read against `chasm/` + `chasm/lib/activity @ v1.31.0`.

## Stage 1 — Current-run index (Requirements 1, 2)

- [ ] 1.1 Add the current-run pointer to the node-store trait + in-memory store:
  `current_run(namespace_id, business_id) -> Option<RunId>`, and an atomic
  set-on-start under the same lock as the node write.
- [ ] 1.2 Add the DSQL backing (per the 0.1 decision) with CAS/OCC fencing consistent with the
  node store's commit path; pointer + root node committed in one transaction.
- [ ] 1.3 Advance/CAS the pointer in `ChasmEngine::start_execution`, enforcing
  `IdReusePolicy`/`IdConflictPolicy` against the current run's status; reject conflicts with the
  v1.31.0 already-started error naming `CurrentRunID`.
- [ ] 1.4 Resolve empty `run_id` in the edge (`activity_execution_key`) via the pointer; `None`
  → `NotFound "activity not found for ID: <id>"`. Remove the blanket "run_id is required"
  rejection for the SA paths.
- [ ] 1.5 `delete_execution` of the current run clears/supersedes the pointer (read-your-write).
- [ ] 1.6* Property test: pointer + node never tear under concurrent Starts; bare-id resolution
  matches the most-recent run across reuse/conflict outcomes.
- [ ] 1.7 Verify: `TestDelete/DeleteNonExistent`, `DeleteActivityNoRunID`, bare-id `Describe`.

## Stage 2 — Describe info fidelity: worker identity + run state (Requirement 3)

- [ ] 2.1 Add `last_worker_identity` to `ActivityState` (prost tag 18, additive); add `identity`
  to `ActivityEvent::Started`; set it in the state-machine apply; fix the 3 test constructors.
- [ ] 2.2 Thread the worker identity from `PollActivityTaskQueue.identity` →
  `ActivityBridge::poll_activity_task` → `record_started`.
- [ ] 2.3 Carry `worker_identity` on `ActivityDescription` (`description_from`) and set
  `ActivityExecutionInfo.last_worker_identity` in `chasm_activity_info`.
- [ ] 2.4 Verify the `standalone_activity_test.go:4831` helper (status/run_state/attempt/
  last_started_time) across the TestComplete/TestDelete/TestDescribe setups it gates.

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

**Cross-spec note.** Heartbeat, retry re-dispatch, and timeout semantics are owned by
`runtime-activity-pump` / `runtime-activity-timeouts`; embedded by-id RPCs are
`api-conformance-activity-by-id`. Where a residual C1 failure traces there, cross-reference rather
than duplicate (no spec sprawl).
