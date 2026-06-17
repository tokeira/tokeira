# Design: Activity Executions First-Class

## Overview

`chasm-foundation` delivered the CHASM substrate, the activity component
(`tokeira-chasm-activity`), the edge `*ActivityExecution` bridge, the shared versioned-
snapshot visibility plane, and (commit `5b5faddd`) the edge admission validation +
`NotFound` fidelity. Against that, `TestStandaloneActivityTestSuite` measures **1/31**
(FINDINGS cluster C1). This design closes the execution-engine gaps behind the remaining
30 failures. The kernel is not extended; all work is edge + runtime/storage (the activity
component and the CHASM engine), per the Implementer mandate.

Design items, in dependency order. **Item 1 (current-run index) is foundational and must be
agreed before any code** — it carries a storage-schema decision with compat implications
(FINDINGS Implementer-mandate rule 3/4: raise config/schema, don't guess).

---

## Design Item 1 — Authoritative current-run index (foundational)

### The gap

`ChasmEngine::start_execution` (`tokeira-runtime/src/chasm/engine.rs:498`) mints a fresh
`run_id` per Start and conflict-checks only the **exact** `ExecutionKey
{namespace_id, business_id, run_id}` (it errors iff `load_tree(req.key)` is non-empty — which,
with a fresh run_id, never fires across runs). The node store
(`tokeira-storage/src/chasm.rs`) keys every node by the full `ExecutionKey`. There is therefore
**no `(namespace_id, business_id) → run` mapping**, so the edge cannot resolve an activity by id
alone and `activity_execution_key` rejects an empty `run_id` with `INVALID_ARGUMENT`. This is the
root blocker for `DeleteNonExistent`, `DeleteActivityNoRunID`, and every describe/poll/cancel/
terminate that omits `run_id`.

### v1.31.0 ground truth

A standalone activity is a CHASM entity keyed `ExecutionKey {NamespaceID, BusinessID, RunID}`
(`BusinessID == activity_id`). Handlers build `chasm.NewComponentRef[*Activity](ExecutionKey{…,
RunID: req.GetRunId()})` and call `ReadComponent`/`PollComponent`
(`chasm/lib/activity/handler.go:116-124,179-197 @ v1.31.0`). When `RunID` is empty, the CHASM
framework resolves the **current run** for the business id. New-run creation on Start is governed
by `BusinessIDReusePolicy`/`BusinessIDConflictPolicy` (mapped from the request's
`ActivityIdReusePolicy`/`ActivityIdConflictPolicy`, `handler.go:19-25`); a conflict against a live
current run yields `ActivityExecutionAlreadyStarted` carrying the `CurrentRunID` (`handler.go:91`).
This is the direct analog of Temporal's workflow current-execution record + workflow-id
reuse/conflict policy. **Ground-truth callout:** confirm, in `chasm/` framework source, (a) exactly
when the current-run pointer is advanced (Start commit) and how it is fenced, and (b) how a *closed*
entity resolves a bare-id read (does it return the most-recent terminal run?), before finalizing AC4
below.

### Design

Add an **authoritative current-run pointer** beside the node store — never a visibility lookup
(a bare-id describe/delete is a read-your-write against authoritative state; resolving it through
the derived projection would break read-your-write and violate the core invariant: root
`AGENTS.md` — history is authority, visibility is a transition-derived, repairable projection
*outside* the correctness path).

- **Mapping.** `(namespace_id, business_id) → CurrentRun { run_id, status, vt_epoch }`. `status`
  lets the engine apply the reuse/conflict policy without loading the run; `vt_epoch` (the run's
  `VersionedTransition` at last advance) provides a fence. Minimum viable is `run_id` alone; carry
  `status`/`epoch` only if the policy/fencing needs them (decide in review).
- **Storage shape.**
  - In-memory (`InMemoryChasmNodeStore`): a second `Mutex<BTreeMap<(NamespaceId, BusinessId),
    CurrentRun>>` alongside `executions`, mutated under the same lock acquisition as the node write
    so the pointer and root node never tear.
  - DSQL: a `chasm_current_run` table keyed `(namespace_id, business_id)`. **Migration shape is a
    decision for review:** if no DSQL baseline has been cut, fold it into the node-table base
    migration; otherwise add `VNNN` (additive — no destructive state-format break). Confirm the
    baseline status against `tokeira-storage/src/dsql/` migrations before writing the migration.
- **Fencing (CAS/OCC).** The pointer advance is part of the Start commit and uses the node store's
  existing OCC discipline. On Start: read the current pointer; evaluate the reuse/conflict policy
  against `status`; if admitted, write the root node **and** CAS the pointer to the new `run_id`
  in one atomic unit (DSQL: same transaction; in-memory: same lock). A losing concurrent Start
  observes the CAS failure and maps to the conflict error. This mirrors `commit`'s
  `NodePersistOutcome::Conflict` handling so there is one fencing model, not two.
- **Run resolution.** Add `ChasmNodeRepository::current_run(namespace_id, business_id) ->
  Option<RunId>` (and the engine method that wraps it). The edge's `activity_execution_key` resolves
  an empty `run_id` through it: `Some(run)` → build the key; `None` → `NotFound "activity not found
  for ID: <activity_id>"` (the message already exists via `map_activity_not_found`). A non-empty
  `run_id` bypasses the pointer (addresses the exact run), preserving today's behaviour.
- **Reuse/conflict policy.** The edge normalizes `IdReusePolicy`/`IdConflictPolicy`
  (`ALLOW_DUPLICATE`/`FAIL` defaults); the engine enforces them at the CAS point against the
  current run's `status`. Rejection returns the v1.31.0 already-started error naming `CurrentRunID`.
- **Closed-run + delete semantics.** A terminal run remains the current run (bare-id reads resolve
  it) until a new Start supersedes it per `IdReusePolicy`. `delete_execution` of the current run
  clears (or supersedes) the pointer so a subsequent bare-id describe is `NotFound` — preserving
  read-your-write. Exact closed-run resolution is pinned to the v1.31.0 ground-truth callout above.

### Acceptance check

Drives R1, R2, and unblocks the no-run_id Delete/Describe tests. Verify with the isolated
`TestDelete/DeleteNonExistent`, `DeleteActivityNoRunID`, and a bare-id `Describe` once landed.

---

## Design Item 2 — Describe info fidelity (worker identity + run state)

Thread the polling worker's identity into persisted state and surface it. Concretely
(`tokeira-chasm-activity` + `tokeira-edge`): add `last_worker_identity` to `ActivityState`
(prost tag 18, additive); give `ActivityEvent::Started` an `identity` field (the `CancelRequested`
event already carries `identity`, so there is precedent) and set it in the state-machine apply;
thread the worker identity from the `PollActivityTaskQueue` handler → `ActivityBridge::poll_activity_task`
→ `record_started`; carry it on `ActivityDescription`; and set
`ActivityExecutionInfo.last_worker_identity` in `chasm_activity_info`
(`workflow_service.rs` — every other field there is already populated). Verify against the
`standalone_activity_test.go:4831` helper (status RUNNING, run_state STARTED, attempt 1,
last_started_time set). Drives R3. This is the highest-leverage item after Item 1 — it blocks the
setup validation in most TestComplete/TestDelete/TestDescribe sub-tests.

## Design Item 3 — Task-token validation on responses

On `RespondActivityTaskCompleted`/`Failed`/`Canceled`, validate the decoded `ActivityTaskToken`
against the resolved activity: stale attempt stamp, mismatched component ref, and namespace
mismatch each map to the v1.31.0 status. tokeira already fences attempts via the state `stamp`
(used in `poll_activity_task`); extend the respond path to surface stale/mismatch as the conformant
error rather than a generic one. Ground-truth the exact codes against `chasm/lib/activity @ v1.31.0`.
Drives R4.

## Design Item 4 — Describe response proto fidelity

Reconcile the `DescribeActivityExecution` response encoding (retry policy / payload) with v1.31.0;
the conformance proto-diff (`@invalid`) localizes the offending field. Translation-only. Drives R5.

## Design Item 5 — Describe long-poll deadline

`describe_activity_execution`'s long-poll currently uses the engine's fixed long-poll budget;
honour the caller's gRPC deadline (min of the two) so deadline-sensitive tests observe the caller
deadline. Edge-only. Drives R6.

## Design Item 6 — List/Count by activity id

Confirm the archetype-scoped `CountActivityExecutions` evaluates an `ActivityId` query predicate
correctly against the visibility store wired by `chasm-foundation`. Visibility-query-only (does not
touch the correctness path). Drives R7.

---

## Sequencing

1. **Item 1 (current-run index)** — agree schema + fencing in review, then implement; unblocks the
   bare-id paths.
2. **Item 2 (worker identity/run state)** — highest remaining leverage (setup validation).
3. Items 3–6 in any order; each is bounded and independently verifiable against its named sub-tests.

Each item is verified by re-running the relevant `TestStandaloneActivityTestSuite` sub-tests against
a statically-SA-enabled `tokeirad` (the suite's `OverrideDynamicConfig(activity.Enabled)` does not
reach an out-of-process server — see the FINDINGS runbook), and the C1 pass-rate in FINDINGS is
updated as items land.
