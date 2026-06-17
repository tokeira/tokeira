# Design: Activity Executions First-Class

## Overview

`chasm-foundation` delivered the CHASM substrate, the activity component
(`tokeira-chasm-activity`), the edge `*ActivityExecution` bridge, the shared versioned-
snapshot visibility plane, and (commit `5b5faddd`) the edge admission validation +
`NotFound` fidelity. Against that, `TestStandaloneActivityTestSuite` measures **1/31**
(FINDINGS cluster C1). This design closes the execution-engine gaps behind the remaining
30 failures. The kernel is not extended; all work is edge + runtime/storage (the activity
component and the CHASM engine), per the Implementer mandate.

Design items, in dependency order. **Item 1 (current-run index) is foundational.** Its schema and
fencing decisions — which carried compat implications (FINDINGS Implementer-mandate rule 3/4) — are
now **resolved and recorded below**: the Stage 0 design gate is closed. The one remaining
externally-ground-truthed point (closed-run bare-id resolution) is a read-source-first step inside
Stage 1, not a blocker.

## Architecture

This feature lives entirely in the **edge** and **runtime/storage** planes; `tokeira-kernel` is not
extended (Implementer mandate). Placement:

- **Edge** (`tokeira-edge`): empty-`run_id` resolution in `activity_execution_key`, id
  reuse/conflict normalization, task-token validation on responses, the describe long-poll deadline,
  and `Count`-by-id — all admission/translation concerns.
- **Runtime** (`tokeira-runtime` CHASM engine + `tokeira-chasm-activity`): the authoritative
  current-run pointer advance under the Start commit, reuse/conflict enforcement, and the
  worker-identity / run-state tracking on the activity component.
- **Storage** (`tokeira-storage`): the current-run pointer beside the `chasm_node` store (in-memory
  map + DSQL `chasm_current_run` table), fenced with the node store's existing OCC/CAS.
- **Projection** (`tokeira-projection`): unchanged for correctness; `Count`-by-id is a
  visibility-query concern only.

The current-run pointer is the CHASM analog of the workflow path's existing `current_execution`
table (migration `V003`): authoritative, fenced at the Start commit, and **never** derived from the
visibility projection.

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
reuse/conflict policy. **Ground-truth — CONFIRMED (task 1.0), verified against v1.31.0:** the
framework keeps a dedicated `current_executions` row keyed `(shard, namespace_id, workflow_id,
archetype_id)` — *no run_id* — carrying `(run_id, state, status, last_write_version)`, resolved on an
empty run_id via `GetCurrentExecution` (`common/persistence/sql/execution.go:681 @ v1.31.0`).
(a) **Fencing:** the row is written **co-transactionally** with the entity create
(`createWorkflowExecutionTx` → `createOrUpdateCurrentExecution`) under an optimistic
`last_write_version` conditional update; a lost race becomes `CurrentWorkflowConditionFailedError` →
the activity's `ActivityExecutionAlreadyStarted(CurrentRunID)` (`chasm/lib/activity/handler.go:91`).
(b) **Closed-run:** a terminal run's pointer **persists** — the row is updated to the terminal state,
not deleted on close; a superseding reuse is admitted only when the current row is terminal
(`CreateWorkflowModeUpdateCurrent` requires `state == COMPLETED`, `execution.go:126,155`); only an
explicit `Delete` removes the row (`DeleteFromCurrentExecutions`, `execution.go:671`), after which a
bare-id read is NotFound. **This confirms AC4/AC5 with no contradiction** and pins two design points:
`vt_epoch` is the *active* fence (the `last_write_version` conditional-update analog — review-watch
#2), and the pointer write is *co-transactional* with the root-node create (review-watch #1).

### Design

Add an **authoritative current-run pointer** beside the node store — never a visibility lookup
(a bare-id describe/delete is a read-your-write against authoritative state; resolving it through
the derived projection would break read-your-write and violate the core invariant: root
`AGENTS.md` — history is authority, visibility is a transition-derived, repairable projection
*outside* the correctness path).

- **Mapping.** `(namespace_id, business_id) → CurrentRun { run_id, status, vt_epoch }`. `status`
  lets the engine apply the reuse/conflict policy without loading the run; `vt_epoch` (the run's
  `VersionedTransition` at last advance) provides the fence. **Decided: carry all three** — Req 2
  requires `status` to enforce the reuse/conflict policy without a run load, and `vt_epoch` is the
  advance fence.
- **Storage shape.**
  - In-memory (`InMemoryChasmNodeStore`): a second `Mutex<BTreeMap<(NamespaceId, BusinessId),
    CurrentRun>>` alongside `executions`, mutated under the same lock acquisition as the node write
    so the pointer and root node never tear.
  - DSQL: a `chasm_current_run` table. **Decided: a new additive migration
    `V056__chasm_current_run.sql`** (next free version after `V055`), modeled on the existing
    workflow `current_execution` table (`V003`). It is its own one-statement migration — **not** a
    fold into `V049__chasm_node`: the pointer is a distinct cardinality (one row per `activity_id`,
    not per node), so it is a new table, not a column. DSQL-safe: spread-key
    `PRIMARY KEY (namespace_id, business_id)`, UUID columns, no `BIGSERIAL` / `CHECK` / foreign keys;
    any secondary index is a separate `CREATE INDEX ASYNC` in its own `VNNN` file. Confirm `V056` is
    still the next free version when implementing (list `crates/tokeira-storage/migrations/`).
- **Fencing (CAS/OCC).** The pointer advance is part of the Start commit and uses the node store's
  existing OCC discipline. On Start: read the current pointer; evaluate the reuse/conflict policy
  against `status`; if admitted, write the root node **and** CAS the pointer to the new `run_id`
  in one atomic unit (DSQL: same transaction; in-memory: same lock). A losing concurrent Start
  observes the CAS failure and maps to the conflict error. This mirrors `commit`'s
  `NodePersistOutcome::Conflict` handling so there is one fencing model, not two.
- **Run resolution.** Add `ChasmNodeRepository::current_run(namespace_id, business_id) ->
  Option<CurrentRun>` (and the engine method that wraps it; the edge takes `.run_id`). The edge's `activity_execution_key` resolves
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
---

## Components and Interfaces

Design Items 1–6 above carry the detailed narrative; this is the interface surface they touch.

- **`ChasmNodeRepository::current_run(namespace_id, business_id) -> Option<CurrentRun>`** (new) —
  authoritative bare-id resolution; in-memory and DSQL implementations. A wrapping `ChasmEngine`
  method exposes it to the edge.
- **`ChasmEngine::start_execution`** (`tokeira-runtime/src/chasm/engine.rs`) — advances/CAS-es the
  pointer at the Start commit and enforces `IdReusePolicy`/`IdConflictPolicy` against the current
  run's `status`.
- **Edge `activity_execution_key`** (`tokeira-edge`) — resolves an empty `run_id` via `current_run`;
  `None` → `NotFound`; a non-empty `run_id` bypasses the pointer and addresses the exact run
  (today's behaviour preserved).
- **`tokeira-chasm-activity`** — `ActivityState.last_worker_identity` and
  `ActivityEvent::Started.identity`, threaded from `PollActivityTaskQueue.identity` through
  `ActivityBridge::poll_activity_task` → `record_started` → `ActivityDescription` →
  `ActivityExecutionInfo`.
- **Edge respond path** — `ActivityTaskToken` validation (stale attempt stamp / mismatched component
  ref / namespace mismatch) on `RespondActivityTaskCompleted`/`Failed`/`Canceled`.

## Data Models

- **`CurrentRun { run_id: RunId, status: ActivityLifecycleStatus, vt_epoch: VersionedTransition }`** —
  the authoritative current-run pointer value (see Item 1 for the carry-all-three decision).
- **DSQL `chasm_current_run`** — new migration `V056__chasm_current_run.sql`, modeled on the
  workflow `current_execution` table (`V003`); spread-key `PRIMARY KEY (namespace_id, business_id)`,
  UUID columns, one `CREATE TABLE` statement, DSQL-safe subset (no `BIGSERIAL`/`CHECK`/FK).
- **`ActivityState`** — additive `last_worker_identity` (prost tag 18) and an `identity` field on
  `ActivityEvent::Started` (the `CancelRequested` event already carries `identity`, so there is
  precedent). Both are additive proto changes; no kernel change.

## Correctness Properties

Each property is verified by a `proptest` test (≥100 iterations) carrying a
`// Feature: activity-executions-first-class, Property N` tag, plus the named conformance sub-tests.

### Property 1: Current-run authority and read-your-write

**Validates: Requirements 1.1, 1.5**

A bare-`activity_id` Describe/Poll/RequestCancel/Terminate/Delete resolves through the authoritative
current-run pointer, never the visibility projection; a `Delete` followed by a bare-id `Describe`
observes the deletion immediately, with no eventual-consistency window.

### Property 2: Pointer/node atomicity under concurrent Starts

**Validates: Requirements 2.1**

The current-run pointer and the root node advance in one fenced unit (same DSQL transaction /
in-memory lock). Under concurrent Starts at most one wins the CAS; the loser maps to the conflict
error. The pointer and node never tear.

### Property 3: Id reuse/conflict fidelity

**Validates: Requirements 2.2, 2.3**

Reuse against a closed current run and conflict against a live current run match the v1.31.0
`IdReusePolicy`/`IdConflictPolicy` outcomes; a rejected conflicting Start returns the v1.31.0
already-started error naming `CurrentRunID`.

### Property 4: Describe info fidelity

**Validates: Requirements 3.1, 3.2**

`DescribeActivityExecution.info` reports `last_worker_identity`, `run_state`, `attempt`,
`last_started_time`, `last_failure`, and `heartbeat_details` matching v1.31.0 for each lifecycle
state.

### Property 5: Task-token validation safety

**Validates: Requirements 4.1**

A response carrying a stale, component-mismatched, or wrong-namespace task token is rejected with
the v1.31.0 status, never applied.

## Error Handling

All messages are verbatim to v1.31.0 and cite their source per AGENTS §8.

- **Bare id, no current run** → gRPC `NOT_FOUND` `activity not found for ID: <activity_id>`
  (`map_activity_not_found`, mirrors `frontend.go @ v1.31.0`).
- **Conflicting Start vs a live current run** → `ActivityExecutionAlreadyStarted` carrying
  `CurrentRunID` (`chasm/lib/activity/handler.go:91 @ v1.31.0`).
- **Stale / mismatched / wrong-namespace task token** → the v1.31.0 status (ground-truth the exact
  codes against `chasm/lib/activity @ v1.31.0` while implementing Item 3).
- **Describe long-poll past the caller deadline** → `DEADLINE_EXCEEDED` on the caller's gRPC
  deadline (min of caller deadline and engine budget).
- **CAS conflict on pointer advance** → mapped to the conflict error, reusing
  `NodePersistOutcome::Conflict` handling so there is one fencing model.

## Testing Strategy

- **Conformance sub-tests (authoritative).** Each item is verified by re-running its named
  `TestStandaloneActivityTestSuite` sub-tests against a **statically-SA-enabled** `tokeirad` — the
  suite's `OverrideDynamicConfig(activity.Enabled)` does not reach an out-of-process server (FINDINGS
  runbook). Item→sub-test mapping is in the Design Items and the tasks.
- **Property tests.** Pointer/node atomicity (Property 2) and bare-id resolution across
  reuse/conflict outcomes (Properties 1, 3), `proptest` ≥100 iterations, tagged as above.
- **Unit tests.** Worker-identity threading, token validation, and the describe encoding fix carry
  focused unit coverage alongside the conformance runs.
- **Checkpoint.** Stage 5 re-runs the full suite and records the new C1 pass-rate in
  `temporal-functional-conformance/reference/FINDINGS.md`; residual failures are triaged into new
  tasks or cross-referenced to `runtime-activity-pump` / `runtime-activity-timeouts`.