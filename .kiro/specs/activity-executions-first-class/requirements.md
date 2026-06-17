# Requirements Document

## Introduction

Standalone Activity Executions are first-class durable objects — an activity addressed
by `(namespace, activity_id)` with its own run, not embedded in a workflow. The CHASM
substrate, the activity component, the edge `*ActivityExecution` bridge, the shared
visibility plane, and the edge admission validation already exist (`chasm-foundation`
spec + commit `5b5faddd`). This spec implements the **execution-engine semantics** the
placeholder reserved: the durable, addressable activity object and the behaviour the
eight `*ActivityExecution` RPCs must exhibit to be conformant.

Scope is driven by the Tier-2 functional conformance corpus. `TestStandaloneActivityTestSuite`
was measured against a statically-SA-enabled `tokeirad` at **1 pass / 31 fail** (recorded
in `temporal-functional-conformance/reference/FINDINGS.md`, cluster C1). The failures
decompose into the requirements below. This spec is the designated home for that work;
it is **not** the embedded-activity by-id path (`api-conformance-activity-by-id`), which
resolves activities inside a workflow run via a `RunKey`.

All behaviour is ground-truthed to Temporal **v1.31.0** per AGENTS §8 (`chasm/lib/activity`
for behaviour, `proto/upstream/` for wire shape). The Implementer mandate from the
conformance FINDINGS binds every requirement: no kernel additions, configuration raised
not invented, ambiguity raised not guessed.

## Glossary

- **Standalone activity (SA)**: an activity execution addressed by `(namespace, activity_id)`,
  identified internally by the CHASM `ExecutionKey {namespace_id, business_id, run_id}`
  where `business_id == activity_id`. It has no enclosing workflow run.
- **Current run**: the run a bare `(namespace, activity_id)` resolves to when no `run_id`
  is supplied — the most recent run for that `activity_id`, per v1.31.0's CHASM current-run
  resolution.
- **Current-run pointer**: the authoritative `(namespace_id, business_id) → run_id` mapping
  that resolves the current run. Lives alongside the node store; never derived from
  visibility.
- **Id reuse / conflict policy**: `ActivityIdReusePolicy` / `ActivityIdConflictPolicy` on
  `StartActivityExecution`, governing whether/how a new Start may supersede the current run
  (defaults: `ALLOW_DUPLICATE` / `FAIL`, per `validateAndNormalizeIDPolicy @ v1.31.0`).
- **Edge / Runtime / Kernel**: as in the conformance FINDINGS — admission+translation /
  authoritative execution / pure state machine. SA execution lives in the runtime's CHASM
  engine + the activity component; the kernel is not extended.

## Requirements

### Requirement 1: Authoritative current-run resolution (foundational)

**User Story:** As an SDK or operator, I want to describe/poll/cancel/terminate/delete a
standalone activity by `activity_id` alone, so that I can address it without holding a run id.

This is the foundational gap: tokeira's `start_execution` keys the node store on the full
`{namespace_id, business_id, run_id}` and mints a fresh `run_id` per Start, so there is no
`(namespace_id, business_id) → current run` resolution. The edge therefore rejects an empty
`run_id` outright. v1.31.0 resolves the current run through the CHASM framework when `RunId`
is empty (`chasm.NewComponentRef(ExecutionKey{NamespaceID, BusinessID, RunID})` +
`ReadComponent`, `chasm/lib/activity/handler.go @ v1.31.0`).

#### Acceptance Criteria

1. WHEN an SA RPC (`Describe`/`Poll`/`RequestCancel`/`Terminate`/`Delete`/`Get*`) is received
   with an empty `run_id`, THE Edge SHALL resolve `(namespace_id, activity_id)` to the current
   run via the authoritative current-run pointer (never via the visibility projection).
2. IF `(namespace_id, activity_id)` has no current run, THEN THE Edge SHALL return gRPC
   `NOT_FOUND` with message `activity not found for ID: <activity_id>` (mirrors
   `frontend.go @ v1.31.0`; the message already lands via `map_activity_not_found`).
3. WHEN `run_id` is non-empty, THE Edge SHALL address that exact run (subject to the existing
   UUID validation), not the current-run pointer.
4. WHEN an activity is closed (terminal), THE current-run pointer SHALL continue to resolve
   to that most-recent run until a new Start supersedes it per the reuse policy — so a
   delete-then-describe and a describe of a terminal activity both resolve correctly
   (ground-truth the closed-run resolution against `chasm/lib/activity @ v1.31.0`).
5. THE resolution SHALL be authoritative and read-your-write consistent: a `Delete` followed
   by a `Describe` (no run_id) SHALL observe the deletion immediately (no eventual-consistency
   window), because correctness must not rest on the derived visibility projection
   (root `AGENTS.md`: history is authority; visibility is a repairable projection outside the
   correctness path).

### Requirement 2: Current-run pointer maintenance + id reuse/conflict policy

**User Story:** As a user starting activities, I want id-reuse and id-conflict policies to
behave like v1.31.0, so that re-using an `activity_id` is governed, not silently duplicated.

#### Acceptance Criteria

1. WHEN `StartActivityExecution` commits a new run, THE Runtime SHALL update the current-run
   pointer to the new `run_id` under the same OCC/CAS fencing the node store uses, so the
   pointer and the root node move together (no torn state, no lost update under concurrent
   Starts).
2. THE Edge SHALL normalize `IdReusePolicy`/`IdConflictPolicy` (defaults `ALLOW_DUPLICATE` /
   `FAIL`, `validateAndNormalizeIDPolicy @ v1.31.0`) and THE Runtime SHALL enforce them against
   the current run: a conflicting Start against a running current run SHALL fail per
   `IdConflictPolicy`; reuse against a closed current run SHALL be admitted/rejected per
   `IdReusePolicy`.
3. WHEN a conflicting Start is rejected, THE Edge SHALL return the v1.31.0 error naming the
   current run (`ActivityExecutionAlreadyStarted` carries `CurrentRunID`,
   `chasm/lib/activity/handler.go:91 @ v1.31.0`).

### Requirement 3: Describe info fidelity — worker identity and run state

**User Story:** As a caller, I want `DescribeActivityExecution` to report the worker that
picked up the activity and its precise run state, so that observation matches v1.31.0.

#### Acceptance Criteria

1. THE persisted `ActivityState` SHALL record the identity of the worker that polled/started
   the current attempt, and `DescribeActivityExecution.info.last_worker_identity` SHALL reflect
   it (currently always empty — the dominant cause of the C1 failures, `standalone_activity_test.go:4831`).
2. `info.run_state`, `info.attempt`, `info.last_started_time`, `info.last_failure`, and
   `info.heartbeat_details` SHALL match v1.31.0 for each lifecycle state
   (`ACTIVITY_EXECUTION_STATUS_RUNNING` + `PENDING_ACTIVITY_STATE_STARTED`, attempt `1` after a
   first poll, etc.).
3. THE worker identity SHALL be carried on the worker poll (`PollActivityTaskQueue.identity`)
   into the `Started` transition; it SHALL NOT require a kernel change (it is activity-component
   state).

### Requirement 4: Task-token validation on responses

**User Story:** As the server, I want to reject stale or mismatched activity task tokens, so
that completion/failure responses are safe.

#### Acceptance Criteria

1. WHEN `RespondActivityTaskCompleted`/`Failed`/`Canceled` is received, THE Edge/Runtime SHALL
   reject a token whose attempt stamp is stale, whose component reference does not match the
   resolved activity, or whose namespace differs, with the v1.31.0 status/message
   (`TestComplete/StaleToken`, `StaleAttemptToken`, `MismatchedTokenComponentRef`,
   `MismatchedTokenNamespace`). Ground-truth the exact codes against `chasm/lib/activity @ v1.31.0`.

### Requirement 5: Describe response proto fidelity

#### Acceptance Criteria

1. THE `DescribeActivityExecution` response SHALL encode retry policy and payload fields exactly
   as v1.31.0 (the conformance proto-diff shows a retry-policy/payload mismatch surfacing an
   `@invalid` marker, `TestDescribeActivityExecution_Completed`).

### Requirement 6: Describe long-poll honours the caller deadline

#### Acceptance Criteria

1. WHEN `DescribeActivityExecution` is a long-poll, THE Edge SHALL return on the caller's gRPC
   deadline (not its own fixed budget) so a caller deadline that expires first yields the
   correct deadline behaviour (`TestDescribeActivityExecution_DeadlineExceeded`).

### Requirement 7: List/Count by activity id

#### Acceptance Criteria

1. `CountActivityExecutions` with a query that constrains `ActivityId` SHALL return the correct
   count (`TestCountActivityExecutions/CountByActivityId`), consistent with the
   archetype-scoped visibility already wired by `chasm-foundation`.

## Out of scope / deferred

- Heartbeat tracking, retry re-dispatch, and timeout semantics are owned by
  `runtime-activity-pump` / `runtime-activity-timeouts`; this spec consumes them, it does not
  re-specify them. Where a C1 failure traces to those, cross-reference rather than duplicate.
- Embedded (in-workflow) by-id activity RPCs are `api-conformance-activity-by-id`.
