# Requirements Document

## Introduction

`StartWorkflowExecution` carries a per-request `WorkflowIdConflictPolicy` (Fail / UseExisting /
TerminateExisting) that decides what happens when a **running** current execution already exists for
the same `(namespace, workflow_id)`. tokeira does not honor it: the per-request policy is dropped at
commit time, the storage layer applies a single store-wide `CurrentExecutionConflictPolicy::Reject`,
and the resulting generic `CommitResult::Conflict` is retried by the lane as if it were a transient
OCC/CAS conflict. Under concurrency (e.g. N callers racing to start the same id) this surfaces as
`lane OCC retry exhausted after 6 conflicts: current execution already exists` and an opaque failure,
instead of the deterministic conflict-policy outcome.

This feature makes the per-request `WorkflowIdConflictPolicy` authoritative: exactly one concurrent
start wins, and every other start resolves per policy — Fail returns `WorkflowExecutionAlreadyStarted`,
UseExisting attaches to the winning run (recording a `WorkflowExecutionOptionsUpdated` event when
`OnConflictOptions` requests it), and TerminateExisting terminates the incumbent then starts anew. It
also adds the per-run request-id → authoring-event tracking (`RequestIdInfos`) that
`DescribeWorkflowExecution` surfaces.

The behaviour is ground-truthed to Temporal v1.31.0: `service/history/api/workflow_id_dedup.go`
(`ResolveDuplicateWorkflowID` / `ResolveWorkflowIDConflictPolicy`) and
`service/history/api/startworkflow/api.go` (`handleUseExistingWorkflowOnConflictOptions`) @ v1.31.0.
This is a core `StartWorkflowExecution` correctness surface; the Nexus `WorkflowRunOperation` corpus
test `TestNexusAsyncOperationWithMultipleCallers` is one consumer but not the only one — any
concurrent same-id start with a conflict policy is affected.

## Glossary

- **Current execution** — the open (CREATED or RUNNING) run that owns the `(namespace, workflow_id)`
  current-execution pointer.
- **WorkflowIdConflictPolicy** — per-request policy for a *running* current execution: `Fail`,
  `UseExisting`, `TerminateExisting`.
- **WorkflowIdReusePolicy** — per-request policy for a *closed* current execution (out of scope here
  except where it interacts; tokeira's existing behaviour is preserved).
- **Terminal conflict** — a start outcome that policy makes final (e.g. Fail → already-started); it
  MUST NOT be retried by the lane.
- **Transient conflict** — an optimistic-concurrency (CAS/seq) collision that is safe to retry.
- **WorkflowExecutionAlreadyStarted** — the serviceerror returned for Fail (and reuse-policy denials);
  maps to gRPC `ALREADY_EXISTS`.
- **OnConflictOptions** — start-request options applied when UseExisting resolves to an existing run:
  `attach_request_id`, `attach_completion_callbacks`, `attach_links`.
- **Attach** — the UseExisting path that records the new request's metadata onto the existing run via
  a `WorkflowExecutionOptionsUpdated` event instead of starting a new run.
- **RequestIdInfo** — per-run mapping from a request id to the event that authored it (`event_id`,
  `event_type`, `buffered`), surfaced by `DescribeWorkflowExecution.WorkflowExtendedInfo`.

## Requirements

### Requirement 1: The per-request conflict policy is authoritative for a running current execution

**User Story:** As a caller starting a workflow whose id is already running, I want my declared
`WorkflowIdConflictPolicy` to decide the outcome, not a server-wide default.

#### Acceptance Criteria

1. WHEN a start finds a running current execution for the same `(namespace, workflow_id)` THEN the
   system SHALL resolve the outcome using the request's `WorkflowIdConflictPolicy`, not a store-wide
   conflict setting.
2. WHEN the conflict policy is `Fail` THEN the system SHALL deny the start with
   `WorkflowExecutionAlreadyStarted` and SHALL NOT create a new run.
3. WHEN the conflict policy is `UseExisting` THEN the system SHALL NOT create a new run and SHALL
   resolve to the existing run (see Requirement 3).
4. WHEN the conflict policy is `TerminateExisting` THEN the system SHALL terminate the existing run
   and start the new one (see Requirement 4).
5. WHEN the current execution is *closed* (not running) THEN the system SHALL apply the existing
   `WorkflowIdReusePolicy` behaviour unchanged (this feature does not alter the reuse path).

### Requirement 2: Fail conflicts are terminal, not retried

**User Story:** As an operator, I want an already-running id with Fail policy to return a clean
already-started error rather than an opaque OCC-exhaustion error.

#### Acceptance Criteria

1. WHEN a `Fail` conflict is resolved THEN the runtime lane SHALL treat it as terminal and SHALL NOT
   re-run the start through OCC retry.
2. WHEN a `Fail` conflict is returned to the edge THEN the system SHALL map it to gRPC
   `ALREADY_EXISTS` with the `WorkflowExecutionAlreadyStarted` message shape of v1.31.0.
3. THE SYSTEM SHALL continue to retry genuine transient OCC/CAS conflicts (sequence/version
   collisions) so that a concurrent race still converges to exactly one winning start.

### Requirement 3: UseExisting attaches to the existing run

**User Story:** As a caller using `UseExisting`, I want my request attached to the already-running run
and to receive that run's id.

#### Acceptance Criteria

1. WHEN `UseExisting` resolves to an existing running run THEN the system SHALL return that run's id
   with `started = false` and status `RUNNING`, and SHALL NOT schedule a new workflow task for the
   attach.
2. WHEN `OnConflictOptions.attach_request_id` is set THEN the system SHALL record a
   `WorkflowExecutionOptionsUpdated` event on the existing run carrying the new request id.
3. WHEN `OnConflictOptions.attach_completion_callbacks` and/or `attach_links` are set THEN the system
   SHALL include the request's completion callbacks and/or links on that
   `WorkflowExecutionOptionsUpdated` event.
4. WHEN no `OnConflictOptions` is present THEN the system SHALL resolve to the existing run without
   recording an options-updated event.
5. WHEN the existing run completes between the conflict check and the attach THEN the system SHALL
   re-evaluate against the reuse policy (consistent with v1.31.0's `ErrWorkflowCompleted`
   re-evaluation), rather than attaching to a closed run.

### Requirement 4: TerminateExisting replaces the incumbent

**User Story:** As a caller using `TerminateExisting`, I want the running run terminated and my new run
started.

#### Acceptance Criteria

1. WHEN `TerminateExisting` resolves against a running run THEN the system SHALL terminate the
   existing run and start the new run, transferring the current-execution pointer to the new run.
2. THE termination and new start SHALL be observable as a terminated incumbent followed by a started
   successor (no lost current-execution pointer, no two concurrent current executions).

### Requirement 5: Request-id tracking is surfaced

**User Story:** As a caller or operator, I want to see which request id authored which event on a run.

#### Acceptance Criteria

1. THE SYSTEM SHALL maintain, per run, a map from request id to the event that authored it
   (`event_id`, `event_type`), for the start request and for each attached request.
2. WHEN a run is started THEN its starting request id SHALL map to the
   `WORKFLOW_EXECUTION_STARTED` event.
3. WHEN a request attaches via `UseExisting` THEN its request id SHALL map to the
   `WORKFLOW_EXECUTION_OPTIONS_UPDATED` event.
4. WHEN `DescribeWorkflowExecution` is called THEN `WorkflowExtendedInfo.RequestIdInfos` SHALL report
   these entries with `buffered = false` and `event_id >= FirstEventID`.

### Requirement 6: Concurrent same-id starts converge deterministically

**User Story:** As a caller in a fan-out (e.g. multiple concurrent Nexus operations targeting one
handler workflow id), I want exactly one start to win and the rest to follow policy.

#### Acceptance Criteria

1. WHEN N concurrent starts target the same `(namespace, workflow_id)` with `Fail` THEN exactly one
   SHALL succeed and the remaining `N-1` SHALL each return `WorkflowExecutionAlreadyStarted`.
2. WHEN N concurrent starts target the same id with `UseExisting` THEN exactly one SHALL start the run
   and the remaining `N-1` SHALL attach (with `attach_request_id` producing one
   `WORKFLOW_EXECUTION_OPTIONS_UPDATED` per attach).
3. THE SYSTEM SHALL NOT surface an OCC-exhaustion error for any of these outcomes.
