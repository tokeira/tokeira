# Design Document — Workflow-Id Conflict Policy Under Concurrency

## Overview

Make the per-request `WorkflowIdConflictPolicy` authoritative at start-commit time and distinguish a
**terminal** current-execution conflict (policy-decided) from a **transient** OCC conflict
(retryable). Today the kernel carries `conflict_policy` on `StartRequest` but the storage commit
ignores it: `commit_transition` consults a store-wide `CurrentExecutionConflictPolicy` and returns a
generic `CommitResult::Conflict` for *both* a current-execution collision and a real CAS/seq
collision. The runtime lane retries `Conflict` up to its OCC bound, so a Fail collision exhausts
retries instead of returning already-started, and a UseExisting collision never attaches.

Ground truth (v1.31.0): `service/history/api/workflow_id_dedup.go`
(`ResolveWorkflowIDConflictPolicy`: Fail → already-started error; UseExisting → `ErrUseCurrentExecution`;
TerminateExisting → terminate+start) and `service/history/api/startworkflow/api.go`
(`handleUseExistingWorkflowOnConflictOptions`: emit `AddWorkflowExecutionOptionsUpdatedEvent` on the
existing run gated by `OnConflictOptions`, return existing RunId with `Started=false`).

## Architecture

```
edge StartWorkflowExecution
  └─ runtime start (lane.submit Start)
       └─ storage commit_transition
            ├─ transient CAS/seq collision      -> CommitResult::Conflict (RETRY, unchanged)
            └─ running current execution exists  -> CommitResult::CurrentExecutionConflict {
                                                       existing_run_key, existing_state, request_ids }
       └─ runtime resolves the conflict by the request's WorkflowIdConflictPolicy:
            Fail             -> terminal: WorkflowExecutionAlreadyStarted  (NO retry)
            UseExisting      -> attach transition on existing run (WorkflowExecutionOptionsUpdated
                                 when OnConflictOptions set), return existing run id
            TerminateExisting-> terminate existing run + start new (current pointer moves)
edge maps WorkflowExecutionAlreadyStarted -> gRPC ALREADY_EXISTS
```

The key structural change is splitting the storage commit's "conflict" outcome into two: a retryable
transient conflict (existing `Conflict`) and a non-retryable **current-execution conflict** that
carries the incumbent's identity/state so the runtime can apply the per-request policy. The lane's
retry loop retries only the former.

## Components and Interfaces

- **`tokeira-storage`** — `CommitResult` gains a `CurrentExecutionConflict { existing_run_key,
  existing_state, existing_status, request_ids }` variant, returned when a zero-seq start collides
  with an open current execution. The store-wide `CurrentExecutionConflictPolicy` is retained only
  for the *closed*-execution reuse path (Reject / AllowAfterClose); the *running* path no longer
  decides the outcome (the request policy does). `commit_transition` reports the collision; it does
  not deny.
- **`tokeira-runtime` (lane / start path)** — on `CurrentExecutionConflict`, apply the request's
  `WorkflowIdConflictPolicy`:
  - `Fail` → return a terminal `StartOutcome::AlreadyStarted { run_id }` (lane does not OCC-retry).
  - `UseExisting` → submit an **attach** command against the existing run, then return
    `StartOutcome::UsedExisting { run_id }`.
  - `TerminateExisting` → submit terminate against the incumbent and start against the new run in the
    correct order (validate-precedes-commit; current pointer moves atomically per the engine's
    existing current-execution mechanics).
  The lane retries only `CommitResult::Conflict` (transient), never `CurrentExecutionConflict`.
- **`tokeira-kernel`** — 
  - An **attach** transition: applied to the existing run, emits `WorkflowExecutionOptionsUpdated`
    carrying the attached request id, completion callbacks, and links (gated by `OnConflictOptions`
    flags). This reuses the existing options-update event; no new event kind is introduced if the
    current `WorkflowExecutionOptionsUpdated` emission is sufficient (verified during implementation —
    see Risks).
  - Per-run `request_id_infos: BTreeMap<String, RequestIdInfo>` recorded on the started event (start
    request id → started event) and on each attach (attached request id → options-updated event).
  - `WorkflowExecutionAlreadyStarted` is represented as a kernel `Reject` (or an explicit start
    outcome) distinct from a transient conflict, so the runtime can classify it as terminal.
- **`tokeira-edge`** — 
  - Map the already-started outcome to gRPC `ALREADY_EXISTS` with the v1.31.0 message shape (so the
    Nexus SDK rehydrates an `ApplicationError` typed `WorkflowExecutionAlreadyStarted`).
  - Populate `DescribeWorkflowExecution.WorkflowExtendedInfo.RequestIdInfos` from the per-run map.

## Data Models

- `RequestIdInfo { event_id: i64, event_type: i32, buffered: bool }` (kernel + edge), mirroring
  `persistencespb.RequestIDInfo`. `buffered` is always `false` for tokeira (no buffered-event model).
- `WorkflowState` gains `request_id_infos: BTreeMap<String, RequestIdInfo>` (serde; build-phase
  schema change — folded into the existing state shape, no migration ALTER).
- `CommitResult::CurrentExecutionConflict { existing_run_key: RunKey, existing_status:
  ExecutionStatus, request_ids: Vec<(String, RequestIdInfo)> }`. `request_ids` lets the edge build the
  already-started error's request-id detail without a second load.
- `OnConflictOptions` already exists on `StartRequest` (`attach_request_id`,
  `attach_completion_callbacks`, `attach_links`); no shape change.

## Correctness Properties

### Property 1: Exactly one winner under concurrency

For N concurrent starts on one `(namespace, workflow_id)`, exactly one start creates the run; the
current-execution pointer references that run; no two runs are ever current simultaneously.

**Validates: Requirements 1.1, 6.1, 6.2**

### Property 2: Fail is terminal and never OCC-retried

A `Fail` resolution against a running current execution yields `WorkflowExecutionAlreadyStarted` and
the lane performs zero start re-submissions for it; transient CAS conflicts remain retried.

**Validates: Requirements 2.1, 2.3**

### Property 3: UseExisting attaches without a new run

A `UseExisting` resolution creates no new run, returns the existing run id with `started=false`, and
(with `attach_request_id`) records exactly one `WorkflowExecutionOptionsUpdated` event on the existing
run per attaching request.

**Validates: Requirements 3.1, 3.2, 6.2**

### Property 4: Request-id map is complete and well-formed

After a start plus K UseExisting attaches, `RequestIdInfos` has 1 entry mapped to
`WORKFLOW_EXECUTION_STARTED` and K entries mapped to `WORKFLOW_EXECUTION_OPTIONS_UPDATED`, all with
`buffered=false` and `event_id >= FirstEventID`.

**Validates: Requirements 5.1, 5.2, 5.3, 5.4**

### Property 5: Closed-execution reuse path is unchanged

When the current execution is closed, the start outcome is exactly what the existing
`WorkflowIdReusePolicy` path produces (no regression).

**Validates: Requirements 1.5**

## Error Handling

- `WorkflowExecutionAlreadyStarted` (Fail, and the unchanged reuse-policy denials) → gRPC
  `ALREADY_EXISTS`. The message includes the workflow id and the running run id, matching
  `generateWorkflowAlreadyStartedError @ v1.31.0`, so the SDK's Nexus `WorkflowRunOperation` surfaces
  an `ApplicationError` typed `WorkflowExecutionAlreadyStarted`.
- `CurrentExecutionConflict` is never propagated to the client; it is an internal commit outcome the
  runtime resolves. A leak of it to the edge is a bug.
- UseExisting attach racing a completion → re-evaluate the reuse policy (Req 3.5); never attach to a
  closed run.
- Transient OCC conflicts remain retried within the lane's existing bound; only the *classification*
  changes (current-execution collisions leave that bound).

## Testing Strategy

- **Kernel unit/golden**: Fail → already-started reject; UseExisting attach emits
  `WorkflowExecutionOptionsUpdated` and records the request-id map; TerminateExisting orders
  terminate-then-start; request-id map maps started vs options-updated correctly.
- **Property (proptest)**: Property 1 (single winner), Property 4 (request-id map shape for a start +
  K attaches).
- **Storage**: `commit_transition` returns `CurrentExecutionConflict` (not `Conflict`) for an open
  current-execution collision, and still returns `Conflict` for a transient CAS collision; the CAS
  property (two concurrent saves, at most one wins) is preserved.
- **Runtime**: concurrent same-id starts (Fail → 1 ok + N-1 already-started; UseExisting → 1 start +
  N-1 attaches) with no OCC-exhaustion; synchronize on observable state (no sleeps).
- **Edge**: already-started → `ALREADY_EXISTS`; `DescribeWorkflowExecution.WorkflowExtendedInfo.
  RequestIdInfos` reports the expected entries.
- **Conformance**: `TestNexusAsyncOperationWithMultipleCallers/{conflict-policy-fail,
  conflict-policy-use-existing}` is the acceptance vehicle (operator-run).

## Risks and Open Questions

- **`WorkflowExecutionOptionsUpdated` replay rejection.** A prior conformance run showed the SDK
  logging `unknown event type WorkflowExecutionOptionsUpdated` (EventID 4) when replaying the handler
  workflow. Before relying on the existing emission, verify tokeira's `WorkflowExecutionOptionsUpdated`
  attributes/position match what v1.31.0 produces and what the SDK replay accepts; if the existing
  emission is malformed, fixing it is in scope for this feature (it is the attach mechanism).
- **TerminateExisting ordering.** Must reuse the engine's existing current-execution transfer
  mechanics; this design does not introduce a new two-run transaction primitive — confirm the current
  start path can express terminate-incumbent-then-start within the lane's per-run model, and raise a
  conformance defect against the engine plan if it cannot.

## Out of Scope

- The `WorkflowIdReusePolicy` (closed-execution) path beyond preserving current behaviour.
- `MultiOperation` (update-with-start) conflict interactions.
- Buffered request-id semantics (`buffered=true`) — tokeira has no buffered-event model.

## Change Classification

**Architectural**: changes a storage commit outcome (`CommitResult`), the runtime start
classification, a kernel state field (`request_id_infos`, build-phase folded), and an edge error
mapping + Describe surface. Carries this design note; requires explicit approval before implementation.
