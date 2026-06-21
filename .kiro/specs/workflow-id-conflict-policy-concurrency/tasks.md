# Implementation Plan

## Overview

Make the per-request `WorkflowIdConflictPolicy` authoritative for a running current execution. Split
the storage commit's conflict outcome into a retryable transient conflict and a non-retryable
current-execution conflict; resolve the latter in the runtime by policy (Fail → already-started,
UseExisting → attach, TerminateExisting → terminate+start); record per-run request-id → event mapping
and surface it via `DescribeWorkflowExecution`. Ground-truthed to `workflow_id_dedup.go` and
`startworkflow/api.go @ v1.31.0`.

## Tasks

- [ ] 1. Storage: type the current-execution conflict
  - Add `CommitResult::CurrentExecutionConflict { existing_run_key, existing_status, request_ids }`;
    return it from `commit_transition` when a zero-seq start collides with an OPEN current execution.
    Keep `CommitResult::Conflict` for transient CAS/seq collisions. Retain the store-wide
    `CurrentExecutionConflictPolicy` only for the closed-execution reuse path.
  - _Requirements: 1.1, 2.3_

- [ ] 2. Kernel: already-started reject + request-id map
  - Represent `WorkflowExecutionAlreadyStarted` as a terminal kernel `Reject` distinct from a
    transient conflict. Add `WorkflowState.request_id_infos` (build-phase fold) and `RequestIdInfo`;
    record the start request id → `WORKFLOW_EXECUTION_STARTED` on start.
  - _Requirements: 2.1, 5.1, 5.2_

- [ ] 3. Kernel: UseExisting attach transition
  - Add an attach transition on the existing run that emits `WorkflowExecutionOptionsUpdated` carrying
    the attached request id / completion callbacks / links per `OnConflictOptions` flags, records the
    attached request id → `WORKFLOW_EXECUTION_OPTIONS_UPDATED` in `request_id_infos`, and schedules no
    workflow task. Verify the event's attributes/position match v1.31.0 (Risks in design.md).
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 5.3_

- [ ] 4. Kernel: TerminateExisting ordering
  - Resolve `TerminateExisting` as terminate-incumbent-then-start using the engine's existing
    current-execution transfer; no new two-run primitive. Raise a conformance defect if the per-run
    lane model cannot express it.
  - _Requirements: 4.1, 4.2_

- [ ] 5. Runtime: resolve the conflict by policy, classify terminal vs transient
  - On `CurrentExecutionConflict`, apply the request `WorkflowIdConflictPolicy`: Fail → terminal
    already-started (no OCC retry); UseExisting → submit attach, return existing run id; Terminate →
    terminate+start. Retry only transient `Conflict`. Re-evaluate reuse policy if the incumbent closed
    during the window.
  - _Requirements: 1.2, 1.3, 1.4, 2.1, 3.5, 6.1, 6.2, 6.3_

- [ ] 6. Edge: error mapping + Describe surface
  - Map already-started → gRPC `ALREADY_EXISTS` with the v1.31.0 message shape; populate
    `DescribeWorkflowExecution.WorkflowExtendedInfo.RequestIdInfos` from `request_id_infos`.
  - _Requirements: 2.2, 5.4_

- [ ] 7. Tests
  - [ ] 7.1 Kernel golden/unit: Fail reject, UseExisting attach event + request-id map,
    TerminateExisting order, closed-path unchanged.
    - _Feature: workflow-id-conflict-policy-concurrency, Property 2, Property 3, Property 5_
    - _Requirements: 2.1, 3.1, 3.2, 4.1, 1.5_
  - [ ] 7.2 Property: single winner; request-id map shape for start + K attaches.
    - _Feature: workflow-id-conflict-policy-concurrency, Property 1, Property 4_
    - _Requirements: 5.1, 6.1, 6.2_
  - [ ] 7.3 Storage: CurrentExecutionConflict vs transient Conflict; CAS property preserved.
    - _Requirements: 1.1, 2.3_
  - [ ] 7.4 Runtime: concurrent Fail (1 ok + N-1 already-started) and UseExisting (1 start + N-1
    attach) with no OCC-exhaustion; synchronize on observable state.
    - _Feature: workflow-id-conflict-policy-concurrency, Property 1_
    - _Requirements: 6.1, 6.2, 6.3_
  - [ ] 7.5 Edge: already-started → ALREADY_EXISTS; RequestIdInfos reported.
    - _Requirements: 2.2, 5.4_

- [ ] 8. Verification gate and operator re-run
  - `cargo +nightly fmt`, `cargo lint`, `cargo test`, `cargo doc -D warnings` on touched crates; then
    operator re-run of `^TestNexusWorkflowTestSuite/TestNexusAsyncOperationWithMultipleCallers`.
  - _Requirements: all_

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 1, "tasks": ["1", "2"] },
    { "wave": 2, "tasks": ["3", "4", "5"] },
    { "wave": 3, "tasks": ["6"] },
    { "wave": 4, "tasks": ["7"] },
    { "wave": 5, "tasks": ["8"] }
  ]
}
```

Wave 1 introduces the typed conflict outcome and the kernel terminal-reject + request-id map. Wave 2
implements the three policy resolutions (attach, terminate, runtime classification). Wave 3 maps the
error and Describe surface at the edge. Wave 4 tests; Wave 5 verifies.

## Notes

- **History is authority / kernel purity.** The conflict *outcome* is decided from durable current-
  execution state; the kernel stays pure (no I/O) and the runtime only classifies + routes. The
  request-id map and the options-updated event are durable history, not derived dispatch.
- **Build-phase schema.** `request_id_infos` folds into `WorkflowState`; no `ALTER`/migration while in
  build phase.
- **No new dependency.** Pure engine logic.
- **Scope discipline.** Reuse-policy (closed) path, MultiOperation, and buffered request-ids are out
  of scope (design.md). The `WorkflowExecutionOptionsUpdated` replay-rejection risk is to be confirmed
  before relying on the existing emission; fixing a malformed emission is in scope (it is the attach
  mechanism).
- **Acceptance.** Operator-run `TestNexusAsyncOperationWithMultipleCallers` (both sub-cases) is the
  conformance vehicle; the change is general `StartWorkflowExecution` behaviour, not Nexus-specific.
