# Implementation Plan: Update Lifecycle API Conformance

## Overview

Make `UpdateWorkflowExecution` and `PollWorkflowExecutionUpdate` honour the `WaitPolicy` blocking
contract and populate `update_ref`, `stage`, and `outcome` from durable truth — history for
ACCEPTED/COMPLETED/rejected, the transient registry for ADMITTED — strictly per `design.md`. No new
kernel command or history event: the kernel already emits the update events; this spec derives
lifecycle metadata from them and adds the runtime wait path plus edge validation/defaulting. The
worker protocol body is unchanged. Polling is read-only.

## Tasks

- [x] 1. Runtime: lifecycle snapshot + wait path
  - [x] 1.1 Define `UpdateLifecycleSnapshot`, `UpdateLifecycleStage`, and `UpdateOutcome` in `crates/tokeira-runtime/src/update.rs`
    - Snapshot carries `{workflow_execution, update_id, update_name, stage, outcome}`; `outcome` is `Some` iff `stage == Completed`; a rejection is `Completed` + `Failure` (no separate stage). Keep this runtime-result type distinct from the edge response DTO.
    - _Requirements: 1.1, 1.3, 4.3_
  - [x] 1.2 Derive stage + outcome from durable truth
    - For a tracked update, resolve the most-advanced stage: ACCEPTED from the committed `WorkflowExecutionUpdateAccepted` event, COMPLETED from `WorkflowExecutionUpdateCompleted` (success) or `WorkflowExecutionUpdateRejected` (failure) on the run snapshot; ADMITTED from transient registry state. Build the snapshot so ACCEPTED/COMPLETED are identical before and after restart.
    - _Requirements: 1.4, 1.5, 4.1, 4.2, 4.4, 5.2, 5.3_
  - [x] 1.3 Implement `wait_for_stage(update_id, requested_stage, server_max_wait)`
    - Block until the update reaches `requested_stage` or the server-max-wait soft timeout fires; on timeout return the actual reached stage without error. Shared by the update call and the poll call. No `tokio::time::sleep`-based spin; use the existing update notification/await mechanism.
    - _Requirements: 2.3, 2.4, 2.5, 3.5_
  - [x] 1.4 Return lifecycle data from `WorkflowRuntimeApi::update_workflow` and the poll method
    - `update_workflow` returns the snapshot after waiting for the (defaulted) requested stage; the poll method resolves the ref to a snapshot or signals not-found. Keep concrete runtime result types distinct from edge DTOs.
    - _Requirements: 1.1, 1.3, 3.1, 3.2_

- [x] 2. Edge: validation, defaulting, and handlers
  - [x] 2.1 Wait-stage and run-targeting helpers in `crates/tokeira-edge/src/workflow_service.rs`
    - **Update-path** wait stage: default absent/`UNSPECIFIED` → `COMPLETED`; reject `ADMITTED` with `INVALID_ARGUMENT` (analog of `errUpdateWorkflowExecutionAsyncAdmittedNotAllowed`, `workflow_handler.go:5277 @ v1.31.0`). **Poll-path:** do NOT default and do NOT reject ADMITTED — UNSPECIFIED/ADMITTED are non-blocking current-stage requests (`update.go:148-238 @ v1.31.0`). Generate a `update_id` (UUID) when the client omits it (update only).
    - **Exact-run targeting:** thread a non-empty well-formed `run_id` from the request execution (update) / `update_ref` (poll) into the `ExecutionRef` instead of hardcoding `run_id: None` (current `workflow_service.rs` drops it); empty `run_id` keeps current-run fallback; malformed `run_id` → `INVALID_ARGUMENT`.
    - _Requirements: 2.1, 2.2, 1.2, 3.3, 6.1, 6.2, 6.3, 6.4_
  - [x] 2.2 Update `WorkflowService::update_workflow_execution`
    - Resolve execution honoring exact `run_id`; apply the update-path defaulting/ADMITTED rejection from 2.1; call the runtime wait path with the defaulted stage; preserve existing protocol behaviour; missing/non-existent run → `NOT_FOUND`. Enforce `first_execution_run_id` per Requirement 6.5: when non-empty and the resolved run's `first_execution_run_id` differs, return `NOT_FOUND` (field equality check against existing `WorkflowState.first_execution_run_id`, matching `updateworkflow/api.go:109-111 @ v1.31.0`).
    - _Requirements: 1.1, 1.2, 2.1, 2.2, 2.3, 2.4, 2.5, 5.1, 6.1, 6.5_
  - [x] 2.3 Update `WorkflowService::poll_workflow_execution_update`
    - Require `update_ref` (else `INVALID_ARGUMENT`, analog of `errUpdateRefNotSet`); resolve the exact `run_id` from the ref; apply NO update-path defaulting/ADMITTED rejection — omitted/UNSPECIFIED/ADMITTED wait stage is non-blocking and returns the current stage, only ACCEPTED/COMPLETED block; unknown `update_id` → `NOT_FOUND` (`pollupdate/api.go @ v1.31.0`); never submit a mutation.
    - _Requirements: 3.1, 3.2, 3.3, 3.5, 3.6, 6.2, 6.3, 6.4_
  - [x] 2.4 Proto projection in `crates/tokeira-edge/src/grpc/translate.rs`
    - Free functions projecting the snapshot to `UpdateRef`, `stage`, and `outcome` on both `UpdateWorkflowExecutionResponse` (ref never null) and `PollWorkflowExecutionUpdateResponse` (echo the ref).
    - _Requirements: 1.1, 1.3, 3.1_
  - [x] 2.5 Verify gRPC error and metric mappings
    - Confirm: update-path ADMITTED / `update_ref`-absent / malformed-`run_id` → `INVALID_ARGUMENT`; poll-path UNSPECIFIED/ADMITTED → non-blocking OK; missing execution, non-existent run, and unknown update → `NOT_FOUND`; server-max-wait expiry on ACCEPTED/COMPLETED → non-error OK with reached stage.
    - _Requirements: 2.2, 2.5, 3.2, 3.3, 3.5, 6.4_

- [x] 3. Required tests
  - [x] 3.1 Property test: Update Ref Stability
    - **Property 1: Update Ref Stability** — initial update and later poll return byte-identical `update_ref`, incl. server-generated `update_id`.
    - _Requirements: 1.1, 1.2, 3.1_
  - [x] 3.2 Property test: Wait Stage Defaulting and ADMITTED Rejection (update path)
    - **Property 2** — on the UPDATE path: UNSPECIFIED/absent → COMPLETED; ADMITTED → `INVALID_ARGUMENT`; other stages unchanged. Include a poll-path counter-case: UNSPECIFIED/ADMITTED on POLL is NOT defaulted and NOT rejected (returns current stage).
    - _Requirements: 2.1, 2.2, 3.5_
  - [x] 3.3 Property test: Wait Returns At-Least-Requested Stage or Times Out Cleanly
    - **Property 3** — for ACCEPTED/COMPLETED waits (update or poll): returns stage ≥ requested (outcome iff COMPLETED) or non-error soft-timeout with actual reached stage; never below requested without timeout. For UNSPECIFIED/ADMITTED on poll: returns immediately with current stage, no blocking.
    - _Requirements: 2.3, 2.4, 2.5, 3.5_
  - [x] 3.4 Property test: Stage Monotonicity
    - **Property 4** — stage non-decreasing on UNSPECIFIED<ADMITTED<ACCEPTED<COMPLETED; rejection observed as COMPLETED+failure.
    - _Requirements: 4.1, 4.3_
  - [x] 3.5 Property test: Outcome Fidelity from History (incl. restart)
    - **Property 5** — outcome equals the committed terminal event payload/failure, set at most once, identical across a simulated runtime restart; admitted-only may be not-found after restart.
    - _Requirements: 4.2, 4.4_
  - [x] 3.6 Property test: Unknown Update Is NOT_FOUND and Poll Is Read-Only
    - **Property 6** — unknown `update_id` → `NOT_FOUND`; no poll submits a mutation.
    - _Requirements: 3.2, 3.6_
  - [x] 3.7 gRPC + non-blocking poll + exact-run tests
    - Missing `update_ref` and unknown update → `NOT_FOUND`/`INVALID_ARGUMENT` as mapped; poll with omitted/UNSPECIFIED/ADMITTED wait policy returns current stage immediately (no block, no error). Exact-run: a non-empty well-formed `run_id` on update and poll targets that run; empty falls back to current; malformed → `INVALID_ARGUMENT`; non-existent run → `NOT_FOUND`.
    - _Requirements: 3.2, 3.3, 3.5, 6.1, 6.2, 6.3, 6.4_

- [x] 4. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3"] },
    { "id": 2, "tasks": ["1.4"] },
    { "id": 3, "tasks": ["2.1", "2.4"] },
    { "id": 4, "tasks": ["2.2", "2.3"] },
    { "id": 5, "tasks": ["2.5"] },
    { "id": 6, "tasks": ["3.1", "3.2", "3.3", "3.4", "3.5", "3.6", "3.7"] },
    { "id": 7, "tasks": ["4"] }
  ]
}
```

## Notes

- No new kernel command and no new history event: the kernel already emits
  `WorkflowExecutionUpdateAccepted/Completed/Rejected`; this spec derives lifecycle metadata from
  them (history-as-authority, AGENTS.md §3) and adds the runtime wait path + edge defaulting.
- The wait path is shared by `UpdateWorkflowExecution` and `PollWorkflowExecutionUpdate`, but the
  two RPCs validate the wait stage differently (verified against v1.31.0): the **update** path
  defaults UNSPECIFIED → COMPLETED and rejects ADMITTED; the **poll** path does neither —
  UNSPECIFIED/ADMITTED are non-blocking current-stage requests, only ACCEPTED/COMPLETED block. Build
  the runtime wait primitive so UNSPECIFIED/ADMITTED return the current stage immediately, so this
  difference is purely the update-path's extra edge validation.
- Exact-run targeting: a non-empty well-formed `run_id` (on the update request execution or the
  poll `update_ref`) targets that exact run; empty `run_id` is the only current-run-fallback case.
  The current handler hardcodes `run_id: None` and must thread the request `run_id` into the
  `ExecutionRef`. v1.31.0 builds the workflow key from `request.WorkflowExecution.RunId`
  (`service/history/api/updateworkflow/api.go:77-81 @ v1.31.0`).
- Stage and outcome are separate axes: stage is monotonic on the 3-value ladder; a rejection is
  `COMPLETED` + failure, not a distinct stage.
- ACCEPTED/COMPLETED survive restart via history replay; admitted-only is transient registry state
  and may be reported not-found after restart — this is the deliberate durability boundary.
- Behaviour anchors (v1.31.0): `common/enums/defaults.go:71-75` (default COMPLETED, update path),
  `service/frontend/workflow_handler.go:5277` (reject ADMITTED, update path),
  `service/frontend/workflow_handler.go` PollWorkflowExecutionUpdate (no defaulting / no ADMITTED
  rejection), `service/history/workflow/update/update.go:148-238` (`WaitLifecycleStage`:
  UNSPECIFIED/ADMITTED return current stage, only ACCEPTED/COMPLETED block),
  `service/history/api/pollupdate/api.go` (unknown → NOT_FOUND),
  `service/history/api/updateworkflow/api.go:77-81` (exact-run key).
