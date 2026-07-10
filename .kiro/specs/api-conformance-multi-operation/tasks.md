# Implementation Plan: Multi-Operation API Conformance (Update-with-Start)

## Overview

Implement `ExecuteMultiOperation` for the `[StartWorkflow, UpdateWorkflow]` composition
(Update-with-Start). Edge translates + validates before mutation; the runtime orchestrates path
selection (mirroring `signal_with_start_workflow`); the fresh-start path folds start + update admission
into one kernel transition via a new `Command::StartAndUpdate` (mirroring `Command::SignalWithStart`);
attach paths reuse the existing update machinery. Failures return the structured `MultiOperationExecution`
error. There is no signal variant.

## Tasks

- [ ] 1. Translation and validation (edge, no mutation)
  - [x] 1.1 Add Update-with-Start edge DTOs and `multi_operation_request_to_edge`
    - Accept only `[Start, Update]`; reject any other shape with `INVALID_ARGUMENT`
      ("Operations have to be exactly [Start, Update].").
    - Enforce start restrictions (cron_schedule, request_eager_execution, workflow_start_delay) and
      update restrictions (first_execution_run_id, workflow_execution.run_id), each with its own message.
    - Enforce per-op namespace match and start/update workflow-id consistency.
    - Reuse the existing start/update field-translation helpers so standalone validation parity is
      automatic. Exhaustively match the `Operation` oneof (future arms rejected until mapped).
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_
  - [x] 1.2 ~~Pin the namespace enablement default as a constant~~ OBSOLETE — v1.31.0 has no
    enablement gate (removed upstream in #8818; `errMultiOperationAPINotAllowed` is dead code at
    the tag). Nothing to build; Req 1.7 rewritten accordingly.
    - _Requirements: 1.7_

- [x] 2. Kernel: atomic start+update fold (raised addition)
  - [x] 2.1 Add `Command::StartAndUpdate(StartAndUpdateRequest)` and `apply_start_and_update`
    - Mirror `Command::SignalWithStart` / `apply_signal_with_start`: one transition emitting
      `WorkflowExecutionStarted`, admitting the update (`admitted_updates` +
      `WorkflowExecutionUpdateAdmitted`), and scheduling the first workflow task.
    - Keep the kernel pure (no I/O/async). Document the WHY: atomicity requires one command = one
      transition = one commit; cite the SignalWithStart precedent.
    - _Requirements: 2.1, 2.6_

- [x] 3. Runtime: composition and path selection
  - [x] 3.1 Add `TokeiraRuntime::execute_multi_operation`
    - Resolve the start leg via the existing `resolve_conflict`.
    - Fresh-run resolutions → submit `Command::StartAndUpdate`, then drive the update wait-stage on the
      new run.
    - `UseExisting` / dedup / update-id-already-in-registry → attach via `update_workflow` on the
      existing run; start response `started = false`.
    - Return a typed result carrying both legs' outcomes plus `started`/`status`, or a typed
      multi-operation error identifying the failing leg.
    - _Requirements: 2.1, 2.2, 2.3, 3.1, 3.2, 3.3_
  - [x] 3.2 Already-completed update no-op path
    - When the target exists and the requested update id already completed, return the stored outcome
      with `started = false` and current `status`, performing no mutation.
    - _Requirements: 2.4, 3.2_
  - [x] 3.3 Update wait-stage reuse
    - Drive the update leg through the existing `update_workflow` / `UpdateWaitPolicy` lifecycle;
      honour Admitted/Accepted/Completed identically to standalone update.
    - _Requirements: 5.1, 5.2_

- [x] 4. Edge handler, response, and structured failure
  - [x] 4.1 Implement the `execute_multi_operation` handler
    - Translate → validate → call runtime adapter → serialize.
    - Add the `runtime_adapter` method bridging edge → `execute_multi_operation` (mirror
      `signal_with_start_workflow`).
    - _Requirements: 1.1, 2.1, 3.1_
  - [x] 4.2 Ordered response serialization (`multi_operation_response_to_proto`)
    - Build `[start_workflow, update_workflow]` in order with correct `started`/`status`/outcome.
    - _Requirements: 3.1, 3.2, 3.3_
  - [x] 4.3 Structured failure serialization (`multi_operation_error_to_status`)
    - Build `MultiOperationExecutionFailure` with one `OperationStatus` per op in order; failing op
      carries its own error; sibling carries `Aborted` + `MultiOperationExecutionAborted`; top-level
      code = first failing op; message "Update-with-Start could not be executed."
    - Verify/extend the edge gRPC status path so the error **detail** is attached (google.rpc.Status
      details); the SDK/corpus unpacks per-op errors from it.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [ ] 5. Required tests
  - [x] 5.1 Kernel golden test for `apply_start_and_update`
    - One transition: `WorkflowExecutionStarted` + `WorkflowExecutionUpdateAdmitted` + scheduled WFT
      (mirror the `apply_signal_with_start` golden test).
    - _Requirements: 2.1_
  - [ ] 5.2 Property test: Validate Before Mutate (Property 1)
    - Mock runtime asserts no mutation method is called for each invalid request class in Req 1.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.5_
  - [ ] 5.3 Property test: Ordered Response + path outcomes (Property 2)
    - Over `InMemoryStore`: fresh-start, dedup-attach, USE_EXISTING-attach, already-completed; assert
      response shape, order, and `started`/`status`.
    - _Requirements: 3.1, 3.2, 3.3, 2.2, 2.3, 2.4_
  - [ ] 5.4 Property test: Atomic, No Partial Commit (Property 3)
    - Fresh-start produces exactly one transition; already-completed produces zero; injected failure
      leaves no run-without-update or update-without-run.
    - _Requirements: 2.1, 2.6_
  - [ ] 5.5 Property test: Structured Failure Fidelity (Property 4)
    - Start-conflict and update-rejected cases: per-op statuses, `Aborted` sibling, first-failing
      top-level code, message.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [ ] 6. Closing-workflow retry (deferred follow-up, explicit)
  - [ ] 6.1 Classify-skip the closing-workflow-retry corpus sub-case(s)
    - Register the specific `TestUpdateWithStartSuite` leaf/leaves in the conformance skip registry with
      a cited reason (depends on primary paths; deliberate deferral, not omission).
    - _Requirements: 6.2_
  - [ ] 6.2 Implement retry-once + `NotFound → Aborted` conversion
    - Pin both dynamic-config gates as v1.31.0-default constants; implement the retry-once path and the
      second-operation error conversion; then flip the skipped leaf/leaves to required pass.
    - _Requirements: 6.1, 6.2_

- [ ] 7. Compatibility + docs
  - [x] 7.1 Reclassify `WorkflowService.ExecuteMultiOperation` as supported
    - Update `FEATURE_MATRIX` / `MULTI_OPERATION_RPCS` in `crates/tokeira-compatibility/src/matrix.rs`
      with evidence.
    - Record the composed-start kernel command in `docs/readiness/command-surface.md`; move the entry in
      `docs/readiness/edge-unimplemented.md`.
    - _Requirements: 2.1, 4.1_

- [ ] 8. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint` and `cargo test-lint`.
  - Run `cargo test -p tokeira-kernel -p tokeira-runtime -p tokeira-edge`.
  - Tier-2 (operator-invoked, not `cargo test`): drive `TestUpdateWithStartSuite` /
    `TestUpdateWorkflowSdkSuite` and the `InternalTaskQueue/multiOp` leaf against a running `tokeirad`;
    confirm in-scope sub-cases pass and only the cited closing-workflow-retry leaf is skipped.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5.1", "5.2", "5.3", "5.4", "5.5"] },
    { "id": 5, "tasks": ["6.1"] },
    { "id": 6, "tasks": ["7.1"] },
    { "id": 7, "tasks": ["8"] },
    { "id": 8, "tasks": ["6.2"] }
  ]
}
```

> Wave 8 (`6.2`, full closing-workflow retry) is intentionally last and may land in a follow-up: the
> primary paths (waves 0–4) and conformance reclassification (wave 6) deliver the behavioural claim;
> `6.1` keeps the deferral honest and cited in the interim.
