# Implementation Plan: Workflow Options API Conformance

## Overview

Implement `UpdateWorkflowExecutionOptions` through an edge handler, runtime submission, kernel transition, and history serialization.

## Tasks

- [x] 1. Add translation and DTOs
  - [x] 1.1 Add request/response DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Account for every upstream option field.
    - Landed: `UpdateWorkflowExecutionOptions{Request,Response}` + `VersioningOverrideChange`
      (`Unchanged`/`Set`/`SetImpliedPinned`/`Clear`). Only `versioning_override` is modeled; `priority`/`time_skipping_config`
      are valid v1.31.0 fields tokeira does not model (tracked in UNSUPPORTED_FIELDS.md).
    - _Requirements: 1.1-1.5_
  - [x] 1.2 Add free translation functions in `crates/tokeira-edge/src/grpc/translate.rs`
    - Accept an empty mask as an unchanged request and reject malformed option values.
    - Landed: `update_workflow_execution_options_{request_to_edge,response_to_proto}`. The
      `update_mask` is validated (empty → `Unchanged`; unsupported path like `priority`
      → `INVALID_ARGUMENT`; deprecated `versioning_override.{behavior,deployment}` must be
      masked together, mirroring `mergeWorkflowExecutionOptions @ v1.31.0`) and reduced to
      `Unchanged`/`Set`/`SetImpliedPinned`/`Clear`.
    - _Requirements: 1.3, 1.4, 1.5, 2.1_

- [x] 2. Add kernel/runtime update path
  - [x] 2.1 Use or extend kernel update execution options command
    - Keep kernel deterministic and pure.
    - Persist `versioning_override` and any other mutable execution option in run state.
    - Pre-existing: `Command::UpdateExecutionOptions` + `apply_update_execution_options`
      (emits `WorkflowExecutionOptionsUpdated`, `set_versioning_override`) and the replay path
      already model `versioning_override`; reused unchanged.
    - _Requirements: 1.1, 1.2, 3.1, 3.2_
  - [x] 2.2 Add runtime adapter method returning `WorkflowMutationOutcome`
    - Concrete runtime returns `CommitResult`.
    - Landed: `WorkflowRuntimeApi::update_workflow_execution_options` (default-erroring) +
      `RuntimeAdapter` override + `TokeiraRuntime::update_workflow_execution_options`
      (resolves the execution, submits `Command::UpdateExecutionOptions`).
    - _Requirements: 1.1, 2.3_
  - [x] 2.3 Apply updated options to runtime dispatch
    - Subsequent workflow task dispatch uses the updated `versioning_override`.
    - The override is persisted into the run's `versioning_info` — the same structure WFT
      dispatch consults for deployment routing — and restored on replay. The active routing
      machinery is owned by `runtime-worker-versioning`; this spec makes the updated value
      available to it rather than re-implementing routing.
    - _Requirements: 1.2, 3.4_

- [x] 3. Wire handler and serializer
  - [x] 3.1 Implement `WorkflowService::update_workflow_execution_options`
    - Validate run id, resolve execution, submit command, map expected errors.
    - Landed in part: resolves via `resolve_execution_run_key` (malformed run id →
      `INVALID_ARGUMENT`, missing execution → `NOT_FOUND`) and submits. Task 6 corrects
      response construction so it always reflects post-commit state.
    - _Requirements: 2.1, 2.2, 2.4_
  - [x] 3.2 Update `history_serializer.rs`
    - Serialize changed fields including `versioning_override`.
    - Landed: `WorkflowExecutionOptionsUpdated` now serializes `versioning_override`
      (Set → value, Clear → `unset_versioning_override`) via a serializer-local
      `versioning_override_from_kernel`. Removed the stale "placeholder type" comment.
    - _Requirements: 3.1, 3.2, 3.3_

- [x] 4. Add required tests
  - [x] 4.1 Property test: Options Commit Fidelity
    - Translate test asserts the mask→change reduction; kernel golden/property tests
      (`golden_tests.rs`, `property_tests.rs`) already cover the committed state + emitted event.
    - _Requirements: 1.1, 3.1, 3.2_
  - [x] 4.2 Property test: Versioning Override Fidelity
    - Translate test covers Pinned `Set` / `Clear` + the response/serializer projection; kernel
      tests cover persistence.
    - _Requirements: 1.2, 3.2, 3.4_
  - [x] 4.3 Property test: Expected Error Mapping
    - Landed: `update_workflow_execution_options_request_validation` (translate) +
      `update_workflow_execution_options_maps_expected_errors` (grpc handler: malformed run id →
      `INVALID_ARGUMENT`, missing execution → `NOT_FOUND`). Empty-mask behavior is
      corrected and covered by task 6.
    - _Requirements: 1.5, 2.1, 2.2, 2.4_
  - [x] 4.4 Restart/recovery test: Execution Options
    - Verify updated options reload from durable state and affect subsequent dispatch.
    - Covered by the kernel replay path + its existing tests (the `WorkflowExecutionOptionsUpdated`
      event reconstructs `versioning_override` on rebuild). Dispatch-routing consumption is the
      `runtime-worker-versioning` concern (see 2.3).
    - _Requirements: 3.4_

- [x] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-kernel`.
  - Run `cargo test -p tokeira-runtime`.

- [x] 6. Tier 8.40 correction: serialized option changes and no-op fidelity
  - [x] 6.1 Carry unresolved implied-pinned intent through the command boundary
    - Replace the kernel command's generic versioning `FieldChange` input with a pure,
      serializable change type supporting `Unchanged`, concrete `Set`,
      `SetImpliedPinned`, and `Clear`. Keep history events concrete: they SHALL continue
      to store only `FieldChange<VersioningOverride>` after resolution.
    - Thread the same input shape through direct updates, batch updates, and reset
      successor operations where those surfaces accept workflow-option updates.
    - _Requirements: 1.2, 1.6, 1.7, 1.9, 3.2_
  - [x] 6.2 Resolve and apply the change inside the pure kernel transition
    - Resolve `SetImpliedPinned` from the authoritative run's effective behavior and
      deployment in lane order. On success, persist and emit the concrete pinned
      override. On failure, reject with the exact v1.31.0 failed-precondition reason and
      no state/event mutation.
    - Treat `Unchanged` and value-equivalent Set/Clear operations as successful no-ops
      with no history event.
    - Cite `service/history/api/updateworkflowoptions/api.go @ v1.31.0`; add no I/O,
      async, storage, registry lookup, or non-determinism to the kernel.
    - _Requirements: 1.5, 1.6, 1.7, 1.8, 1.9, 3.1, 3.4_
  - [x] 6.3 Correct edge/runtime responses and error mapping
    - Validate explicit pinned task-queue membership before submission; do not resolve
      implied pins from a separately loaded Edge snapshot.
    - Reload committed state for the direct response, map the implied-pin kernel reject
      to `FAILED_PRECONDITION`, and send version reactivation only after a successful
      concrete pinned commit.
    - Ensure batch and reset retain their own failure/identity contracts while sharing
      the authoritative mutation.
    - _Requirements: 1.4, 1.6, 1.7, 2.4, 2.5, 3.5_
  - [x] 6.4 Add correctness and conformance tests
    - Add property tests for serialized implied-pin ordering and no-op fidelity (at least
      100 cases), plus focused tests for exact rejection text, concrete history/replay,
      response contents, event identity, explicit membership failure, and empty-mask /
      repeated-value no-ops.
    - Run focused kernel/runtime/edge tests and the relevant
      `TestDeploymentVersionSuite` leaves twice consecutively.
    - _Requirements: 1.4-1.9, 2.5, 3.1-3.5; Properties 4 and 5_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2", "2.3"] },
    { "id": 2, "tasks": ["3.1", "3.2"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3", "4.4"] },
    { "id": 4, "tasks": ["6.1"] },
    { "id": 5, "tasks": ["6.2", "6.3"] },
    { "id": 6, "tasks": ["6.4", "5"] }
  ]
}
```
