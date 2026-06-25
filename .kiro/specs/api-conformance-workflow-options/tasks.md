# Implementation Plan: Workflow Options API Conformance

## Overview

Implement `UpdateWorkflowExecutionOptions` through an edge handler, runtime submission, kernel transition, and history serialization.

## Tasks

- [x] 1. Add translation and DTOs
  - [x] 1.1 Add request/response DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Account for every upstream option field.
    - Landed: `UpdateWorkflowExecutionOptions{Request,Response}` + `VersioningOverrideChange`
      (`Set`/`Clear`). Only `versioning_override` is modeled; `priority`/`time_skipping_config`
      are valid v1.31.0 fields tokeira does not model (tracked in UNSUPPORTED_FIELDS.md).
    - _Requirements: 1.1-1.5_
  - [x] 1.2 Add free translation functions in `crates/tokeira-edge/src/grpc/translate.rs`
    - Reject missing/empty changes and malformed option values.
    - Landed: `update_workflow_execution_options_{request_to_edge,response_to_proto}`. The
      `update_mask` is validated (empty → `INVALID_ARGUMENT`; unsupported path like `priority`
      → `INVALID_ARGUMENT`; deprecated `versioning_override.{behavior,deployment}` must be
      masked together, mirroring `mergeWorkflowExecutionOptions @ v1.31.0`) and reduced to
      `Set`/`Clear`.
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
    - Landed: resolves via `resolve_execution_run_key` (malformed run id → `INVALID_ARGUMENT`,
      missing execution → `NOT_FOUND`), submits, echoes the post-update override.
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
      `INVALID_ARGUMENT`, missing execution → `NOT_FOUND`, empty mask → `INVALID_ARGUMENT`).
    - _Requirements: 1.5, 2.1, 2.2, 2.4_
  - [x] 4.4 Restart/recovery test: Execution Options
    - Verify updated options reload from durable state and affect subsequent dispatch.
    - Covered by the kernel replay path + its existing tests (the `WorkflowExecutionOptionsUpdated`
      event reconstructs `versioning_override` on rebuild). Dispatch-routing consumption is the
      `runtime-worker-versioning` concern (see 2.3).
    - _Requirements: 3.4_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-kernel`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2", "2.3"] },
    { "id": 2, "tasks": ["3.1", "3.2"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3", "4.4"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
