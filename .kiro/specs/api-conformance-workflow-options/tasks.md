# Implementation Plan: Workflow Options API Conformance

## Overview

Implement `UpdateWorkflowExecutionOptions` through an edge handler, runtime submission, kernel transition, and history serialization.

## Tasks

- [ ] 1. Add translation and DTOs
  - [ ] 1.1 Add request/response DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Account for every upstream option field.
    - _Requirements: 1.1-1.5_
  - [ ] 1.2 Add free translation functions in `crates/tokeira-edge/src/grpc/translate.rs`
    - Reject missing/empty changes and malformed option values.
    - _Requirements: 1.3, 1.4, 1.5, 2.1_

- [ ] 2. Add kernel/runtime update path
  - [ ] 2.1 Use or extend kernel update execution options command
    - Keep kernel deterministic and pure.
    - Persist `versioning_override` and any other mutable execution option in run state.
    - _Requirements: 1.1, 1.2, 3.1, 3.2_
  - [ ] 2.2 Add runtime adapter method returning `WorkflowMutationOutcome`
    - Concrete runtime returns `CommitResult`.
    - _Requirements: 1.1, 2.3_
  - [ ] 2.3 Apply updated options to runtime dispatch
    - Subsequent workflow task dispatch uses the updated `versioning_override`.
    - _Requirements: 1.2, 3.4_

- [ ] 3. Wire handler and serializer
  - [ ] 3.1 Implement `WorkflowService::update_workflow_execution_options`
    - Validate run id, resolve execution, submit command, map expected errors.
    - _Requirements: 2.1, 2.2, 2.4_
  - [ ] 3.2 Update `history_serializer.rs`
    - Serialize changed fields including `versioning_override`.
    - _Requirements: 3.1, 3.2, 3.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Options Commit Fidelity
    - _Requirements: 1.1, 3.1, 3.2_
  - [ ] 4.2 Property test: Versioning Override Fidelity
    - _Requirements: 1.2, 3.2, 3.4_
  - [ ] 4.3 Property test: Expected Error Mapping
    - _Requirements: 1.5, 2.1, 2.2, 2.4_
  - [ ] 4.4 Restart/recovery test: Execution Options
    - Verify updated options reload from durable state and affect subsequent dispatch.
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
