# Implementation Plan: Update Lifecycle API Conformance

## Overview

Add update reference and stage metadata to update responses while preserving the existing worker protocol and runtime update registry behavior.

## Tasks

- [ ] 1. Add lifecycle DTOs
  - [ ] 1.1 Extend update response DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Add update reference and stage fields.
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 1.2 Update proto projection in `crates/tokeira-edge/src/grpc/translate.rs`
    - Populate `update_ref` and `stage` from DTOs.
    - _Requirements: 1.1, 1.2, 2.1_

- [ ] 2. Extend runtime update registry
  - [ ] 2.1 Add lifecycle snapshot fields in `crates/tokeira-runtime/src/update.rs`
    - Track admitted, accepted, completed, rejected, timeout, and cleanup states.
    - _Requirements: 1.4, 3.2, 3.3_
  - [ ] 2.2 Return lifecycle data from `WorkflowRuntimeApi::update_workflow` and poll methods
    - Keep concrete runtime result types distinct from edge response DTOs.
    - _Requirements: 1.1, 1.2, 2.1_

- [ ] 3. Wire edge handlers and errors
  - [ ] 3.1 Update `WorkflowService::update_workflow_execution`
    - Preserve existing protocol behavior and add metadata.
    - _Requirements: 1.1, 1.2, 3.1_
  - [ ] 3.2 Update `WorkflowService::poll_workflow_execution_update`
    - Validate run id and map unknown update ids.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_
  - [ ] 3.3 Verify gRPC errors and metrics
    - _Requirements: 2.2, 2.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Update Ref Stability
    - _Requirements: 1.1, 2.1_
  - [ ] 4.2 Property test: Stage Monotonicity
    - _Requirements: 1.2, 1.3, 3.3_
  - [ ] 4.3 Property test: Poll Is Read-Only
    - _Requirements: 2.2, 2.4_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "2.2"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
