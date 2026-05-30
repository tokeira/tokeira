# Implementation Plan: Workflow Task Completion API Conformance

## Overview

Complete `RespondWorkflowTaskCompleted` field handling and return-new-workflow-task semantics without moving workflow semantics out of the kernel/runtime path.

## Tasks

- [ ] 1. Preserve completion metadata
  - [ ] 1.1 Update `crates/tokeira-edge/src/grpc/translate.rs`
    - Preserve `sdk_metadata`, `worker_version_stamp`, deployment, metering, sticky, and versioning inputs.
    - Do not use `TryFrom`; follow existing free-function translation.
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2_
  - [ ] 1.2 Update kernel request/event models
    - Add supported metadata to `WorkflowTaskCompletedRequest` and `HistoryEventKind::WorkflowTaskCompleted`.
    - _Requirements: 1.1, 1.2_

- [ ] 2. Implement sticky, metering, deployment, and versioning fields
  - [ ] 2.1 Add validation and translation in `WorkflowService::respond_workflow_task_completed`
    - Validate sticky/versioning/deployment fields and pass accepted values to runtime/kernel DTOs.
    - _Requirements: 2.1, 2.2, 2.3, 4.3_
  - [ ] 2.2 Persist sticky/versioning/deployment state
    - Update kernel state and history metadata without adding I/O to the kernel.
    - _Requirements: 1.2, 2.1, 2.2_
  - [ ] 2.3 Apply sticky/versioning routing in runtime dispatch
    - Subsequent WFT dispatch honors accepted sticky and versioning metadata.
    - _Requirements: 2.1, 2.2_
  - [ ] 2.4 Verify error and metric mapping
    - Update `errors.rs`, `grpc/errors.rs`, and `grpc_error_code` if new variants are required.
    - _Requirements: 4.1, 4.2, 4.3_

- [ ] 3. Implement return-new-WFT semantics
  - [ ] 3.1 Extend runtime completion response path
    - Return an immediately available WFT only after durable scheduling/start.
    - Preserve existing inline query and eager activity behavior.
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Metadata Fidelity
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 4.2 Property test: Sticky and Versioning Fidelity
    - _Requirements: 2.1, 2.2, 2.3, 4.3_
  - [ ] 4.3 Property test: Return-New-WFT Safety
    - _Requirements: 3.2, 3.3_
  - [ ] 4.4 Restart/recovery test: Completion Routing Metadata
    - Verify sticky/versioning/deployment metadata reloads and affects subsequent dispatch.
    - _Requirements: 2.1, 2.2_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.
  - Run `cargo test -p tokeira-kernel`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3", "2.4", "3.1"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3", "4.4"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
