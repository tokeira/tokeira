# Implementation Plan: Workflow Describe API Conformance

## Overview

Complete `DescribeWorkflowExecution` by adding a consistent description snapshot, translating pending runtime state into upstream proto fields, and covering expected error paths.

## Tasks

- [ ] 1. Add describe snapshot DTOs
  - [ ] 1.1 Extend edge/internal describe DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Add execution config and pending entity DTOs.
    - Keep callback entries empty until callback state exists.
    - _Requirements: 1.1, 1.2, 1.6_
  - [ ] 1.2 Add response projection in `crates/tokeira-edge/src/translate/from_internal.rs`
    - Populate `execution_config`, `pending_activities`, `pending_children`, `pending_workflow_task`, and `pending_nexus_operations`.
    - Leave unknown event-id fields default rather than inventing placeholders.
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.7, 2.3_

- [ ] 2. Add runtime/projection snapshot read path
  - [ ] 2.1 Add a read-only snapshot method to `WorkflowRuntimeApi` if existing visibility APIs are insufficient
    - Concrete runtime returns a snapshot from `RunRepository::load_run`.
    - Edge adapter distinguishes this read from mutation methods that return `WorkflowMutationOutcome`.
    - _Requirements: 2.1, 2.2_
  - [ ] 2.2 Preserve kernel purity
    - Add only serializable state fields to `tokeira-kernel` if missing event linkage is needed.
    - Do not add I/O, async, metrics, or storage to the kernel.
    - _Requirements: 1.3, 1.4, 1.5, 1.7_

- [ ] 3. Wire handler and errors
  - [ ] 3.1 Update `WorkflowService::describe_workflow_execution`
    - Validate non-empty `run_id` before resolution.
    - Return `WorkflowNotFound` for unresolved executions.
    - _Requirements: 1.8, 1.9, 1.10_
  - [ ] 3.2 Verify gRPC mapping and metrics
    - Confirm `grpc/errors.rs` maps expected errors.
    - Confirm `grpc_error_code` emits `invalid_argument` and `not_found`.
    - _Requirements: 3.1, 3.2, 3.3_

- [ ] 4. Add required tests
  - [ ] 4.1 Add unit tests for every response section
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 1.7_
  - [ ] 4.2 Add property tests for snapshot consistency and pending activity fidelity
    - **Property 1:** Single Snapshot Consistency.
    - **Property 2:** Pending Activity Fidelity.
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 4.3 Add gRPC tests for malformed run id and not found
    - **Property 3:** Expected Error Mapping.
    - _Requirements: 1.8, 1.9, 3.1, 3.2_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "2.2", "3.1"] },
    { "id": 2, "tasks": ["3.2", "4.1", "4.2", "4.3"] },
    { "id": 3, "tasks": ["5"] }
  ]
}
```
