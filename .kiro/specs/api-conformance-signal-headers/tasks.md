# Implementation Plan: Signal Header API Conformance

## Overview

Thread signal headers and links through the signal path without silent drops.

## Tasks

- [ ] 1. Add signal DTO fields
  - [ ] 1.1 Update edge DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Add `header` and `links` fields where missing.
    - _Requirements: 1.1, 2.1_
  - [ ] 1.2 Update gRPC translation in `crates/tokeira-edge/src/grpc/translate.rs`
    - Preserve `SignalWorkflowExecutionRequest.header`.
    - Preserve `SignalWorkflowExecutionRequest.links`.
    - _Requirements: 1.1, 1.4, 2.2_

- [ ] 2. Thread headers into kernel/history
  - [ ] 2.1 Update `to_internal::signal_request`
    - Map header and links into `SignalRequest`.
    - _Requirements: 1.1, 2.2, 2.3_
  - [ ] 2.2 Update kernel command/event state
    - Persist signal header and links deterministically.
    - _Requirements: 1.2, 3.1_
  - [ ] 2.3 Update history serializer
    - Emit signal header and any supported links.
    - _Requirements: 1.3, 2.1_

- [ ] 3. Preserve handler behavior
  - [ ] 3.1 Validate run id and not-found mapping in `WorkflowService::signal_workflow_execution`
    - _Requirements: 3.2, 3.3_
  - [ ] 3.2 Apply the same signal field policy to `SignalWithStartWorkflowExecution`
    - _Requirements: 3.4_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Header Round Trip
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 4.2 Property test: Link Round Trip
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 4.3 Property test: Existing Signal Behavior
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-kernel`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3", "3.1", "3.2"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
