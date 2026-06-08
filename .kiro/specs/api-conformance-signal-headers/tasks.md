# Implementation Plan: Signal Header API Conformance

## Overview

Thread signal headers and links through the signal path without silent drops.

## Tasks

- [x] 1. Add signal DTO fields
  - [x] 1.1 Update edge DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Add `header` and `links` fields where missing.
    - _Requirements: 1.1, 2.1_
  - [x] 1.2 Update gRPC translation in `crates/tokeira-edge/src/grpc/translate.rs`
    - Preserve `SignalWorkflowExecutionRequest.header`.
    - Preserve `SignalWorkflowExecutionRequest.links`.
    - Reject any supplied `Link` whose `variant` oneof is absent with `INVALID_ARGUMENT`.
    - _Requirements: 1.1, 2.1, 2.4_

- [x] 2. Thread headers into kernel/history
  - [x] 2.1 Update `to_internal::signal_request`
    - Map header and links into `SignalRequest`.
    - _Requirements: 1.1, 2.1, 2.2_
  - [x] 2.2 Update kernel command/event state
    - Persist signal header and links deterministically. Header is a signaled-event attributes field; links are separate event data lifted to top-level history links by the serializer.
    - _Requirements: 1.2, 3.1_
  - [x] 2.3 Update history serializer
    - Emit signal header in `WorkflowExecutionSignaledEventAttributes.header`.
    - Emit signal links in top-level `HistoryEvent.links` for the signaled event.
    - _Requirements: 1.3, 2.2, 2.3_

- [x] 3. Preserve handler behavior
  - [x] 3.1 Validate run id and not-found mapping in `WorkflowService::signal_workflow_execution`
    - _Requirements: 3.2, 3.3_
  - [x] 3.2 Apply the same signal field policy to `SignalWithStartWorkflowExecution`
    - On new-run SignalWithStart, apply request `header` and `links` to both `WorkflowExecutionStarted` and `WorkflowExecutionSignaled`.
    - On existing-run SignalWithStart, apply request `header` and `links` to the signaled event.
    - _Requirements: 3.4, 3.5, 3.6_

- [x] 4. Add required tests
  - [x] 4.1 Property test: Header Round Trip
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 4.2 Property test: Link Round Trip
    - Verify links round-trip through top-level `HistoryEvent.links` on the signaled event.
    - Verify absent `Link.variant` is rejected as `INVALID_ARGUMENT`.
    - _Requirements: 2.1, 2.2, 2.3, 2.4_
  - [x] 4.3 Property test: Existing Signal Behavior
    - Verify SignalWithStart new-run and existing-run header/link propagation.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 5. Checkpoint
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
