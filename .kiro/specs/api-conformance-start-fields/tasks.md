# Implementation Plan: Start Field API Conformance

## Overview

Implement full external start field handling across `StartWorkflowExecution` and `SignalWithStartWorkflowExecution`, including durable delayed start, callbacks, metadata, links, versioning override, cron, and current-run conflict policies.

## Tasks

- [ ] 1. Audit and model start fields
  - [ ] 1.1 Add a field-policy table test in `crates/tokeira-edge/src/translate/to_internal.rs`
    - Cover every field listed under `StartWorkflowExecutionRequest` in `UNSUPPORTED_FIELDS.md`.
    - _Requirements: 1.1-1.10_
  - [ ] 1.2 Add validation/error mappings for invalid internal-only fields
    - Verify `errors.rs`, `grpc/errors.rs`, and `grpc_error_code` map malformed/internal-only start input to `INVALID_ARGUMENT`.
    - _Requirements: 1.9, 1.10_

- [ ] 2. Translate all external start fields
  - [ ] 2.1 Update `start_request` translation
    - Map reuse/conflict enum values.
    - Preserve `request_eager_execution`.
    - Translate delay, callbacks, metadata, links, versioning override, and client cron into internal DTOs.
    - Reject only invalid internal fields or malformed values.
    - _Requirements: 1.1-1.10, 3.3_
  - [ ] 2.2 Update `signal_with_start_request` translation
    - Reuse the same start-field helper as normal start.
    - Ensure validation happens before start/signal mutation.
    - _Requirements: 2.1, 2.3, 2.4_

- [ ] 3. Add durable start model fields
  - [ ] 3.1 Extend `StartRequest`, `WorkflowExecutionStarted`, and run state
    - Add conflict policies, delayed-start metadata, callback registrations, user metadata, links, versioning override, and cron policy.
    - Keep the kernel pure.
    - _Requirements: 3.1, 3.2_
  - [ ] 3.2 Update history serializer and describe projection for newly supported fields
    - _Requirements: 3.1, 3.3_
  - [ ] 3.3 Add storage/current-execution conflict handling
    - Enforce reuse/conflict policies in the current-execution index for memory and DSQL-backed state.
    - _Requirements: 1.1, 3.4_
  - [ ] 3.4 Add delayed-start and cron runtime scheduling
    - Store durable timer entries and schedule the first WFT or next cron run only when the timer fires.
    - _Requirements: 1.3, 1.8, 3.4_
  - [ ] 3.5 Add terminal callback dispatch
    - Persist callback state in the kernel and dispatch callbacks exactly once from runtime-derived effects after terminal transitions.
    - _Requirements: 1.4, 3.4_
  - [ ] 3.6 Apply versioning override to dispatch routing
    - Persist override in run state and use it when choosing WFT routing/versioning metadata.
    - _Requirements: 1.7, 3.4_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: No Silent Drop
    - _Requirements: 1.3-1.9, 2.3_
  - [ ] 4.2 Property test: Start/SignalWithStart Parity
    - _Requirements: 2.1, 3.1_
  - [ ] 4.3 Property test: Durable Start Metadata
    - _Requirements: 3.1, 3.2_
  - [ ] 4.4 Restart/recovery tests
    - Verify delayed starts, cron state, terminal callbacks, and versioning override routing survive repository reload.
    - _Requirements: 3.4_

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
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["3.4", "3.5", "3.6"] },
    { "id": 4, "tasks": ["4.1", "4.2", "4.3", "4.4"] },
    { "id": 5, "tasks": ["5"] }
  ]
}
```
