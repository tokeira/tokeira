# Implementation Plan: Edge Nexus Task Transport

## Overview

The original worker transport and worker-target dispatch are implemented, but
their JSON task token is not compatible with Temporal v1.31.0. These tasks retain
the working queue/dispatch surface and replace only the public token/correlation
contract, then integrate the opaque runtime correlation used by the edge-owned
HTTP waiter in `edge-nexus-http-dispatch`.

## Tasks

- [x] 1. Existing worker transport baseline
  - [x] 1.1 Queue-isolated `NexusTaskBroker`, poll handler, request translation,
    Respond handlers, worker/external target routing, and timeout tracking.
    - _Requirements: 1.1-1.6, 3.1-3.5, 4.1-4.4, 7.1-7.6, 8.1-8.4, 9.1-9.3, 10.1-10.3, 11.1-11.5_

- [x] 2. Protobuf token and broker correlation correction
  - [x] 2.1 Replace JSON `NexusTaskToken` with the exact three-field prost
    `temporal.server.api.token.v1.NexusTask` wire shape; exact decode errors and
    valid-field checks; PBT for Property 1.
    - _Requirements: 2.1-2.9_
  - [x] 2.2 Add broker `outstanding` correlation keyed by server-authored UUID
    `task_id`; register before queue visibility; atomically consume; PBTs for
    queue isolation and single consumption (Properties 2-3).
    - _Requirements: 1.1-1.9_
  - [x] 2.3 Convert workflow publisher schedule/cancel paths to pass private
    `(run_key, operation_id, scheduled_event_id)` correlation to the broker rather
    than the token.
    - _Requirements: 8.1-8.4, 9.1-9.3_

- [x] 3. Respond validation and routing
  - [x] 3.1 Decode protobuf tokens, enforce stable token namespace through the
    shared two-branch admission helper, and return the exact mismatch/decode/token
    errors without consuming correlation.
    - _Requirements: 2.7-2.8, 5.1, 5.6, 5.9, 6.1, 6.3, 6.6_
  - [x] 3.2 Validate async operation-token length, deprecated failure-details JSON,
    and modern `NexusHandlerFailureInfo` before broker consumption; Property 5.
    - _Requirements: 5.7, 5.10-5.13, 6.4, 6.7-6.10_
  - [x] 3.3 Route a consumed workflow correlation through the existing Nexus
    resolution path; unknown/repeated IDs return the exact `NOT_FOUND` error.
    - _Requirements: 5.12-5.14, 6.9-6.11_
  - [x] 3.4 Add an opaque HTTP waiter ID to runtime correlation and keep the
    waiter plus public worker-outcome type in the edge; valid completion/failure
    resolves exactly one waiter, while timeout/cancellation removes only its
    edge waiter and runtime delivery lease; Properties 3 and 6.
    - _Requirements: 5.15, 6.12_

- [x] 4. Translation and regression coverage
  - [x] 4.1 Preserve all start/cancel request fields through worker poll
    translation; PBT for Property 4.
    - _Requirements: 4.1-4.4_
  - [x] 4.2 Exact-string tests for every task-token/Respond error and proof that
    invalid requests leave correlation outstanding.
    - _Requirements: 2.7-2.8, 5.6-5.13, 6.3-6.10_
  - [x] 4.3 End-to-end workflow and HTTP publish→poll→respond tests.
    - _Requirements: 3.1-3.3, 5.14-5.15, 6.11-6.12, 8.1-8.4_

- [x] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run focused `tokeira-runtime` and `tokeira-edge` Nexus tests.
  - Run the token-validation leaves in `TestNexusAPIValidationTestSuite`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["2.1", "2.2"] },
    { "id": 1, "tasks": ["2.3", "3.1", "3.2"] },
    { "id": 2, "tasks": ["3.3", "3.4", "4.1"] },
    { "id": 3, "tasks": ["4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```

## Notes

- The task token is protobuf, not JSON, and contains no workflow/run identity.
- Validation precedes correlation consumption.
- Broker state is a disposable delivery/waiter mechanism; workflow history remains
  authoritative.
- No kernel change is part of this correction.
