# Implementation Plan: Activity Events API Conformance

## Overview

Complete activity poll response and heartbeat detail conformance by storing heartbeat progress, threading timing fields, and populating activity history event link fields.

## Tasks

- [ ] 1. Preserve heartbeat details
  - [ ] 1.1 Update token heartbeat translation in `crates/tokeira-edge/src/grpc/translate.rs`
    - Preserve `RecordActivityTaskHeartbeatRequest.details`.
    - _Requirements: 2.1, 2.3_
  - [ ] 1.2 Extend runtime activity tracking to store latest heartbeat details
    - Keep details keyed by run/activity/attempt.
    - Clear or supersede details according to retry/terminal lifecycle.
    - _Requirements: 2.1, 2.2, 2.4_

- [ ] 2. Populate poll response timing and details
  - [ ] 2.1 Extend `StartedActivityTask` and broker/dispatch DTOs
    - Carry scheduled time, current attempt scheduled time, started time, and heartbeat details.
    - _Requirements: 1.2, 1.3, 1.4, 1.5_
  - [ ] 2.2 Update `poll_activity_task_queue` response projection
    - Leave timestamp fields default when unknown.
    - _Requirements: 1.1, 1.6_

- [ ] 3. Populate activity history event linkage
  - [ ] 3.1 Ensure kernel state retains scheduled/start event ids needed by serializer
    - Keep kernel deterministic and pure.
    - _Requirements: 3.1, 3.3_
  - [ ] 3.2 Update `history_serializer.rs` activity event attributes
    - Populate event ids when known; leave default for legacy missing state.
    - _Requirements: 3.1, 3.2_

- [ ] 4. Add required tests
  - [ ] 4.1 Property test: Heartbeat Round Trip
    - _Requirements: 1.2, 2.1, 2.2_
  - [ ] 4.2 Property test: Timing Authorship
    - _Requirements: 1.3, 1.4, 1.5, 1.6_
  - [ ] 4.3 Property test: Event Link Fidelity
    - _Requirements: 3.1, 3.2_

- [ ] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.
  - Run `cargo test -p tokeira-storage`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2", "3.2"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5"] }
  ]
}
```
