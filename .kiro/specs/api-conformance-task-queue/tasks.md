# Implementation Plan: Task Queue API Conformance

## Overview

Implement `ListTaskQueuePartitions` and complete `DescribeTaskQueue` using runtime queue diagnostics.

## Tasks

- [ ] 1. Add partition response support
  - [ ] 1.1 Add translation helpers in `crates/tokeira-edge/src/grpc/translate.rs`
    - Produce one normal partition for the current queue model.
    - _Requirements: 1.1, 1.3_
  - [ ] 1.2 Implement `list_task_queue_partitions`
    - Validate namespace/task queue before runtime lookup.
    - _Requirements: 1.1, 1.2, 3.1, 3.2_

- [ ] 2. Complete describe diagnostics
  - [ ] 2.1 Add read-only runtime queue diagnostics
    - Broker pollers, backlog status, worker reachability, and build-id data.
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 2.2 Update describe response projection
    - Preserve deprecated field behavior.
    - _Requirements: 2.4, 2.5_

- [ ] 3. Add required tests
  - [ ] 3.1 Property test: Single-Partition Compatibility
    - _Requirements: 1.1, 1.3_
  - [ ] 3.2 Property test: Describe Reflects Runtime State
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 3.3 Property test: Validation Before Lookup
    - _Requirements: 3.1, 3.2_

- [ ] 4. Checkpoint
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
    { "id": 3, "tasks": ["4"] }
  ]
}
```
