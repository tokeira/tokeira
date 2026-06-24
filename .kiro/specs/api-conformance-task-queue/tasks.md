# Implementation Plan: Task Queue API Conformance

## Overview

Implement `ListTaskQueuePartitions` and complete `DescribeTaskQueue` using runtime queue diagnostics.

## Tasks

- [x] 1. Add partition response support
  - [x] 1.1 Add translation helpers in `crates/tokeira-edge/src/grpc/translate.rs`
    - Produce one normal partition for the current queue model.
    - Landed: `list_task_queue_partitions_{request_to_edge,response_to_proto}` +
      `task_queue_partition_to_proto`; edge types `ListTaskQueuePartitions{Request,Response}`
      and `TaskQueuePartition` in `translate/mod.rs`.
    - _Requirements: 1.1, 1.3_
  - [x] 1.2 Implement `list_task_queue_partitions`
    - Validate namespace/task queue before runtime lookup.
    - Landed: `WorkflowService::list_task_queue_partitions` (interceptor +
      `Action::ListTaskQueuePartitions`) returns one root partition per task type keyed by
      the bare queue name (v1.31.0 `matching_engine.go:1609`; single partition → no
      `/_sys/<name>/<n>` suffix; `owner_host_name` empty — no edge-plane matching-host
      membership). Validation at the gRPC boundary → `INVALID_ARGUMENT`.
    - _Requirements: 1.1, 1.2, 3.1, 3.2_

- [ ] 2. Complete describe diagnostics
  - [ ] 2.1 Add read-only runtime queue diagnostics
    - Broker pollers, backlog status, worker reachability, and build-id data.
    - **Deferred** (not in the edge-unimplemented worklist): `DescribeTaskQueue` already
      responds (pollers + config + versioning); backlog count/age/rate and build-id
      reachability are field-level `Partial` enrichment tracked in
      `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`, not whole-RPC UNIMPLEMENTED work.
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 2.2 Update describe response projection
    - Preserve deprecated field behavior.
    - **Deferred** with 2.1 (field-level enrichment).
    - _Requirements: 2.4, 2.5_

- [ ] 3. Add required tests
  - [x] 3.1 Property test: Single-Partition Compatibility
    - Landed: `list_task_queue_partitions_returns_one_root_partition_per_type` (grpc handler
      test) — exactly one root partition per task type, keyed by the queue name.
    - _Requirements: 1.1, 1.3_
  - [ ] 3.2 Property test: Describe Reflects Runtime State
    - **Deferred** with task 2 (Describe field enrichment).
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 3.3 Property test: Validation Before Lookup
    - Landed: `list_task_queue_partitions_validates_before_lookup` — empty namespace,
      absent task queue, empty name, and unrecognized kind enum all reject before lookup.
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
