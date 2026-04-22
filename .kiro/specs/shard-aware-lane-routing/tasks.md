# Implementation Plan: Shard-Aware Lane Routing

## Overview

Replace hash-based lane routing (`hash(run_key) % lane_count`) with shard-aware routing (`shard_id % lane_count`) in `tokeira-runtime`. The change touches two core functions in `scanner.rs` and 9 call sites across 6 subsystems. No kernel or edge changes.

## Tasks

- [x] 1. Update core routing functions in `scanner.rs`
  - [x] 1.1 Change `lane_index_for` to accept `ShardId` instead of `RunKey`
    - Replace the hash-based computation with `shard_id.0 as usize % lane_count.max(1)`
    - Remove `Hash` import and `DefaultHasher` usage
    - _Requirements: 1.1, 1.2, 11.1_
  - [x] 1.2 Change `pick_lane` to accept `ShardId` instead of `RunKey`
    - Add `debug_assert!(!lanes.is_empty())` and `debug_assert_eq!(lanes.len(), lane_count.max(1))`
    - _Requirements: 1.3_
  - [x]* 1.3 Write property test: shard-to-lane computation correctness
    - **Property 1: Shard-to-lane computation correctness**
    - Generate random `(ShardId, lane_count >= 1)`, verify result equals `shard_id.0 as usize % lane_count`
    - Use `proptest` with `ProptestConfig { cases: 100, .. }` minimum
    - Tag: `// Feature: shard-aware-lane-routing, Property 1: Shard-to-lane computation correctness`
    - **Validates: Requirements 1.1**
  - [x]* 1.4 Write property test: lane index bounds
    - **Property 3: Lane index bounds**
    - Generate random `(ShardId, lane_count >= 1)`, verify `0 <= result < lane_count`
    - Use `proptest` with `ProptestConfig { cases: 100, .. }` minimum
    - Tag: `// Feature: shard-aware-lane-routing, Property 3: Lane index bounds`
    - **Validates: Requirements 2.2**
  - [x]* 1.5 Write unit tests for core routing functions
    - `test_lane_index_for_basic`: `ShardId(0)` with 4 lanes → 0, `ShardId(7)` with 4 lanes → 3
    - `test_pick_lane_returns_correct_handle`: build `Vec<LaneHandle>`, verify correct handle returned
    - `test_lane_count_one_always_zero`: any ShardId with `lane_count=1` returns 0
    - _Requirements: 1.1, 1.3, 2.2_

- [x] 2. Migrate scanner callers to use `ShardId`
  - [x] 2.1 Update `run_timer_scanner` closure in `scanner.rs`
    - Replace `due.run_key` with captured `shard_id` from `for shard_id in active_shards` loop
    - _Requirements: 3.1, 3.2_
  - [x] 2.2 Update `run_wft_timeout_scanner` closure in `wft_timeout.rs`
    - Replace `entry.run_key` with `entry.shard_id` from `WftTimeoutEntry`
    - _Requirements: 4.1_
  - [x] 2.3 Update `scan_activity_timeouts_once` in `activity_timeout.rs`
    - Replace `entry.run_key` with `entry.shard_id` from `ActivityTrackingEntry` (works for both shard-filtered and unfiltered scans)
    - _Requirements: 5.1_
  - [x] 2.4 Update nexus timeout scanner closure in `nexus.rs`
    - Replace `entry.run_key` with `entry.shard_id` from `NexusTimeoutEntry`
    - _Requirements: 6.1_
  - [x] 2.5 Update `run_workflow_timeout_scanner` closure in `timeout.rs`
    - Replace `entry.run_key` with `entry.shard_id` from tracking entry
    - _Requirements: 6a.1_
  - [x] 2.6 Update `sweep_shard` due-timer loop in `recovery.rs`
    - Replace `due.run_key` with `shard_id` parameter (already the first arg of `sweep_shard`)
    - _Requirements: 7.1, 7.2_

- [x] 3. Migrate publisher and runtime callers to derive `ShardId`
  - [x] 3.1 Update `RuntimeDispatchPublisher::pick_lane` in `publisher.rs`
    - Derive `ShardId` via `shard_for(run_key, self.shard_count)` before calling `pick_lane`
    - `shard_count` is already on the struct
    - _Requirements: 8.1, 8.2, 10.1, 10.2_
  - [x] 3.2 Update `TokeiraRuntime::pick_lane` in `runtime.rs`
    - Read `shard_count` from `self.shard_owner.read().unwrap().shard_count()`
    - Derive `ShardId` via `shard_for(run_key, shard_count)` before calling `pick_lane`
    - _Requirements: 9.1, 9.3, 10.1, 10.2_
  - [x] 3.3 Update `TokeiraRuntime::lane_index` in `runtime.rs`
    - Same derivation as `pick_lane`: read `shard_count` from `ShardOwner`, derive `ShardId`
    - _Requirements: 9.2, 9.3, 10.1, 10.2_
  - [x]* 3.4 Write property test: shard-lane affinity
    - **Property 2: Shard-lane affinity**
    - Generate random `(run_key_a, run_key_b, shard_count >= 1, lane_count >= 1)` where both keys map to the same shard, verify same lane index
    - Use `proptest` with `ProptestConfig { cases: 100, .. }` minimum
    - Tag: `// Feature: shard-aware-lane-routing, Property 2: Shard-lane affinity`
    - **Validates: Requirements 1.4, 2.1**
  - [x]* 3.5 Write property test: end-to-end routing determinism
    - **Property 4: End-to-end routing determinism**
    - Generate random `(run_key, shard_count >= 1, lane_count >= 1)`, call composed routing twice, verify equal results
    - Use `proptest` with `ProptestConfig { cases: 100, .. }` minimum
    - Tag: `// Feature: shard-aware-lane-routing, Property 4: End-to-end routing determinism`
    - **Validates: Requirements 2.3, 11.1, 11.2**
  - [x]* 3.6 Write property test: RunKey-to-lane affinity through shard derivation
    - **Property 5: RunKey-to-lane affinity through shard derivation**
    - Generate random `(run_key_a, run_key_b, shard_count >= 1, lane_count >= 1)` in same shard, verify `shard_for` + `lane_index_for` produces same lane for both
    - Use `proptest` with `ProptestConfig { cases: 100, .. }` minimum
    - Tag: `// Feature: shard-aware-lane-routing, Property 5: RunKey-to-lane affinity through shard derivation`
    - **Validates: Requirements 8.1, 9.1, 9.2**
  - [x]* 3.7 Write unit tests for publisher and runtime routing
    - `test_publisher_routes_via_shard`: two RunKeys in the same shard routed by publisher land on the same lane
    - `test_runtime_pick_lane_uses_shard`: two RunKeys in the same shard routed by runtime land on the same lane
    - _Requirements: 8.1, 9.1_

- [x] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Update architecture documentation
  - [x] 5.1 Update `docs/architecture/005-decisions-and-boundaries.md`
    - Document the routing change from `hash(run_key) % lane_count` to `shard_id % lane_count`
    - Explain the rationale: shard movement blast radius reduced from all lanes to one lane
    - _Requirements: 11.1, 11.2, 11.3_

- [x] 6. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The design uses Rust throughout — all code examples use Rust
- Property tests use `proptest` crate with minimum 100 iterations
- Callers split into two patterns: timer scanner/sweeper pass loop/parameter `shard_id` directly; timeout scanners (WFT, activity, nexus, workflow execution) use `entry.shard_id` from tracking entries; publisher/runtime derive via `shard_for(run_key, shard_count)`
- No new data models, error types, or dependencies introduced
