# Implementation Plan: Durable Backlog Integration

## Overview

Adds Tier C (durable backlog) to the three-tier delivery model. The implementation spans `tokeira-storage` (BacklogEntry/BacklogPayload type change) and `tokeira-runtime` (broker timestamp wrappers, waiter tracking, grace scanner, drain loop, runtime wiring). Tasks are sequenced so each step builds on the previous and ends with wiring everything together.

## Tasks

- [x] 1. Replace `BacklogTaskKind` + `BacklogEntry` with `BacklogPayload` enum in `tokeira-storage`
  - [x] 1.1 Define `BacklogPayload` enum and update `BacklogEntry` in `tokeira-storage/src/api.rs`
    - Remove `BacklogTaskKind` enum
    - Add `BacklogPayload` enum with `Workflow { logical_seq }` and `Activity { activity_id, input, schedule_event_id, attempt }` variants
    - Replace `kind: BacklogTaskKind` with `payload: BacklogPayload` on `BacklogEntry`
    - Remove the now-unused `logical_seq`, `input`, `schedule_event_id`, `attempt` Option fields if present
    - _Requirements: 9.3, 9.4_

  - [x] 1.2 Update `InMemoryStore` implementation in `tokeira-storage/src/memory.rs`
    - Update `persist_to_backlog` and `drain_backlog` to use the new `BacklogEntry` shape
    - _Requirements: 9.3_

  - [x] 1.3 Update all existing call sites and tests that reference `BacklogTaskKind` or old `BacklogEntry` fields
    - Fix `backlog_insertion_order_matches_input_order` test in `memory.rs`
    - Fix `commit_transition_does_not_write_backlog` test in `memory.rs`
    - Fix `MockTimerRepo` in `runtime.rs` (`persist_to_backlog`, `drain_backlog` signatures)
    - Fix any other compile errors across the workspace from the type change
    - _Requirements: 9.3, 9.4_

- [x] 2. Checkpoint — Storage type changes
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Broker timestamp wrappers and live-ready entry tracking
  - [x] 3.1 Add `TimestampedWorkflowTask` and `TimestampedActivityTask` structs in `broker.rs`
    - `TimestampedWorkflowTask { task: DispatchableWorkflowTask, entered_at: tokio::time::Instant }`
    - `TimestampedActivityTask { task: DispatchableActivityTask, entered_at: tokio::time::Instant }`
    - _Requirements: 1.1, 1.2, 1.4_

  - [x] 3.2 Change `BrokerState` to use `TimestampedWorkflowTask` in `sticky_ready` and `general_ready`
    - Update `publish_workflow_task` to wrap tasks with `entered_at = Instant::now()`
    - Update `try_take` to unwrap `TimestampedWorkflowTask` back to `DispatchableWorkflowTask` on delivery
    - Preserve original `entered_at` when promoting sticky → general (Requirement 1.3)
    - _Requirements: 1.1, 1.3, 1.4_

  - [x] 3.3 Change `ActivityBrokerState` to use `TimestampedActivityTask` in `ready`
    - Update `publish_activity_task` to wrap tasks with `entered_at = Instant::now()`
    - Update `try_take` to unwrap `TimestampedActivityTask` back to `DispatchableActivityTask` on delivery
    - _Requirements: 1.2, 1.4_

  - [x] 3.4 Update all existing broker tests to compile with the new timestamped types
    - Existing behavior must be unchanged — tests should pass without modification to assertions
    - _Requirements: 1.1, 1.2_

  - [x] 3.5 Write property test: Property 1 — Publish records entry timestamp
    - **Property 1: Publish records entry timestamp**
    - For any workflow or activity task published when no sync match occurs, the task is stored with a non-default `entered_at` timestamp
    - **Validates: Requirements 1.1, 1.2, 4.8, 8.1**

  - [x] 3.6 Write property test: Property 2 — Sticky promotion preserves original timestamp
    - **Property 2: Sticky promotion preserves original timestamp**
    - For any sticky workflow task promoted to general tier, the `entered_at` timestamp equals the original entry timestamp
    - **Validates: Requirements 1.3**

- [x] 4. Waiter tracking in both brokers
  - [x] 4.1 Add `waiter_counts: HashMap<QueueKey, usize>` to `BrokerState` and `ActivityBrokerState`
    - _Requirements: 10.1, 10.2_

  - [x] 4.2 Update `poll_workflow_task` to increment waiter count on entry, decrement on exit
    - Increment before waiting on `Notify`, decrement after task received or timeout
    - Remove entry from map when count reaches 0
    - _Requirements: 10.1_

  - [x] 4.3 Update `poll_activity_task` to increment waiter count on entry, decrement on exit
    - Same pattern as workflow broker
    - _Requirements: 10.2_

  - [x] 4.4 Add `queues_with_waiters()` method to `InMemoryBroker` and `InMemoryActivityBroker`
    - Returns `HashSet<QueueKey>` of queues where waiter count > 0
    - _Requirements: 10.1, 10.2, 10.3_

- [x] 5. Broker `take_expired` methods
  - [x] 5.1 Add `take_expired(grace_window)` to `InMemoryBroker`
    - Scan `sticky_ready` and `general_ready`, remove entries where `Instant::now() - entered_at >= grace_window`
    - Remove dedup keys from `enqueued` set for expired entries
    - Return the expired `DispatchableWorkflowTask` values
    - _Requirements: 3.2, 3.4, 5.2_

  - [x] 5.2 Add `take_expired(grace_window)` to `InMemoryActivityBroker`
    - Scan `ready`, remove entries where `Instant::now() - entered_at >= grace_window`
    - Remove dedup keys from `enqueued` set for expired entries
    - Return the expired `DispatchableActivityTask` values
    - _Requirements: 3.2, 3.4, 5.2_

- [x] 6. Add `BacklogConfig` struct
  - Create `BacklogConfig` in `tokeira-runtime` (e.g., new file or in `scanner.rs` / `broker.rs`)
  - Fields: `workflow_grace_window`, `activity_grace_window`, `grace_scan_interval`, `drain_interval`, `drain_batch_limit`
  - Implement `Default` with values from the design (5s grace windows, 1s scan interval, 2s drain interval, 100 batch limit)
  - _Requirements: 2.1, 2.2, 2.3, 3.6, 4.5, 4.6_

- [x] 7. Checkpoint — Broker changes
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Grace scanner background task
  - [x] 8.1 Implement `run_grace_scanner` async function
    - Follow the `run_timer_scanner` pattern: `loop { tokio::select! { cancel => break, sleep(interval) => {} }; scan_cycle(); }`
    - Each cycle: call `take_expired` on both brokers to collect expired tasks
    - Construct `BacklogEntry` values with correct `BacklogPayload` variant for each expired task
    - Call `persist_to_backlog` with the batch
    - On `persist_to_backlog` failure: log `tracing::warn!`, re-publish expired tasks back to the broker (dedup keys were already removed by `take_expired`, so re-publish succeeds and tasks get fresh timestamps)
    - Use `CancellationToken` for shutdown; in-flight `persist_to_backlog` completes before exit
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 9.3, 11.1, 11.4_

  - [x] 8.2 Implement `scan_grace_once` helper (testable single-cycle function)
    - Extracted logic for one grace scanner cycle, analogous to `scan_due_timers_once`
    - Takes broker refs, repo ref, config ref; returns nothing (side effects on broker + storage)
    - _Requirements: 3.1, 3.2, 3.3, 3.5_

  - [x] 8.3 Write property test: Property 3 — Grace scanner moves exactly the expired tasks
    - **Property 3: Grace scanner moves exactly the expired tasks**
    - Use `tokio::time::pause()` for deterministic time control
    - For any set of tasks with varying entry timestamps, after one scan cycle, exactly the expired tasks are removed and persisted
    - **Validates: Requirements 3.2, 3.3, 8.2**

  - [x] 8.4 Write property test: Property 4 — Grace scanner clears dedup keys on backlog persistence
    - **Property 4: Grace scanner clears dedup keys on backlog persistence**
    - After grace scanner moves a task, re-publishing the same `(run_key, logical_seq)` or `(run_key, activity_id, attempt)` is accepted
    - **Validates: Requirements 3.4, 5.2**

  - [x] 8.5 Write property test: Property 5 — Persist failure retains tasks in live-ready
    - **Property 5: Persist failure retains tasks in live-ready**
    - Use mock storage that returns an error from `persist_to_backlog`
    - After a failed scan cycle, all expired tasks remain in the live-ready tier with dedup keys intact
    - **Validates: Requirements 3.7**

- [x] 9. Checkpoint — Grace scanner
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Drain loop background task
  - [x] 10.1 Implement `run_drain_loop` async function
    - Follow the `run_timer_scanner` pattern with `CancellationToken`
    - Each cycle: call `queues_with_waiters()` on both brokers
    - For each queue with waiters: call `drain_backlog(queue, limit)` from storage
    - Reconstruct `DispatchableWorkflowTask` or `DispatchableActivityTask` from `BacklogPayload`
    - Re-publish to the appropriate broker in FIFO order (ascending `insertion_seq`)
    - On `drain_backlog` failure: log `tracing::warn!`, skip queue, continue to next
    - In-flight `drain_backlog` completes before shutdown exit
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.7, 4.8, 6.3, 7.2, 9.4, 11.2, 11.4_

  - [x] 10.2 Implement `drain_once` helper (testable single-cycle function)
    - Extracted logic for one drain loop cycle
    - Takes broker refs, repo ref, config ref; performs drain + re-publish
    - _Requirements: 4.2, 4.3, 4.4_

  - [x] 10.3 Write property test: Property 6 — Drain loop targets only queues with waiters
    - **Property 6: Drain loop targets only queues with waiters**
    - Use mock storage that records `drain_backlog` calls
    - Verify `drain_backlog` is called only for queues where `queues_with_waiters()` reports > 0
    - **Validates: Requirements 4.2, 4.3, 10.3**

  - [x] 10.4 Write property test: Property 7 — Drain loop routes entries to the correct broker by kind
    - **Property 7: Drain loop routes entries to the correct broker by kind**
    - For any `BacklogEntry` with `Workflow` payload, verify it is re-published to `InMemoryBroker`
    - For any `BacklogEntry` with `Activity` payload, verify it is re-published to `InMemoryActivityBroker`
    - **Validates: Requirements 4.4, 9.3, 9.4**

  - [x] 10.5 Write property test: Property 8 — Dedup prevents double dispatch
    - **Property 8: Dedup prevents double dispatch**
    - For any task already in the live-ready tier, re-publishing the same dedup key is suppressed
    - **Validates: Requirements 5.1, 5.3, 5.4, 8.4**

  - [x] 10.6 Write property test: Property 9 — FIFO order preserved through drain and re-publish
    - **Property 9: FIFO order preserved through drain and re-publish**
    - For any ordered sequence of `BacklogEntry` values from `drain_backlog`, the drain loop re-publishes them in the same order
    - **Validates: Requirements 6.3, 7.3**

- [x] 11. Checkpoint — Drain loop
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Runtime wiring — Spawn grace scanner and drain loop
  - [x] 12.1 Add `BacklogConfig` parameter to `TokeiraRuntime` constructors
    - Add `backlog_config: BacklogConfig` parameter to `new`, `new_with_nexus`, `new_with_nexus_and_shards`
    - _Requirements: 2.1_

  - [x] 12.2 Add grace scanner fields to `TokeiraRuntime` struct
    - Add `grace_scanner_handle: Option<tokio::task::JoinHandle<()>>`
    - Add `grace_scanner_cancel: CancellationToken`
    - Spawn `run_grace_scanner` in `new_with_nexus_and_shards`, passing broker clones, repo, config, cancel token
    - _Requirements: 3.1, 11.1, 11.4_

  - [x] 12.3 Add drain loop fields to `TokeiraRuntime` struct
    - Add `drain_loop_handle: Option<tokio::task::JoinHandle<()>>`
    - Add `drain_loop_cancel: CancellationToken`
    - Spawn `run_drain_loop` in `new_with_nexus_and_shards`, passing broker clones, repo, config, cancel token
    - _Requirements: 4.1, 11.2, 11.4_

  - [x] 12.4 Add `shutdown_grace_scanner` and `shutdown_drain_loop` methods
    - Follow the existing `shutdown_timer_scanner` pattern (cancel → take handle → timeout 5s → await)
    - _Requirements: 11.1, 11.2, 11.4_

  - [x] 12.5 Update existing tests and callers of `TokeiraRuntime::new*` to pass `BacklogConfig`
    - Add `BacklogConfig::default()` to all existing constructor call sites
    - _Requirements: 2.2_

- [x] 13. Integration tests — Full lifecycle and shutdown
  - [x] 13.1 Write integration test: publish → grace expiry → backlog persist → drain → re-publish → poll delivery
    - Use `InMemoryStore` and `tokio::time::pause()` for deterministic time
    - Verify a task published with no poller is eventually delivered after grace expiry + drain cycle
    - _Requirements: 1.1, 3.2, 3.3, 4.4, 4.8, 6.2, 6.3_

  - [x] 13.2 Write integration test: graceful shutdown during in-flight storage call
    - Cancel the grace scanner and drain loop while a storage call is in progress
    - Verify the in-flight call completes and the background task exits cleanly
    - _Requirements: 11.1, 11.2, 11.4_

- [x] 14. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property test tasks are required (not optional) per project guidance
- Each property test maps to a correctness property from the design document
- Use `proptest` for property-based tests (already used in the project)
- Use `tokio::time::pause()` for deterministic time control in grace window tests
- Use `tokio::time::Instant` for monotonic timestamps (not wall-clock)
- Use `CancellationToken` for background task shutdown
- `rustfmt max_width = 90`
- The `BacklogEntry` type change affects existing storage tests — those must be updated in task 1.3
- Broker changes must preserve all existing test behavior
