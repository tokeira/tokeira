# Implementation Plan: Sweeper and Recovery

## Overview

Implement shard-scoped ownership, post-failover sweep reconstruction, epoch-fenced task tokens, and shard-scoped scanning for the Tokeira runtime. This adds `ShardOwner` state tracking, a `shard_for()` deterministic mapping, six new shard-filtered `RunRepository` query methods, a one-time `sweep_shard()` function, a `LeaseRenewer` background task, shard-scoped timeout tracking with `shard_id` on all tracking entries, shard-scoped timer scanning, `InMemoryStore` shard-to-run mapping, `TokeiraRuntime` shard lifecycle methods, command admission gating, and epoch fencing on task tokens.

All code is Rust. The implementation spans `tokeira-types` (no changes needed — `ShardId` and `ShardEpoch` already exist), `tokeira-kernel` (extend `ActivityState` with timestamps), `tokeira-storage` (trait extensions including epoch on `commit_transition`, InMemoryStore) and `tokeira-runtime` (ShardOwner, sweeper, lease renewer, scanner changes, runtime methods).

## Tasks

- [x] 1. Define `shard_for()` mapping and `ShardOwner` struct
  - [x] 1.1 Implement `shard_for(run_key, shard_count) -> ShardId` in `tokeira-runtime`
    - Pure deterministic function: `ShardId((run_key.0.as_u128() as u32) % shard_count)`
    - Place in a new `shard.rs` module, add `pub mod shard;` to `lib.rs`
    - _Requirements: 14.1, 14.2_

  - [x] 1.2 Implement `ShardOwner`, `OwnedShard`, and `ShardState` structs
    - `ShardState` enum: `Sweeping`, `Active`, `Draining`
    - `OwnedShard`: `epoch: ShardEpoch`, `state: ShardState`, `cancel: CancellationToken`
    - `ShardOwner`: `shards: HashMap<ShardId, OwnedShard>`, `shard_count: u32`
    - Methods: `owns(shard_id) -> Option<ShardEpoch>` (returns epoch only if Active), `epoch_of(shard_id) -> Option<ShardEpoch>` (returns epoch in any state — used for completion validation during Draining), `is_active(shard_id) -> bool`, `record_acquired(shard_id, epoch) -> CancellationToken`, `mark_active(shard_id)`, `mark_draining(shard_id)`, `remove(shard_id)`
    - _Requirements: 1.2, 11.1, 11.4_

  - [x] 1.3 Write property test for shard ownership round-trip
    - **Property 1: Shard ownership round-trip**
    - **Validates: Requirements 1.2**
    - For any `ShardId` and `ShardEpoch`, after `record_acquired` then `mark_active`, `owns()` returns `Some(epoch)` with the same value

  - [x] 1.4 Write property test for deterministic shard assignment
    - **Property 16: Deterministic shard assignment in InMemoryStore**
    - **Validates: Requirements 14.1, 14.2**
    - For any `RunKey` and `shard_count > 0`, `shard_for()` always returns the same `ShardId`, and the result is in `[0, shard_count)`

  - [x] 1.5 Write property test for command rejection during sweep phase
    - **Property 12: Commands are rejected during sweep phase**
    - **Validates: Requirements 11.1, 11.2, 11.4**
    - For any shard in `Sweeping` state, `is_active()` returns false; after `mark_active()`, `is_active()` returns true

- [x] 2. Checkpoint - Verify ShardOwner and shard_for
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Extend `RunRepository` trait with shard-filtered query methods
  - [x] 3.1 Add sweep entry types to `tokeira-storage/src/api.rs`
    - Define `WorkflowTimeoutSweepEntry`, `ActivitySweepEntry`, `NexusSweepEntry` structs
    - _Requirements: 8.1, 9.1, 10.1_

  - [x] 3.2 Add six shard-filtered methods to the `RunRepository` trait
    - `list_dispatchable_workflow_tasks_for_shard(shard_id, limit)`
    - `list_dispatchable_activity_tasks_for_shard(shard_id, limit)`
    - `list_due_timers_for_shard(shard_id, now, limit)`
    - `list_runs_with_workflow_timeouts_for_shard(shard_id, limit)`
    - `list_open_activities_for_shard(shard_id, limit)`
    - `list_pending_nexus_operations_for_shard(shard_id, limit)`
    - Add corresponding delegation in the `Arc<T>` blanket impl
    - _Requirements: 4.1, 5.1, 6.1, 8.1, 9.1, 10.1, 14.3, 14.4, 14.5, 14.6_

  - [x] 3.3 Extend `commit_transition` to accept `ShardEpoch` parameter
    - Add `epoch: ShardEpoch` parameter to `RunRepository::commit_transition`
    - Update the `Arc<T>` blanket impl delegation
    - Update `InMemoryStore::commit_transition` to validate epoch against `bundle_leases` (skip validation when `epoch == ShardEpoch::ZERO` for backward compatibility)
    - Update all call sites in `tokeira-runtime` (lane.rs) to pass the epoch from `ShardOwner` (or `ShardEpoch::ZERO` until shard lifecycle is wired)
    - _Requirements: 1.4, 1.5, 1.6_

  - [x] 3.4 Extend `ActivityState` with `scheduled_at` and `started_at` timestamps
    - Add `scheduled_at: OffsetDateTime` and `started_at: Option<OffsetDateTime>` to `ActivityState` in `tokeira-kernel/src/state.rs`
    - Update the kernel to populate `scheduled_at` from `builder.now` (the `happened_at` of the emitted event) when processing `ScheduleActivity` in `apply_workflow_command`
    - `started_at` is set by the runtime in `start_activity_task` (the activity-start OCC upsert), NOT by the kernel — activity starts are runtime-side operations
    - Update all `ActivityState` construction sites (kernel, storage tests) to provide the new fields
    - _Requirements: 8.4, 8.5_

  - [x] 3.5 Extend `PendingNexusOperation` with `schedule_to_close_timeout` and `scheduled_at`
    - Add `schedule_to_close_timeout: Option<Duration>` and `scheduled_at: OffsetDateTime` to `PendingNexusOperation` in `tokeira-kernel/src/state.rs`
    - Update the kernel to populate both fields when processing `ScheduleNexusOperation` in `apply_workflow_command`
    - Update all `PendingNexusOperation` construction sites (kernel, storage tests) to provide the new fields
    - _Requirements: 10.4, 10.5_

- [x] 4. Implement `InMemoryStore` shard support
  - [x] 4.1 Add `run_shard_map: HashMap<RunKey, ShardId>` and `shard_count: u32` to `StoreState`
    - Add a constructor or builder method to set `shard_count`
    - On `commit_transition` for a new run (transition_seq == 0), compute `shard_for(run_key, shard_count)` and insert into `run_shard_map`
    - _Requirements: 14.1, 14.2_

  - [x] 4.2 Implement the six shard-filtered query methods on `InMemoryStore`
    - Filter candidates by looking up each run's shard in `run_shard_map`
    - `list_runs_with_workflow_timeouts_for_shard`: scan `runs` for open runs with timeout config in the given shard
    - `list_open_activities_for_shard`: scan `activity_state_table` for activities belonging to runs in the given shard
    - `list_pending_nexus_operations_for_shard`: scan `runs` for pending nexus operations in the given shard
    - _Requirements: 14.3, 14.4, 14.5, 14.6_

  - [x] 4.3 Write property test for shard-filtered query correctness
    - **Property 17: Shard-filtered query correctness in InMemoryStore**
    - **Validates: Requirements 14.3, 14.4, 14.5, 14.6**
    - Create runs distributed across multiple shards; verify shard-filtered queries return only items belonging to the specified shard and return all such items (up to limit)

- [x] 5. Checkpoint - Verify storage layer extensions
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Add `shard_id` field to all timeout tracking entries
  - [x] 6.1 Add `shard_id: ShardId` to `WorkflowTimeoutEntry` in `timeout.rs`
    - Update all call sites that construct `WorkflowTimeoutEntry` to provide `shard_id`
    - Add `remove_all_for_shard(shard_id)` and `snapshot_for_shard(shard_id)` to `WorkflowTimeoutTrackingState`
    - _Requirements: 13.1, 13.4_

  - [x] 6.2 Add `shard_id: ShardId` to `ActivityTrackingEntry` in `activity_timeout.rs`
    - Update all call sites that construct `ActivityTrackingEntry` to provide `shard_id`
    - Add `remove_all_for_shard(shard_id)` and `snapshot_for_shard(shard_id)` to `ActivityTrackingState`
    - _Requirements: 13.2, 13.4_

  - [x] 6.3 Add `shard_id: ShardId` to `NexusTimeoutEntry` in `nexus.rs`
    - Update all call sites that construct `NexusTimeoutEntry` to provide `shard_id`
    - Add `remove_all_for_shard(shard_id)` and `snapshot_for_shard(shard_id)` to `NexusTimeoutTrackingState`
    - _Requirements: 13.3, 13.4_

  - [x] 6.4 Write property test for shard-scoped timeout scanning
    - **Property 14: Shard-scoped timeout scanning**
    - **Validates: Requirements 13.1, 13.2, 13.3**
    - Create entries spanning multiple shards; verify `snapshot_for_shard` returns only entries for the specified shard

  - [x] 6.5 Write property test for tracking state cleanup on shard relinquish
    - **Property 15: Tracking state cleanup on shard relinquish**
    - **Validates: Requirements 13.4, 15.4**
    - Insert entries for multiple shards; call `remove_all_for_shard` for one shard; verify entries for that shard are gone and entries for other shards remain

- [x] 7. Checkpoint - Verify shard-scoped tracking state
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement `sweep_shard()` one-time scan function
  - [x] 8.1 Define `SweepResult` struct and implement `sweep_shard()` async function
    - `SweepResult` with counts: `workflow_tasks_republished`, `activity_tasks_republished`, `due_timers_injected`, `workflow_timeout_entries_reconstructed`, `activity_tracking_entries_reconstructed`, `nexus_timeout_entries_reconstructed`, `expired_sticky_claims_cleared`
    - Call `list_dispatchable_workflow_tasks_for_shard` → publish each to `InMemoryBroker` (clear expired sticky → `sticky_preferred = None`)
    - Call `list_dispatchable_activity_tasks_for_shard` → publish each to `InMemoryActivityBroker`
    - Call `list_due_timers_for_shard` → inject `Command::TimerDue` via lane submit
    - Call `list_runs_with_workflow_timeouts_for_shard` → insert into `WorkflowTimeoutTrackingState`
    - Call `list_open_activities_for_shard` → insert into `ActivityTrackingState`
    - Call `list_pending_nexus_operations_for_shard` → insert into `NexusTimeoutTrackingState`
    - _Requirements: 4.1, 4.2, 4.3, 5.1, 5.2, 5.3, 6.1, 6.2, 6.3, 7.1, 7.2, 7.3, 8.1, 8.2, 8.3, 9.1, 9.2, 9.3, 10.1, 10.2, 10.3_

  - [x] 8.2 Write property test for workflow task sweep completeness
    - **Property 5: Workflow task sweep completeness**
    - **Validates: Requirements 4.1, 4.2**
    - Create runs with pending workflow tasks in a shard; run `sweep_shard`; verify broker contains a task for each run

  - [x] 8.3 Write property test for activity task sweep completeness
    - **Property 6: Activity task sweep completeness**
    - **Validates: Requirements 5.1, 5.2**
    - Create runs with dispatchable activities in a shard; run `sweep_shard`; verify activity broker contains a task for each activity

  - [x] 8.4 Write property test for due timer sweep completeness
    - **Property 7: Due timer sweep completeness**
    - **Validates: Requirements 6.1, 6.2**
    - Create runs with due timers in a shard; run `sweep_shard`; verify `TimerDue` commands were submitted for each timer

  - [x] 8.5 Write property test for expired sticky claims
    - **Property 8: Expired sticky claims are republished without sticky preference**
    - **Validates: Requirements 7.1, 7.2**
    - Create runs with expired sticky claims and pending workflow tasks; run `sweep_shard`; verify tasks in broker have `sticky_preferred = None`

  - [x] 8.6 Write property test for activity tracking reconstruction fidelity
    - **Property 9: Activity tracking reconstruction fidelity**
    - **Validates: Requirements 8.1, 8.2, 8.3**
    - Create open activities with timeout config; run `sweep_shard`; verify `ActivityTrackingState` entries match authoritative state

  - [x] 8.7 Write property test for workflow timeout tracking reconstruction fidelity
    - **Property 10: Workflow timeout tracking reconstruction fidelity**
    - **Validates: Requirements 9.1, 9.2, 9.3**
    - Create open runs with timeout config; run `sweep_shard`; verify `WorkflowTimeoutTrackingState` entries match authoritative state

  - [x] 8.8 Write property test for Nexus timeout tracking reconstruction fidelity
    - **Property 11: Nexus timeout tracking reconstruction fidelity**
    - **Validates: Requirements 10.1, 10.2, 10.3**
    - Create pending Nexus operations with timeout config; run `sweep_shard`; verify `NexusTimeoutTrackingState` entries match authoritative state

- [x] 9. Checkpoint - Verify sweep function and all reconstruction properties
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement `LeaseRenewer` background task
  - [x] 10.1 Implement `run_lease_renewer()` async function
    - Periodically call `LeaseRepository::renew_bundle` with shard_id, owner, epoch
    - On `Renewed`: continue at configured interval
    - On `Rejected`: signal `on_lost` oneshot sender, break loop
    - On transient error: retry with bounded backoff; after `max_retries` consecutive failures, treat as lease lost
    - Use `CancellationToken` for graceful shutdown
    - Use `DbClass::Control` permits
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 10.2 Write property test for command rejection on lease loss
    - **Property 13: Command rejection on lease loss**
    - **Validates: Requirements 2.4, 15.1**
    - After `mark_draining` on a shard, `is_active()` returns false and `owns()` returns None

- [x] 11. Implement epoch fencing on task tokens in `TokeiraRuntime`
  - [x] 11.1 Update `start_polled_workflow_task` to set `token.shard_epoch` from `ShardOwner`
    - Look up `shard_for(run_key)`, get epoch from `shard_owner.owns(shard_id)`, set on token
    - Return error if shard not owned
    - _Requirements: 3.1_

  - [x] 11.2 Update `start_activity_task` to set `token.shard_epoch` from `ShardOwner`
    - Same pattern as workflow task start
    - _Requirements: 3.2_

  - [x] 11.3 Update `validate_activity_token` to check `shard_epoch` against current epoch
    - Check `token.shard_epoch` matches `shard_owner.epoch_of(shard_for(token.run_key))` — use `epoch_of()` not `owns()` so that in-flight completions succeed during Draining (Req 15.3)
    - Replace the existing `ShardEpoch::ZERO` check with a lookup against `ShardOwner`
    - _Requirements: 3.3_

  - [x] 11.4 Update `complete_workflow_task` to validate `shard_epoch` against current epoch
    - Check `token.shard_epoch` matches `shard_owner.epoch_of(shard_for(token.run_key))` — use `epoch_of()` not `owns()` so that in-flight completions succeed during Draining (Req 15.3)
    - _Requirements: 3.3_

  - [x] 11.5 Write property test for task tokens carrying current shard epoch
    - **Property 3: Task tokens carry current shard epoch**
    - **Validates: Requirements 3.1, 3.2**
    - For any owned shard with epoch E, started workflow/activity tokens have `shard_epoch == E`

  - [x] 11.6 Write property test for stale epoch completion rejection
    - **Property 4: Stale epoch completions are rejected**
    - **Validates: Requirements 3.3**
    - For any token with `shard_epoch != current_epoch`, completion is rejected and state is unchanged

  - [x] 11.7 Write property test for epoch fencing rejecting stale commits
    - **Property 2: Epoch fencing rejects stale commits**
    - **Validates: Requirements 1.5**
    - For any shard with epoch E in storage, a commit with epoch E' ≠ E is rejected

- [x] 12. Checkpoint - Verify epoch fencing
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Wire shard lifecycle into `TokeiraRuntime`
  - [x] 13.1 Add `shard_owner: Arc<RwLock<ShardOwner>>` and `owner_identity: String` to `TokeiraRuntime`
    - Update constructors to accept `shard_count` and `owner_identity`
    - _Requirements: 1.1, 1.2_

  - [x] 13.2 Implement `acquire_shard(&self, shard_id) -> Result<ShardEpoch>` on `TokeiraRuntime`
    - Call `LeaseRepository::try_acquire_bundle`
    - On `Acquired`: record in `ShardOwner` (Sweeping state), start `LeaseRenewer`, run `sweep_shard`, transition to Active, then start shard-scoped scanners
    - Scanners (timer, workflow timeout, activity timeout, Nexus timeout) MUST NOT start until the shard is Active — this prevents them from injecting commands before sweep has reconstructed volatile state
    - On `Rejected`: return error, do not proceed
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 11.1, 11.2, 11.3, 11.4, 11.5_

  - [x] 13.3 Implement `relinquish_shard(&self, shard_id)` on `TokeiraRuntime`
    - Mark shard as `Draining` in `ShardOwner`
    - Cancel shard-scoped background tasks via `CancellationToken`
    - Remove all tracking entries for the shard from all three tracking states
    - Remove shard from `ShardOwner`
    - _Requirements: 15.1, 15.2, 15.3, 15.4, 15.5_

  - [x] 13.4 Add shard ownership check to `submit()` method
    - Before routing to lane, check `shard_owner.is_active(shard_for(run_key))`
    - Return error if shard not active
    - _Requirements: 11.1, 11.2, 15.1_

- [x] 14. Implement shard-scoped timer scanning
  - [x] 14.1 Update timer scanner to accept shard filter
    - Modify `run_timer_scanner` (or create a shard-scoped variant) to call `list_due_timers_for_shard` per owned shard instead of global `list_due_timers`
    - The scanner should iterate over owned shards each cycle, or spawn one scan loop per shard
    - _Requirements: 12.1, 12.2, 12.3_

  - [x] 14.2 Update timeout scanners to use `snapshot_for_shard`
    - Modify `scan_workflow_timeouts_once` to use `snapshot_for_shard` filtered by owned shards
    - Modify `scan_activity_timeouts_once` to use `snapshot_for_shard` filtered by owned shards
    - Modify `scan_nexus_timeouts_once` to use `snapshot_for_shard` filtered by owned shards
    - _Requirements: 13.1, 13.2, 13.3_

  - [x] 14.3 Write property test for timer scanner shard scoping
    - **Property 18: Timer scanner shard scoping**
    - **Validates: Requirements 12.1**
    - Create due timers across multiple shards; run shard-scoped timer scan for one shard; verify only timers for that shard are processed

- [x] 15. Checkpoint - Verify shard-scoped scanning
  - Ensure all tests pass, ask the user if questions arise.

- [x] 16. Integration wiring and final tests
  - [x] 16.1 Update `TokeiraRuntime::new` and `new_with_nexus` to initialize shard infrastructure
    - Initialize `ShardOwner` with configured `shard_count`
    - Pass `shard_owner` to all components that need shard awareness
    - Ensure backward compatibility: existing tests that don't use shards should still work (default single-shard or shard-unaware mode)
    - _Requirements: all_

  - [x] 16.2 Update module exports in `tokeira-runtime/src/lib.rs`
    - Export `ShardOwner`, `OwnedShard`, `ShardState`, `shard_for`, `SweepResult`, `sweep_shard`, `run_lease_renewer`
    - _Requirements: all_

  - [ ]* 16.3 Write integration tests for full shard lifecycle
    - Test acquire → sweep → admit → relinquish → drain with `InMemoryStore`
    - Test epoch fencing end-to-end: worker holds token from epoch N, shard moves to epoch N+1, completion rejected
    - Test sweep reconstructs all tracking state correctly
    - _Requirements: 1.1, 1.2, 2.1, 3.1, 3.2, 3.3, 4.1, 5.1, 6.1, 8.1, 9.1, 10.1, 11.1, 15.1_

- [x] 17. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- All property test sub-tasks are REQUIRED (not optional) — this is the most critical correctness feature in the runtime
- Each property test references a specific property from the design document (Properties 1–18)
- The sweeper is a one-time async function, not a long-lived background task
- Run actors are loaded on demand after sweep, not eagerly rehydrated
- The `shard_for()` function is pure and stateless — no storage lookup needed
- Existing tests should continue to work with a default shard-unaware configuration
- `commit_transition` gains a `ShardEpoch` parameter; passing `ShardEpoch::ZERO` skips epoch validation for backward compatibility
- `ActivityState` gains `scheduled_at` and `started_at` fields so the sweeper can reconstruct timeout tracking without history replay
- Shard-scoped scanners start only after the shard reaches Active state — the LeaseRenewer is the only background task started during Sweeping
- Depends on Features 1, 2, 3, 4, 5, and 9 (not just 1, 2, 4)
