# Implementation Plan: Commit Fencing Correctness

## Overview

Fix the TOCTOU fencing hole in `commit_transition_for_bundle`, route all runtime mutation paths through the fenced commit, correct the hard-coded shard count in continue-as-new successor timeout routing, and split the two largest source files into focused sub-modules along correctness boundaries.

Target files:

- `crates/tokeira-storage/src/dsql/run_repository.rs` — collapse epoch check into the write transaction
- `crates/tokeira-storage/src/memory.rs` — atomic fencing under same lock
- `crates/tokeira-runtime/src/runtime.rs` — route activity start/retry through fenced commit
- `crates/tokeira-runtime/src/lane.rs` — replace hard-coded shard count with `shard_owner.shard_count()`

Ordering rationale: exploratory tests first (confirm bugs exist on unfixed code), then core fencing fix, then runtime path audit, then shard count fix, then module splits (after fencing is correct so the split is a pure reorganisation), then property test verification, then full checkpoint.

## Tasks

- [x] 1. Write bug condition exploration tests
  - **Property 1: Bug Condition** - Stale Owner Commits Through TOCTOU Race and Unfenced Paths
  - **CRITICAL**: These tests MUST FAIL on unfixed code — failure confirms the bug exists
  - **DO NOT attempt to fix the tests or the code when they fail**
  - **NOTE**: These tests encode the expected behavior — they will validate the fix when they pass after implementation
  - **GOAL**: Surface counterexamples that demonstrate the three bug modes exist
  - **Scoped PBT Approach**: Scope properties to concrete failing cases for reproducibility
  - Test Mode 1 (TOCTOU race): Simulate Runtime A holding bundle at epoch 3, advance durable epoch to 4 externally, then call `commit_transition_for_bundle` with epoch 3 — assert `CommitResult::Conflict` (will FAIL on unfixed code because epoch check is in a separate rolled-back transaction)
  - Test Mode 2 (Unfenced activity start): Call `start_activity_task` path with `ShardEpoch::ZERO` while durable epoch is non-zero — assert the commit is rejected when the caller's lease is stale (will FAIL on unfixed code because it bypasses fencing)
  - Test Mode 3 (Unfenced activity retry): Call `complete_activity_task` retry path with `ShardEpoch::ZERO` while durable epoch is non-zero — assert the commit is rejected (will FAIL on unfixed code)
  - Test Mode 4 (Hard-coded shard count): With `shard_count = 32`, commit a continue-as-new successor with workflow timeouts — assert timeout entry shard_id equals `shard_for(run_key, 32)` not `shard_for(run_key, 1)` (will FAIL on unfixed code)
  - Run tests on UNFIXED code
  - **EXPECTED OUTCOME**: Tests FAIL (this is correct — it proves the bugs exist)
  - Document counterexamples found to understand root cause
  - Mark task complete when tests are written, run, and failures are documented
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [x] 2. Write preservation property tests (BEFORE implementing fix)
  - **Property 2: Preservation** - Zero-Epoch Bypass and Matching-Epoch Commit
  - **IMPORTANT**: Follow observation-first methodology
  - Observe: `commit_transition_for_bundle(run_key, transition, ShardEpoch::ZERO)` skips epoch check and applies based on `transition_seq` on unfixed code
  - Observe: `commit_transition_for_bundle(run_key, transition, epoch)` where epoch matches durable lease returns `CommitResult::Applied` on unfixed code
  - Observe: When `transition_seq` is stale (concurrent write to same run), unfixed code returns `CommitResult::Conflict` and lane retry loop handles it
  - Observe: In-memory store produces same fencing behavior as DSQL store for non-zero epochs
  - Write property-based test: for all commits with `ShardEpoch::ZERO`, the result depends only on `transition_seq` OCC — fencing is skipped entirely
  - Write property-based test: for all commits where caller epoch matches durable epoch, result is `CommitResult::Applied` (assuming `transition_seq` is current)
  - Write property-based test: for all OCC conflicts (stale `transition_seq`), result is `CommitResult::Conflict` regardless of epoch
  - Write property-based test: lane OCC retry loop retries up to `max_occ_retries` before surfacing error
  - Verify all tests pass on UNFIXED code
  - **EXPECTED OUTCOME**: Tests PASS (this confirms baseline behavior to preserve)
  - Mark task complete when tests are written, run, and passing on unfixed code
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 3. Fix core fencing in `commit_transition_for_bundle`

  - [x] 3.1 Pass real epoch through to `commit_transition` in DSQL run repository
    - In `crates/tokeira-storage/src/dsql/run_repository.rs`, function `commit_transition_for_bundle`
    - Remove the separate epoch-check transaction (the block that opens a transaction, reads `shard_lease.epoch`, and rolls back)
    - Change the call from `self.commit_transition(run_key, transition, ShardEpoch::ZERO)` to `self.commit_transition(run_key, transition, epoch)` so the existing in-transaction epoch check activates
    - The existing `should_check_epoch` branch in `commit_transition` already reads `shard_lease` within the write transaction — passing the real epoch enables it
    - _Bug_Condition: isBugCondition(input) where path == MutationPath::FencedBundle AND epoch != ShardEpoch::ZERO AND epoch_check_transaction != write_transaction_
    - _Expected_Behavior: epoch check and mutation write execute within the SAME DSQL transaction; OCC detects concurrent epoch change atomically_
    - _Preservation: When epoch == ShardEpoch::ZERO, should_check_epoch is false, fencing is skipped — preserving single-node/test behavior_
    - _Requirements: 2.1, 2.5_

  - [x] 3.2 Fix atomic fencing in memory store
    - In `crates/tokeira-storage/src/memory.rs`, function `commit_transition_for_bundle`
    - Ensure the epoch check and the mutation write execute under the same `Mutex<StoreState>` lock acquisition
    - Currently the implementation drops the lock between the epoch check and the inner `commit_transition` call (because `commit_transition` re-acquires it)
    - Inline the epoch check into the same lock scope as the write, or restructure to hold the lock across both operations
    - _Bug_Condition: epoch check and write not atomic under same lock_
    - _Expected_Behavior: epoch check and mutation are atomic — no interleaving possible_
    - _Preservation: When epoch == ShardEpoch::ZERO, skip fencing entirely as before_
    - _Requirements: 2.1, 3.4_

  - [x]* 3.3 Unit tests for core fencing fix
    - Test `commit_transition_for_bundle` with matching epoch → `CommitResult::Applied`
    - Test `commit_transition_for_bundle` with stale epoch → `CommitResult::Conflict`
    - Test `commit_transition_for_bundle` with `ShardEpoch::ZERO` → skips fence, applies based on `transition_seq`
    - Test `commit_transition_for_bundle` with no lease row → `CommitResult::Conflict`
    - Test in-memory store: same four cases produce identical results
    - _Requirements: 2.1, 2.5, 3.1, 3.4_

- [x] 4. Fix runtime mutation paths (activity start, activity retry, all other unfenced paths)

  - [x] 4.1 Route activity start through fenced commit
    - In `crates/tokeira-runtime/src/runtime.rs`, function `start_activity_task` (line ~2151)
    - Replace `repo.commit_transition(task.run_key, transition, ShardEpoch::ZERO)` with `repo.commit_transition_for_bundle(task.run_key, execution_home_bundle, transition, epoch)`
    - Resolve `execution_home_bundle` and `epoch` from the `ShardOwner` for the run's shard
    - _Bug_Condition: input.path == MutationPath::ActivityStart AND input.epoch == ShardEpoch::ZERO AND deployment_is_controller_managed_
    - _Expected_Behavior: activity start commits are fenced by the caller's current shard epoch_
    - _Preservation: In single-node compose (no controller), epoch remains ZERO and fencing is skipped_
    - _Requirements: 2.2_

  - [x] 4.2 Route activity retry through fenced commit
    - In `crates/tokeira-runtime/src/runtime.rs`, function `complete_activity_task` retry path (line ~2300)
    - Replace `repo.commit_transition(token.run_key, transition, ShardEpoch::ZERO)` with `repo.commit_transition_for_bundle(token.run_key, execution_home_bundle, transition, epoch)`
    - Same pattern as activity start — resolve bundle and epoch from `ShardOwner`
    - _Bug_Condition: input.path == MutationPath::ActivityRetry AND input.epoch == ShardEpoch::ZERO AND deployment_is_controller_managed_
    - _Expected_Behavior: activity retry commits are fenced by the caller's current shard epoch_
    - _Preservation: In single-node compose (no controller), epoch remains ZERO and fencing is skipped_
    - _Requirements: 2.3_

  - [x] 4.3 Audit all remaining `commit_transition(..., ShardEpoch::ZERO)` call sites
    - Search for all direct calls to `commit_transition` with `ShardEpoch::ZERO` in the runtime crate
    - For each call site: determine if it's a production mutation path that should be fenced
    - Route any remaining unfenced production paths through `commit_transition_for_bundle`
    - Document any intentional `ShardEpoch::ZERO` usages (e.g., test helpers, single-node paths) with comments explaining why they're exempt
    - _Requirements: 2.2, 2.3, 2.5_

  - [x]* 4.4 Unit tests for runtime path fixes
    - Test activity start path routes through fenced commit with correct epoch
    - Test activity retry path routes through fenced commit with correct epoch
    - Test that all production mutation paths in controller-managed mode use non-zero epoch
    - Test that single-node compose paths still use `ShardEpoch::ZERO` (preservation)
    - _Requirements: 2.2, 2.3, 3.1_

- [x] 5. Fix hard-coded shard count in continue-as-new successor

  - [x] 5.1 Replace hard-coded shard count with actual shard count from ownership state
    - In `crates/tokeira-runtime/src/lane.rs`, continue-as-new successor timeout tracking (line ~693)
    - Change `crate::shard::shard_for(new_state.run_key, 1)` to `crate::shard::shard_for(new_state.run_key, shard_owner.read().unwrap().shard_count())`
    - The `shard_owner` is already available in the lane's closure scope
    - _Bug_Condition: input.shard_count_used != input.actual_shard_count AND input.shard_count_used == 1_
    - _Expected_Behavior: timeout entry shard matches shard_for(run_key, owner.shard_count())_
    - _Preservation: With shard_count == 1 (single-node), behavior is unchanged since shard_for(key, 1) == 0 always_
    - _Requirements: 2.4_

  - [x]* 5.2 Unit tests for shard count fix
    - Test with `shard_count = 32`: continue-as-new successor timeout entry uses `shard_for(run_key, 32)` not `shard_for(run_key, 1)`
    - Test with `shard_count = 1`: behavior unchanged (both produce `ShardId(0)`)
    - Property-based test: generate random `(run_key, shard_count)` pairs and verify `shard_for(run_key, shard_count)` is deterministic and bounded by `shard_count`
    - _Requirements: 2.4_

- [x] 6. Split storage module along correctness boundaries

  - [x] 6.1 Split `crates/tokeira-storage/src/dsql/run_repository.rs` into sub-modules
    - Create `crates/tokeira-storage/src/dsql/run_repository/mod.rs` — `DsqlRunRepository` struct, constructor, shared helpers, re-exports
    - Create `crates/tokeira-storage/src/dsql/run_repository/commit.rs` — `commit_transition`, `commit_transition_for_bundle`, `write_transition`, epoch fencing
    - Create `crates/tokeira-storage/src/dsql/run_repository/load.rs` — `load_run`, `read_history`, `resolve_execution`, `find_latest_run`
    - Create `crates/tokeira-storage/src/dsql/run_repository/activity.rs` — activity dispatch table operations, `list_dispatchable_activity_tasks`
    - Create `crates/tokeira-storage/src/dsql/run_repository/timers.rs` — timer bucket operations, `list_due_timers`
    - Create `crates/tokeira-storage/src/dsql/run_repository/leases.rs` — bundle lease acquire/renew/relinquish, `LeaseRepository` impl
    - Create `crates/tokeira-storage/src/dsql/run_repository/dispatch.rs` — dispatch backlog persist/drain
    - Create `crates/tokeira-storage/src/dsql/run_repository/visibility.rs` — projection log append
    - All existing public API signatures, trait implementations preserved — this is a reorganisation, not a rewrite
    - _Requirements: 2.6, 3.6_

  - [x]* 6.2 Verify storage module split preserves all existing tests
    - Run `cargo test -p tokeira-storage` — all existing tests must pass unchanged
    - Run `cargo lint` — no new warnings from the split
    - Verify all re-exports are correct (no broken imports in dependent crates)
    - _Requirements: 3.6_

- [x] 7. Split runtime module along correctness boundaries

  - [x] 7.1 Split `crates/tokeira-runtime/src/runtime.rs` into sub-modules
    - Create `crates/tokeira-runtime/src/runtime/mod.rs` — `TokeiraRuntime` struct, constructor, public facade, re-exports
    - Create `crates/tokeira-runtime/src/runtime/workflow_task.rs` — WFT started/completed/failed/timed-out
    - Create `crates/tokeira-runtime/src/runtime/activity.rs` — activity start/complete/fail/retry/timeout
    - Create `crates/tokeira-runtime/src/runtime/query.rs` — query dispatch and buffered query resolution
    - Create `crates/tokeira-runtime/src/runtime/timeout.rs` — workflow/activity/nexus timeout scanning entry points
    - Create `crates/tokeira-runtime/src/runtime/commit.rs` — single fenced-commit entry point that all mutation paths call
    - All existing public API signatures, trait implementations preserved — this is a reorganisation, not a rewrite
    - The `commit.rs` sub-module makes it obvious that all durable mutations flow through the fenced commit entry point
    - _Requirements: 2.7, 3.6_

  - [x]* 7.2 Verify runtime module split preserves all existing tests
    - Run `cargo test -p tokeira-runtime` — all existing tests must pass unchanged
    - Run `cargo lint` — no new warnings from the split
    - Verify all re-exports are correct (no broken imports in dependent crates)
    - _Requirements: 3.6_

- [x] 8. Verify fix with property tests

  - [x] 8.1 Verify bug condition exploration tests now pass
    - **Property 1: Expected Behavior** - Stale Owner Commits Rejected Atomically
    - **IMPORTANT**: Re-run the SAME tests from task 1 — do NOT write new tests
    - The tests from task 1 encode the expected behavior (stale epoch → Conflict, unfenced paths → now fenced)
    - When these tests pass, it confirms the expected behavior is satisfied
    - Run bug condition exploration tests from step 1
    - **EXPECTED OUTCOME**: Tests PASS (confirms bugs are fixed)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x] 8.2 Verify preservation tests still pass
    - **Property 2: Preservation** - Zero-Epoch Bypass and Matching-Epoch Commit
    - **IMPORTANT**: Re-run the SAME tests from task 2 — do NOT write new tests
    - Run preservation property tests from step 2
    - **EXPECTED OUTCOME**: Tests PASS (confirms no regressions)
    - Confirm all preservation tests still pass after fix (no regressions)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [x]* 8.3 Additional property-based tests for comprehensive coverage
    - Generate random `(run_key, epoch, durable_epoch)` triples and verify: if `epoch != durable_epoch && epoch != ZERO`, result is `Conflict`; if `epoch == durable_epoch`, result depends only on `transition_seq`; if `epoch == ZERO`, fencing is skipped
    - Generate random activity task tokens with various epoch values and verify all paths route through the fenced commit when `controller_managed_placement` is true
    - Full lane execution integration test: start workflow, complete WFT, start activity, advance epoch externally, attempt activity completion → verify `Conflict`
    - Continue-as-new chain with workflow timeouts: verify timeout entries are tracked by the correct shard scanner across the chain
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.1, 3.2, 3.3_

- [x] 9. Checkpoint — Ensure all tests pass
  - Run `cargo test --workspace` — all tests pass
  - Run `cargo lint` — no warnings
  - Run `cargo +nightly fmt --all --check` — formatting clean
  - Run `cargo doc --workspace --no-deps` — documentation builds
  - Ensure all property tests from tasks 1, 2, 8.1, 8.2, 8.3 pass
  - Ensure all unit tests from tasks 3.3, 4.4, 5.2 pass
  - Ensure module split verification from tasks 6.2, 7.2 passes
  - Ask the user if questions arise

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1", "2"] },
    { "id": 1, "tasks": ["3.1", "3.2"] },
    { "id": 2, "tasks": ["3.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3", "5.1"] },
    { "id": 4, "tasks": ["4.4", "5.2"] },
    { "id": 5, "tasks": ["6.1"] },
    { "id": 6, "tasks": ["6.2"] },
    { "id": 7, "tasks": ["7.1"] },
    { "id": 8, "tasks": ["7.2"] },
    { "id": 9, "tasks": ["8.1", "8.2", "8.3"] },
    { "id": 10, "tasks": ["9"] }
  ]
}
```

## Notes

- Tasks 1 and 2 are independent and can be written in parallel
- Tasks 4 and 5 are independent of each other but both depend on task 3
- Module splits (6, 7) come AFTER fencing fixes because splitting first would make the fencing changes harder to review (too many files changing at once)
- The `*` suffix on task numbers indicates test tasks — ALL are required per project convention
- The exploration tests (task 1) are expected to FAIL on unfixed code — this is the correct outcome confirming the bug exists
- The preservation tests (task 2) are expected to PASS on unfixed code — this captures baseline behavior
