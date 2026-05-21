# Commit Fencing Correctness Bugfix Design

## Overview

The `commit_transition_for_bundle` function in the DSQL run repository has a TOCTOU fencing hole: the epoch check runs in a transaction that is rolled back before the actual mutation commits via `commit_transition(..., ShardEpoch::ZERO)`. This creates a race window where another runtime can acquire the bundle lease between the check and the write, allowing stale owners to corrupt workflow state. Additionally, two runtime mutation paths (activity start and activity retry) bypass the fenced commit entirely, and the continue-as-new successor timeout path uses a hard-coded shard count of 1.

The fix collapses the epoch check into the same DSQL transaction as the mutation write, makes `commit_transition` private to the storage module, routes all runtime mutation paths through `commit_transition_for_bundle`, corrects the hard-coded shard count, and splits the two largest source files into focused sub-modules along correctness boundaries.

## Glossary

- **Bug_Condition (C)**: Any production commit path where the epoch check and the mutation write execute in separate transactions, OR where a mutation path bypasses the epoch check entirely, OR where shard routing uses an incorrect shard count
- **Property (P)**: Every production transition commit is fenced by the authoritative DSQL lease epoch in the same transaction as the mutation — a stale owner's commit fails atomically via OCC (SQLSTATE 40001)
- **Preservation**: Single-node/test behaviour with `ShardEpoch::ZERO` continues to skip fencing; the lane OCC retry loop continues to handle `CommitResult::Conflict`; all existing public API signatures and test coverage are preserved through the module split
- **`commit_transition`**: The internal function in `dsql/run_repository.rs` that opens a transaction, checks `transition_seq` via `SELECT ... FOR UPDATE`, writes the mutation, and commits
- **`commit_transition_for_bundle`**: The public fenced entry point that additionally verifies the shard_lease epoch within the same transaction
- **`ShardEpoch::ZERO`**: Sentinel value indicating no fencing required (single-node compose, tests)
- **OCC (SQLSTATE 40001)**: DSQL's optimistic concurrency control rejection — if another transaction modifies a row in this transaction's read set before commit, the commit fails with serialization failure
- **execution-home bundle**: The `ShardId` derived from `(namespace_id, workflow_id, shard_count)` that determines which runtime node owns a workflow's mutations

## Bug Details

### Bug Condition

The bug manifests in three independent failure modes: (1) the TOCTOU race in `commit_transition_for_bundle`, (2) unfenced mutation paths in `runtime.rs`, and (3) incorrect shard routing in the continue-as-new successor timeout path.

**Formal Specification:**
```
FUNCTION isBugCondition(input)
  INPUT: input of type CommitAttempt { path: MutationPath, epoch: ShardEpoch, shard_count_used: u32, actual_shard_count: u32 }
  OUTPUT: boolean

  // Mode 1: TOCTOU race — epoch check and write in separate transactions
  LET toctou_race = input.path == MutationPath::FencedBundle
                    AND input.epoch != ShardEpoch::ZERO
                    AND epoch_check_transaction != write_transaction

  // Mode 2: Unfenced path — mutation bypasses epoch check entirely
  LET unfenced = input.path IN [MutationPath::ActivityStart, MutationPath::ActivityRetry]
                 AND input.epoch == ShardEpoch::ZERO
                 AND deployment_is_controller_managed

  // Mode 3: Incorrect shard routing
  LET wrong_shard = input.shard_count_used != input.actual_shard_count
                    AND input.shard_count_used == 1

  RETURN toctou_race OR unfenced OR wrong_shard
END FUNCTION
```

### Examples

- **TOCTOU race**: Runtime A holds bundle 7 at epoch 3. Runtime B acquires bundle 7 at epoch 4. Runtime A's `commit_transition_for_bundle` checks epoch 3 in transaction T1 (matches durable epoch 3), rolls back T1. Before Runtime A's `commit_transition` opens T2, Runtime B's lease acquisition commits epoch 4. Runtime A's T2 commits the mutation unfenced — state corruption.
- **Unfenced activity start**: Runtime A starts an activity via `start_activity_task`. The commit calls `repo.commit_transition(task.run_key, transition, ShardEpoch::ZERO)` directly, bypassing the bundle fence. If Runtime A has lost its lease, the write succeeds silently.
- **Unfenced activity retry**: Same pattern as activity start but in the `complete_activity_task` retry path.
- **Hard-coded shard count**: A continue-as-new successor with workflow timeouts is routed via `shard_for(new_state.run_key, 1)`, which always produces `ShardId(0)`. With 32 actual shards, the timeout entry is tracked by the wrong shard's scanner and may never fire.

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- When `ShardEpoch::ZERO` is passed (single-node compose, tests), the commit skips the epoch check and writes without fencing — preserving the local development experience
- When the epoch matches the current durable lease, the transition applies successfully and returns `CommitResult::Applied`
- When an OCC conflict occurs due to concurrent writes to the same run (not epoch mismatch), the system returns `CommitResult::Conflict` and the lane retry loop re-attempts
- The in-memory store continues to enforce fencing semantics when a non-zero epoch is provided
- The lane OCC retry loop continues to retry up to `max_occ_retries` before surfacing the error
- All existing public API signatures, trait implementations, and test coverage are preserved through the module split

**Scope:**
All inputs where `epoch == ShardEpoch::ZERO` should be completely unaffected by this fix. This includes:
- Single-node compose deployments without a placement controller
- Unit tests and integration tests that pass `ShardEpoch::ZERO`
- The `InMemoryStore` test backend when epoch is zero

## Hypothesized Root Cause

Based on the code analysis, the root causes are:

1. **Separate-transaction epoch check in `commit_transition_for_bundle`**: The function opens a transaction, reads `shard_lease.epoch`, rolls it back, then calls `self.commit_transition(run_key, transition, ShardEpoch::ZERO)` which opens a *new* transaction for the actual write. The rollback releases the read-set, so DSQL's OCC cannot detect a concurrent epoch change between the two transactions.

2. **`commit_transition` already has epoch fencing logic**: Confusingly, `commit_transition` itself contains a `should_check_epoch` branch that reads `shard_lease` within the write transaction — but `commit_transition_for_bundle` defeats this by passing `ShardEpoch::ZERO` to the inner call. The fix is to pass the real epoch through so the existing in-transaction check activates.

3. **Direct `commit_transition` calls in runtime.rs**: The activity start path (line ~2151) and activity retry path (line ~2300) call `repo.commit_transition(...)` with `ShardEpoch::ZERO` instead of routing through `commit_transition_for_bundle`. These paths were written before the fencing model was complete.

4. **Hard-coded shard count in lane.rs**: At line ~693, the continue-as-new successor timeout tracking uses `shard_for(new_state.run_key, 1)` instead of reading `shard_count` from the `ShardOwner`. This was likely a placeholder that was never updated when multi-shard support landed.

## Correctness Properties

Property 1: Bug Condition - Stale Owner Rejection

_For any_ commit attempt where the caller's epoch does not match the durable `shard_lease.epoch` for the execution-home bundle, the fixed `commit_transition_for_bundle` SHALL reject the commit atomically (returning `CommitResult::Conflict`) without writing any mutation state, because the epoch check and the mutation write execute within the same DSQL transaction.

**Validates: Requirements 2.1, 2.5**

Property 2: Preservation - Zero-Epoch Bypass

_For any_ commit attempt where `epoch == ShardEpoch::ZERO`, the fixed code SHALL produce exactly the same behavior as the original code — skipping the epoch check and committing the transition based solely on the `transition_seq` OCC fence — preserving single-node and test behaviour.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

Property 3: Bug Condition - All Mutation Paths Fenced

_For any_ runtime mutation path (activity start, activity retry, workflow task completion, timer fire, signal, continue-as-new) in a controller-managed deployment, the fixed code SHALL route through `commit_transition_for_bundle` with the caller's current shard epoch, ensuring no unfenced write path exists.

**Validates: Requirements 2.2, 2.3**

Property 4: Bug Condition - Correct Shard Routing

_For any_ continue-as-new successor with workflow timeouts, the fixed code SHALL compute the timeout entry's shard using `shard_for(run_key, owner.shard_count())` where `shard_count` comes from the `ShardOwner`, producing the same shard assignment as all other routing paths.

**Validates: Requirements 2.4**

## Fix Implementation

### Changes Required

Assuming our root cause analysis is correct:

**File**: `crates/tokeira-storage/src/dsql/run_repository.rs`

**Function**: `commit_transition_for_bundle`

**Specific Changes**:
1. **Remove the separate epoch-check transaction**: Delete the block that opens a transaction, reads `shard_lease.epoch`, and rolls back. Instead, pass the real `epoch` through to `self.commit_transition(run_key, transition, epoch)` so the existing in-transaction epoch check in `commit_transition` activates.

2. **Make `commit_transition` module-private**: Change the `RunRepository` trait's `commit_transition` method visibility or add a `#[doc(hidden)]` / deprecation marker. In practice, since it's a trait method, the approach is to make the runtime always call `commit_transition_for_bundle` and treat `commit_transition` as the internal implementation detail. The trait method remains for backward compatibility but callers outside the storage crate should use `commit_transition_for_bundle`.

**File**: `crates/tokeira-runtime/src/runtime.rs`

**Function**: `start_activity_task` (line ~2151)

**Specific Changes**:
3. **Route activity start through fenced commit**: Replace `repo.commit_transition(task.run_key, transition, ShardEpoch::ZERO)` with `repo.commit_transition_for_bundle(task.run_key, execution_home_bundle, transition, epoch)` where `execution_home_bundle` and `epoch` are resolved from the `ShardOwner` for the run's shard.

**Function**: `complete_activity_task` retry path (line ~2300)

**Specific Changes**:
4. **Route activity retry through fenced commit**: Same pattern as activity start — replace the direct `commit_transition` call with `commit_transition_for_bundle` carrying the caller's epoch and bundle.

**File**: `crates/tokeira-runtime/src/lane.rs`

**Function**: continue-as-new successor timeout tracking (line ~693)

**Specific Changes**:
5. **Replace hard-coded shard count**: Change `crate::shard::shard_for(new_state.run_key, 1)` to `crate::shard::shard_for(new_state.run_key, shard_owner.read().unwrap().shard_count())` — the `shard_owner` is already available in the lane's closure scope.

**File**: `crates/tokeira-storage/src/memory.rs`

**Function**: `commit_transition_for_bundle`

**Specific Changes**:
6. **Atomic fencing in memory store**: The in-memory store already checks the epoch under the same lock as the mutation (the `Mutex<StoreState>` is held across both the epoch check and the `commit_transition` call). However, the current implementation drops the lock between the epoch check and the inner `commit_transition` call (because `commit_transition` re-acquires it). Fix: inline the epoch check into the same lock scope as the write, or restructure to hold the lock across both operations.

### Module Splits

**File**: `crates/tokeira-storage/src/dsql/run_repository.rs` → split into:

| Sub-module | Responsibility |
|------------|---------------|
| `commit.rs` | `commit_transition`, `commit_transition_for_bundle`, `write_transition`, epoch fencing |
| `load.rs` | `load_run`, `read_history`, `resolve_execution`, `find_latest_run` |
| `activity.rs` | Activity dispatch table operations, `list_dispatchable_activity_tasks` |
| `timers.rs` | Timer bucket operations, `list_due_timers` |
| `leases.rs` | Bundle lease acquire/renew/relinquish, `LeaseRepository` impl |
| `dispatch.rs` | Dispatch backlog persist/drain |
| `visibility.rs` | Projection log append |
| `mod.rs` | `DsqlRunRepository` struct, constructor, shared helpers, re-exports |

**File**: `crates/tokeira-runtime/src/runtime.rs` → split into:

| Sub-module | Responsibility |
|------------|---------------|
| `workflow_task.rs` | WFT started/completed/failed/timed-out |
| `activity.rs` | Activity start/complete/fail/retry/timeout |
| `query.rs` | Query dispatch and buffered query resolution |
| `timeout.rs` | Workflow/activity/nexus timeout scanning entry points |
| `commit.rs` | Single fenced-commit entry point that all mutation paths call |
| `mod.rs` | `TokeiraRuntime` struct, constructor, public facade, re-exports |

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that demonstrate the bug on unfixed code, then verify the fix works correctly and preserves existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bug BEFORE implementing the fix. Confirm or refute the root cause analysis. If we refute, we will need to re-hypothesize.

**Test Plan**: Write tests that simulate concurrent epoch changes between the check and write transactions, and tests that exercise the unfenced activity paths with a non-zero epoch expectation. Run these tests on the UNFIXED code to observe failures and understand the root cause.

**Test Cases**:
1. **TOCTOU Race Test**: Simulate two runtimes — Runtime A checks epoch in T1 (matches), Runtime B advances epoch, Runtime A commits in T2. On unfixed code, Runtime A's commit succeeds (bug). On fixed code, it returns `Conflict`.
2. **Unfenced Activity Start Test**: Call `start_activity_task` while the shard epoch has advanced. On unfixed code, the commit succeeds with `ShardEpoch::ZERO` (bug). On fixed code, it returns `Conflict`.
3. **Unfenced Activity Retry Test**: Same as above but for the retry path in `complete_activity_task`.
4. **Hard-coded Shard Count Test**: With `shard_count = 32`, commit a continue-as-new successor with workflow timeouts and verify the timeout entry's `shard_id` matches `shard_for(run_key, 32)` not `shard_for(run_key, 1)`.

**Expected Counterexamples**:
- The TOCTOU test will show that the epoch check passes but the subsequent unfenced commit succeeds even after the epoch has advanced
- The unfenced path tests will show that mutations commit without any epoch validation
- The shard count test will show timeout entries routed to `ShardId(0)` regardless of the actual shard assignment

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds, the fixed function produces the expected behavior.

**Pseudocode:**
```
FOR ALL input WHERE isBugCondition(input) DO
  result := commit_transition_for_bundle_fixed(input)
  ASSERT result == CommitResult::Conflict { reason: "stale shard epoch..." }
         OR result == CommitResult::Conflict { reason: "DSQL serialization conflict" }
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold, the fixed function produces the same result as the original function.

**Pseudocode:**
```
FOR ALL input WHERE NOT isBugCondition(input) DO
  ASSERT commit_transition_for_bundle_original(input) == commit_transition_for_bundle_fixed(input)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- It generates many test cases automatically across the input domain
- It catches edge cases that manual unit tests might miss
- It provides strong guarantees that behavior is unchanged for all non-buggy inputs

**Test Plan**: Observe behavior on UNFIXED code first for zero-epoch commits and matching-epoch commits, then write property-based tests capturing that behavior.

**Test Cases**:
1. **Zero-Epoch Preservation**: For any transition with `ShardEpoch::ZERO`, verify the fixed code produces the same `CommitResult` as the original code (no fencing applied)
2. **Matching-Epoch Preservation**: For any transition where the caller's epoch matches the durable lease epoch, verify the commit succeeds with `CommitResult::Applied`
3. **OCC Retry Preservation**: For any transition where `transition_seq` is stale (concurrent write to same run), verify the fixed code still returns `CommitResult::Conflict` and the lane retry loop handles it
4. **In-Memory Store Parity**: For any transition, verify the in-memory store produces the same fencing behavior as the DSQL store (both reject stale epochs, both skip for zero epoch)

### Unit Tests

- Test `commit_transition_for_bundle` with matching epoch → `Applied`
- Test `commit_transition_for_bundle` with stale epoch → `Conflict`
- Test `commit_transition_for_bundle` with `ShardEpoch::ZERO` → skips fence, applies based on `transition_seq`
- Test `commit_transition_for_bundle` with no lease row → `Conflict`
- Test activity start path routes through fenced commit with correct epoch
- Test activity retry path routes through fenced commit with correct epoch
- Test continue-as-new successor timeout uses `shard_owner.shard_count()` not `1`

### Property-Based Tests

- Generate random `(run_key, epoch, durable_epoch)` triples and verify: if `epoch != durable_epoch && epoch != ZERO`, result is `Conflict`; if `epoch == durable_epoch`, result depends only on `transition_seq`; if `epoch == ZERO`, fencing is skipped
- Generate random `(run_key, shard_count)` pairs and verify `shard_for(run_key, shard_count)` is deterministic and bounded by `shard_count` — ensuring the hard-coded `1` fix doesn't regress
- Generate random activity task tokens with various epoch values and verify all paths route through the fenced commit when `controller_managed_placement` is true

### Integration Tests

- Full lane execution with epoch fencing: start workflow, complete WFT, start activity, advance epoch externally, attempt activity completion → verify `Conflict`
- Continue-as-new chain with workflow timeouts: verify timeout entries are tracked by the correct shard scanner across the chain
- Module split verification: run the full existing test suite after the split to confirm no behavioral regression
