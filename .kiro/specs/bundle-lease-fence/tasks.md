# Implementation Plan

## Overview

Show the stale writes first, live, with tests that fail on the current code. Then:
- add the index that makes the epoch a key column;
- make the lease repository write the epoch only on a change of ownership;
- fence every write of a run's authoritative state;
- make the runtime build the fence, treat its loss as final, and stop at its own expiry;
- prove the properties on DSQL and on both stores.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Add a test-only pause point to the DSQL repository that holds a write after its lease check and before its
    commit, behind `dsql-integration`
    - _Requirements: 1.1, 1.2_
  - [ ] 1.2 Write the live tests: a takeover commits while a write is paused, through `commit_transition_for_bundle`,
    `commit_transition` with an epoch, and a deletion's first transaction
    - _Requirements: 1.1, 1.2, 1.3_
  - [ ] 1.3 Run them on the current code on an ephemeral DSQL cluster, and confirm each stale write commits
    - _Requirements: 1.1, 1.2, 1.3_

- [ ] 2. The index
  - [ ] 2.1 Add the migration at the next free number: the unique index on `shard_lease (shard_id, epoch)`, `ASYNC`,
    with the schema contracts, baseline lock, build information, migration tests and the migration paragraph of
    `crates/tokeira-storage/AGENTS.md`
    - _Requirements: 1.3, 2.1_

- [ ] 3. The lease repository
  - [ ] 3.1 Split acquisition into the insert, the refresh that leaves the epoch, and the takeover, in both stores
    - _Requirements: 1.5, 2.3, 3.3_
  - [ ] 3.2 Make renewal one conditional update without `FOR UPDATE`, in both stores
    - _Requirements: 1.4, 2.3, 3.3_
  - [ ] 3.3 Write property tests for Property 5, with checks of the DSQL statements' text
    - **Property 5: Only a change of ownership writes the epoch**
    - **Validates: Requirements 1.4, 1.5, 2.3, 3.3**

- [ ] 4. The fence
  - [ ] 4.1 Add `ExecutionHomeFence`, `WriteFence` and `LeaseFenceLost`, and pass a `WriteFence` to every fenced write
    method in place of the bare epoch
    - _Requirements: 2.2, 2.4, 2.7_
  - [ ] 4.2 Fence `commit_transition`, `commit_transition_for_bundle` (removing its separate check) and
    `delete_run_for_bundle` on DSQL with the fence read, and in memory under the store's lock
    - _Requirements: 1.1, 1.2, 2.2, 2.4_
  - [ ] 4.3 Fence every transaction of `materialize_reset_successor`, and `abandon_materialization` when a recovery
    passes its fence, in both stores
    - _Requirements: 1.6, 2.2_

- [ ] 5. The runtime
  - [ ] 5.1 Build the fence from the ownership state that admitted each write: the lane, the activity paths that commit
    directly, `delete_workflow`, a reset's materialization, and the recovery switch. Add a test that audits every caller
    of the fenced write methods for `Unfenced` under controller-managed placement
    - _Requirements: 2.2, 2.7, 3.1_
  - [ ] 5.2 Map `LeaseFenceLost` to `NotShardOwner` without a lane retry, and cancel the home's acquisition
    - _Requirements: 1.7, 2.4, 2.5_
  - [ ] 5.3 Set the local deadline at acquisition and move it on each committed renewal under controller-managed
    placement, whether or not dispatch reconciliation is enabled
    - _Requirements: 1.8, 2.6_
  - [ ] 5.4 Write the runtime tests for Properties 3 and 4, with an injected clock and no sleeps
    - **Property 3: A lapsed lease is taken over**
    - **Property 4: A lost fence is final**
    - **Validates: Requirements 1.7, 1.8, 2.4, 2.5, 2.6**

- [ ] 6. Write the live property tests for Properties 1 and 2: at least 100 generated interleavings of fenced writes,
  renewals, refreshes, takeovers and releases on DSQL, driven by the pause points and barriers
  - **Property 1: No stale owner commits**
  - **Property 2: The owner's writes never abort each other**
  - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2, 2.3, 3.2**

- [ ] 7. The live DSQL suite: the exploration cases, a stopped materialization refused after a takeover, a recovery
  switch under a lost lease, the owner's renewals and refreshes beside its writes, and a write that lands first and
  aborts a takeover, with its rows in `docs/testing/dsql-live-suites.md`
  - _Requirements: 2.1-2.6_

- [ ] 8. Align the specs this changes: dsql-shard-leasing's design, commit-fencing-correctness's Property 1 and
  criterion 2.1, dsql-core-persistence's epoch validation, bounded-bulk-writes' fenced materialization and recovery
  switch, and workflow-dispatch's `ExecutionHomeFence`
  - _Requirements: 2.2, 2.3_

- [ ] 9. Checkpoint: the exploration tests pass, each negative control fails its test, the live suite passes on an
  ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- Property tests use `proptest`, tagged `// Feature: bundle-lease-fence, Property N: <title>`.
- Tasks 3 and 4 build on 2; task 5 on 4; tasks 6 and 7 on 3 to 5.
- The pause points compile only under `dsql-integration` and change no production path.
- The model is `spec/tla/30_bundle_lease.tla`. Its configurations and negative controls stay as they are; this change
  makes the code match the protocol it checks.
