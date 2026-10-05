# Implementation Plan

## Overview

Stop writing the activity state copy that nothing reads, on DSQL and in memory, while keeping dispatch rows, run deletion's cleanup and the snapshot layout.

## Tasks

- [x] 1. DSQL repository
  - [x] 1.1 In `commit_transition`, stop upserting `activity_state` for `ActivityOp::Upsert` and deleting it for `ActivityOp::Delete`, keeping the `activity_dispatch` writes
    - _Requirements: 2.1, 3.1_
  - [x] 1.2 In `materialize_reset_successor`, stop inserting `activity_state` rows
    - _Requirements: 2.2_
  - [x] 1.3 Remove `upsert_activity` and its SQL, and keep run deletion's `activity_state` delete
    - _Requirements: 3.2_
  - [x] 1.4 Keep the unit test that run deletion still deletes the run's `activity_state` rows: `authoritative_delete_covers_every_run_owned_table_and_history_is_last` already asserts it
    - _Requirements: 3.2_

- [x] 2. In-memory store
  - [x] 2.1 Remove `activity_state_table` from the store's state, with its upkeep on commit, reset and run deletion
    - _Requirements: 2.1, 2.2_
  - [x] 2.2 Write the snapshot's activity state table empty, and discard the entries a restored snapshot holds
    - _Requirements: 2.3, 3.4_
  - [x] 2.3 Write property tests for Properties 1 and 2
    - **Property 1: No activity state outside run state**
    - **Property 2: Snapshots from earlier releases restore**
    - **Validates: Requirements 2.1, 2.2, 2.3, 3.1, 3.4**

- [x] 3. Correct the comment in `crates/tokeira-storage/src/dsql/codec.rs` and the passage in `docs/architecture/010-history-as-authority.md` that call `activity_state` the timeout sweep's source, and the passages in architecture docs 040, 050 and 090 that present it as live state

- [x] 4. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: activity-state-writes, Property N: <title>`.
- Tests that assert on `activity_state_table` (`storage-memory-fidelity` Requirement 6.7) go with it.
