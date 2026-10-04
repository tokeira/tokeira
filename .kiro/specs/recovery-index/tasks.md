# Implementation Plan: Recovery Index

## Overview

Index the runs that hold Recovery_Work, then move the Sweep and the Sampler onto a paged Candidate listing. Order: shared derivation, schema, storage, runtime. Each step leaves the workspace green; the bar of root `AGENTS.md` §10.4 runs at each checkpoint.

## Tasks

- [ ] 1. Shared derivation
  - [ ] 1.1 Add `crates/tokeira-storage/src/recovery_index.rs` with `recovery_needed`, `RecoveryEntries` and `recovery_entries`, folding the rules of the `collect_*` functions and `dispatchable_workflow_task` without changing them
    - _Requirements: 1.3, 1.4, 4.2, 4.3, 4.4_
  - [ ] 1.2 Unit tests: one per row of the design's derivation table, including a closed run's kept activities (no entries) and a closed run's pending callback (an entry)
    - _Requirements: 4.3, 4.4_
  - [ ] 1.3 Property test: the predicate covers every derived entry
    - **Property 1: The predicate covers every derived entry**
    - **Validates: Requirements 1.3, 7.1, 7.3**

- [ ] 2. Schema
  - [ ] 2.1 Add `V072__workflow_hot_recovery_needed.sql` and `V073__idx_workflow_hot_recovery.sql`
    - _Requirements: 1.1, 2.1_
  - [ ] 2.2 Move the baseline lock, `crates/tokeira-storage/schema-contract.toml`, `crates/tokeira-build-info/schema-contract.toml`, the migration runner's head and the schema-bootstrap tests to V073, as V068 did
    - _Requirements: 1.1, 2.1_

- [ ] 3. Write the flag
  - [ ] 3.1 `insert_workflow_hot` writes `recovery_needed(state)` in its insert and its `ON CONFLICT ... DO UPDATE SET`
    - _Requirements: 1.2, 1.5_
  - [ ] 3.2 Unit test: the statement includes the column and binds the predicate of the state it encodes
    - **Property 4: The stored flag matches the committed state** (DSQL half)
    - **Validates: Requirements 1.2, 7.2**

- [ ] 4. Candidate listing
  - [ ] 4.1 Add `RecoveryCursor`, `RecoveryPage` and `list_recovery_candidates_for_shard` to `RunRepository`, the `Arc` delegate and the edge's history-notifying wrapper
    - _Requirements: 2.2, 2.3_
  - [ ] 4.2 DSQL: the two-phase listing, Legacy then Flagged, with the cursor logic as a pure function over a page-fetch step
    - _Requirements: 2.2, 2.3, 2.4, 2.5, 3.1_
  - [ ] 4.3 Property test: phases end in order and resume strictly after the cursor
    - **Property 3: Phases end in order and resume strictly after the cursor**
    - **Validates: Requirements 2.3, 2.5, 3.1**
  - [ ] 4.4 In-memory store: the shard's runs filtered by the predicate, in run-key order, with the same cursor contract
    - _Requirements: 6.1, 6.2_
  - [ ] 4.5 Property test: paging returns every Candidate exactly once, in order
    - **Property 2: Paging returns every Candidate exactly once, in order**
    - **Validates: Requirements 2.2, 2.3, 2.4, 6.1**
  - [ ] 4.6 Property test: a run is listed exactly when the predicate holds for its committed state
    - **Property 4: The stored flag matches the committed state** (in-memory half)
    - **Validates: Requirements 1.2, 7.2**

- [ ] 5. Checkpoint: storage compiles, lints and passes its tests

- [ ] 6. Runtime
  - [ ] 6.1 `sweep_shard` replaces its six per-kind reads with one loop over Candidate pages of 100 states, deriving entries with `recovery_entries` and inserting them as today
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_
  - [ ] 6.2 The Sampler counts `reconstructible_nexus_deliveries` from Candidate pages of each active shard
    - _Requirements: 5.1, 5.2_
  - [ ] 6.3 Regression: the Sweep's property tests (runtime-sweeper-recovery Properties 5–11) and the Sampler's tests pass unchanged
    - **Property 5: The Sweep's reconstruction is unchanged**
    - **Validates: Requirements 4.2, 4.3, 4.4**

- [ ] 7. Remove the replaced listings
  - [ ] 7.1 Remove the six per-kind shard listings and the reconstructible-delivery listing from the trait, both stores, the delegates and the runtime's test doubles, and fold their `collect_*` helpers into `recovery_index`
    - _Requirements: 4.3, 5.2_

- [ ] 8. Final checkpoint: the full §10.4 bar

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "2.2", "3.1"] },
    { "id": 2, "tasks": ["3.2", "4.1"] },
    { "id": 3, "tasks": ["4.2", "4.4"] },
    { "id": 4, "tasks": ["4.3", "4.5", "4.6"] },
    { "id": 5, "tasks": ["5"] },
    { "id": 6, "tasks": ["6.1", "6.2"] },
    { "id": 7, "tasks": ["6.3", "7.1"] },
    { "id": 8, "tasks": ["8"] }
  ]
}
```

## Notes

- All property tests are required.
- No kernel type and no stored blob changes; the new column is the only schema change besides the index.
- Live DSQL validation (index use for both phases, index build time) is listed in the requirements and is not blocking.
