# Implementation Plan

## Overview

Show the four defects first, on both stores, with tests that fail on the current code. Then:
- add the budgets and the bulk-write record;
- page the spill;
- split a deletion into its first transaction and a purge;
- materialize a reset successor in pages behind its record, and keep its timers in the scanner;
- have the runtime finish every purge;
- prove the limits on a live cluster.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Give the in-memory store its model of DSQL's limits for the covered writes, with unit tests at each limit's boundary
    - **Property 9: The in-memory store refuses what DSQL refuses**
    - **Validates: Requirements 1.5, 2.12**
  - [ ] 1.2 Write the contract tests, each run on both stores:
    - a pass of 3,001 small expired tasks, and one of eleven tasks whose payloads are about 1 MB each;
    - the deletion of a closed run owning more than 3,000 rows;
    - a materialization whose copied history encodes to more than 1 MiB, one to more than 10 MiB, and one whose successor holds 3,500 timers.
    - _Requirements: 1.1-1.5_
  - [ ] 1.3 Run them on the current code, on the in-memory store and on an ephemeral DSQL cluster, and confirm each fails as 1.1 to 1.4 predict
    - _Requirements: 1.1-1.5_

- [ ] 2. Budgets and the bulk-write record
  - [ ] 2.1 Add `write_budget`: the budgets, DSQL's limits and the paging helper
    - _Requirements: 2.1_
  - [ ] 2.2 Add migrations V077 to V080, with the schema contracts, baseline lock, build information, migration tests and the migration paragraph of `crates/tokeira-storage/AGENTS.md`, as V074 to V076 did
    - _Requirements: 2.3, 2.4, 2.8_
  - [ ] 2.3 Add `RunBulkWrite`, `BulkWritePhase` and `list_run_bulk_writes` to both stores, and records to the in-memory store's snapshot
    - _Requirements: 2.3, 2.5, 2.8_

- [ ] 3. The backlog spill
  - [ ] 3.1 Write `persist_to_backlog`'s entries in pages, and stop at a failed page with `BacklogPersistError`, in both stores
    - _Requirements: 2.1, 2.2, 3.1, 3.2_
  - [ ] 3.2 Make `scan_grace_once` re-publish only the tasks from `persisted` on
    - _Requirements: 2.2_
  - [ ] 3.3 Write property tests for Property 2, in the stores and through `scan_grace_once`
    - **Property 2: A failed spill keeps what it persisted and re-publishes the rest**
    - **Validates: Requirements 2.2, 3.1, 3.2**

- [ ] 4. Deletion and the purge
  - [ ] 4.1 Make `delete_run_for_bundle` the first transaction, in both stores, and restate the trait's contract
    - _Requirements: 2.3, 3.3, 3.4_
  - [ ] 4.2 Add `purge_run` to both stores: the switch of a `materializing` record, the tables in order, history last, and the record with the last rows
    - _Requirements: 2.4, 2.5, 2.9_
  - [ ] 4.3 Write property tests for Properties 3 and 4
    - **Property 3: A run has mutable state or a bulk-write record, never both**
    - **Property 4: A purge finishes, removes only its run's rows, and removes history last**
    - **Validates: Requirements 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9**

- [ ] 5. Materialization and the timer scanner
  - [ ] 5.1 Materialize a reset successor in steps, in both stores: the read, the batches, the record, the copy and the final transaction, with History Size as the sum of the batches
    - _Requirements: 2.7, 2.8, 2.11, 3.5_
  - [ ] 5.2 Pass `StaleTimer` to `delete_due_timer_if_matches`, and keep a due timer whose run is missing while that run has mutable state or a `materializing` record
    - _Requirements: 2.10, 3.6_
  - [ ] 5.3 Write property tests for Properties 5, 6, 7 and 8
    - **Property 5: A materialized successor is complete, or invisible**
    - **Property 6: An abandoned materialization leaves nothing behind**
    - **Property 7: The copied history is split at event boundaries within the batch budget**
    - **Property 8: The timer scanner keeps a materializing run's timers**
    - **Validates: Requirements 2.1, 2.7, 2.8, 2.9, 2.10, 2.11, 3.5, 3.6**

- [ ] 6. Runtime
  - [ ] 6.1 Add the purger, started with the runtime's other loops
    - _Requirements: 2.5, 2.9_
  - [ ] 6.2 Make `delete_workflow` purge in a task the caller's cancellation doesn't stop, and hand a failed purge to the purger
    - _Requirements: 2.5, 2.6_
  - [ ] 6.3 Hand a failed materialization's successor to the purger from the lane
    - _Requirements: 2.9_
  - [ ] 6.4 Hand a shard's records to the purger from `sweep_shard`
    - _Requirements: 2.5, 2.9_
  - [ ] 6.5 Write the runtime's unit tests: a deletion whose purge fails, the sweep's handoff, and a failed reset's purge
    - _Requirements: 2.5, 2.6, 2.9_

- [ ] 7. Write property tests for Property 1 across the four writes, asserting the budgets
  - **Property 1: Every transaction stays within the budgets**
  - **Validates: Requirements 2.1, 2.2, 2.4, 2.7**

- [ ] 8. The live DSQL suite: the contract tests, an interrupted deletion, an abandoned materialization, and DSQL's limits apart from the budgets, with its rows in `docs/testing/dsql-live-suites.md`
  - _Requirements: 2.1-2.9_

- [ ] 9. Align the specs this changes: runtime-durable-backlog criterion 3.7 and its design, temporal-ui-support's deletion design, continue-as-new-advice Requirement 1.6 and Property 1, and run-growth-limits criterion 3.4 and Out of Scope
  - _Requirements: 2.2, 2.3, 2.4, 2.11_

- [ ] 10. Checkpoint: the exploration tests pass on both stores, each negative control fails its test, the live suite passes on an ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- Property tests use `proptest`, tagged `// Feature: bounded-bulk-writes, Property N: <title>`.
- Tasks 3, 4 and 5 build on 2; task 6 on 4 and 5; tasks 7 and 8 on 3 to 6.
- The contract tests share one body between the stores, so the in-memory store's model and DSQL answer the same cases.
- DSQL's limits were observed on 2026-10-08. The live suite asserts them apart from the budgets, so a change in DSQL fails a test rather than a purge.
