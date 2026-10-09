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

- [x] 1. Exploration, before the fix
  - [x] 1.1 Give the in-memory store its model of DSQL's limits for the covered writes, with unit tests at each limit's boundary
    - **Property 9: The in-memory store refuses what DSQL refuses**
    - **Validates: Requirements 1.5, 2.12**
  - [x] 1.2 Write the contract tests, each run on both stores:
    - a pass of 3,001 small expired tasks, and one of eleven tasks whose payloads are about 1 MB each;
    - the deletion of a closed run owning more than 3,000 rows;
    - a materialization whose copied history encodes to more than 1 MiB, one to more than 10 MiB, and one whose successor holds 3,500 timers;
    - a start of the same workflow id between a reset's commit on its base and its successor's materialization;
    - a plain reset of a closed workflow, with no start in between, which must pass before and after the fix.
    - _Requirements: 1.1-1.6, 3.5_
  - [x] 1.3 Run them on the current code, on the in-memory store and on an ephemeral DSQL cluster, and confirm each defect case fails as 1.1 to 1.4 and 1.6 predict, and the plain reset passes
    - _Requirements: 1.1-1.6_
    - DONE. On the code before the fix every defect case failed on both stores, and the plain reset of a closed workflow passed on both. The in-memory model refused the spills at 3,001 rows and at 10,000,280 bytes, the deletion at 3,607 rows, the reset with 3,500 timers at 3,001 rows, and the resets' single batches as values of 2,400,393 and 11,441,053 bytes. On an ephemeral cluster DSQL refused the deletion, the 3,001-entry spill and the 3,500 timer rows at its row limit, the eleven entries of about 1 MB at its size limit and the 2.4 MiB batch at its value limit, and broke the connection for the 11 MiB batch's message. With a start in between, the successor replaced the started run as current on both stores.

- [x] 2. Budgets and the bulk-write record
  - [x] 2.1 Add `write_budget`: the budgets, DSQL's limits and the paging helper
    - _Requirements: 2.1_
  - [x] 2.2 Add the four migrations at the next free numbers (V077 to V080 at the time of writing), with the schema contracts, baseline lock, build information, migration tests and the migration paragraph of `crates/tokeira-storage/AGENTS.md`, as V074 to V076 did
    - _Requirements: 2.3, 2.4, 2.8_
  - [x] 2.3 Add `RunBulkWrite`, `BulkWritePhase` and `list_run_bulk_writes` to both stores, and records to the in-memory store's snapshot
    - _Requirements: 2.3, 2.5, 2.8_

- [x] 3. The backlog spill
  - [x] 3.1 Write `persist_to_backlog`'s entries in pages, and stop at a failed page with `BacklogPersistError`, in both stores
    - _Requirements: 2.1, 2.2, 3.1, 3.2_
  - [x] 3.2 Make `scan_grace_once` re-publish only the tasks from `persisted` on
    - _Requirements: 2.2_
  - [x] 3.3 Write property tests for Property 2, in the stores and through `scan_grace_once`
    - **Property 2: A failed spill keeps what it persisted and re-publishes the rest**
    - **Validates: Requirements 2.2, 3.1, 3.2**

- [x] 4. Deletion and the purge
  - [x] 4.1 Make `delete_run_for_bundle` the first transaction, in both stores, and restate the trait's contract
    - _Requirements: 2.3, 3.3, 3.4_
  - [x] 4.2 Add `abandon_materialization` and `purge_run` to both stores: the switch of a `materializing` record, the tables in order, history last, and the record with the last rows
    - _Requirements: 2.4, 2.5, 2.9_
  - [x] 4.3 Write property tests for Properties 3 and 4
    - **Property 3: A run has mutable state or a bulk-write record, never both**
    - **Property 4: A purge finishes, removes only its run's rows, and removes history last**
    - **Validates: Requirements 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9**

- [x] 5. Materialization and the timer scanner
  - [x] 5.1 Materialize a reset successor in steps, in both stores: the read, the batches, the record, the copy and the final transaction, with History Size as the sum of the batches
    - _Requirements: 2.7, 2.8, 2.11, 3.5_
  - [x] 5.2 Check the current pointer in the final transaction against `expected_current`: the run the pointer names when `reset_workflow` admits the reset, open or closed, read with `find_latest_run` and carried to the lane in `ResetRequest::expected_current_run_key`. Correct `reset_workflow`'s out-of-date comment on `find_latest_run`
    - _Requirements: 1.6, 2.13, 3.5_
  - [x] 5.3 Pass `StaleTimer` to `delete_due_timer_if_matches`, and keep a due timer whose run is missing while that run has mutable state or a `materializing` record
    - _Requirements: 2.10, 3.6_
  - [x] 5.4 Write property tests for Properties 5, 6, 7, 8 and 10
    - **Property 5: A materialized successor is complete, or invisible**
    - **Property 6: An abandoned materialization leaves nothing behind**
    - **Property 7: The copied history is split at event boundaries within the batch budget**
    - **Property 8: The timer scanner keeps a materializing run's timers**
    - **Property 10: A successor replaces only the run the pointer named at admission**
    - **Validates: Requirements 1.6, 2.1, 2.7, 2.8, 2.9, 2.10, 2.11, 2.13, 3.5, 3.6**

- [x] 6. Runtime
  - [x] 6.1 Add the purger, started with the runtime's other loops
    - _Requirements: 2.5, 2.9_
  - [x] 6.2 Make `delete_workflow` return after the first transaction, which it runs with the handoff to the purger in a task the caller's cancellation doesn't stop. Add `DeleteWorkflowRequest::purge_inline`, set by the batch delete and the namespace's reclaim, which purges before returning and hands a failed purge to the purger
    - _Requirements: 2.5, 2.6_
  - [x] 6.3 Hand a failed materialization's successor to the purger from the lane
    - _Requirements: 2.9_
    - DONE: the lane purges the successor itself before it answers, since it runs the materialization and holds no purger handle, and `reset_workflow` then hands the successor to the purger, which retries a purge that failed.
  - [x] 6.4 Make `sweep_shard` switch the shard's `materializing` records before activation, and hand all its records to the purger without waiting
    - _Requirements: 2.5, 2.9_
    - DONE: `recover_bulk_writes` makes the switch and the handoff right after the sweep, before activation, on both of the sweep's paths and on both acquisition paths, checking the acquisition as the dispatch repair does.
  - [x] 6.5 Write the runtime's unit tests: a deletion's return and its purge, an inline purge and one that fails, the sweep's switch and handoff, and a failed reset's purge
    - _Requirements: 2.5, 2.6, 2.9, 2.13_

- [x] 7. Write property tests for Property 1 across the four writes, asserting the budgets
  - **Property 1: Every transaction stays within the budgets**
  - **Validates: Requirements 2.1, 2.2, 2.4, 2.7**

- [x] 8. The live DSQL suite: the contract tests, an interrupted deletion, an abandoned materialization, and DSQL's limits apart from the budgets, with its rows in `docs/testing/dsql-live-suites.md`
  - _Requirements: 2.1-2.9_
  - DONE on ephemeral clusters: the ten bulk-write tests and the six limit probes passed. With six faults patched into the DSQL code at once, each failed its own tests and the plain reset of a closed workflow still passed: a copy that skips the record's check, a deletion without its record, a final transaction without the pointer check, a spill in one transaction, History Size from the first batch, and timer rows left uncopied.

- [x] 9. Align the specs this changes: runtime-durable-backlog criterion 3.7 and its design, temporal-ui-support's deletion design, continue-as-new-advice Requirement 1.6 and Property 1, and run-growth-limits criterion 3.4 and Out of Scope
  - _Requirements: 2.2, 2.3, 2.4, 2.6, 2.11_

- [x] 10. Checkpoint: the exploration tests pass on both stores, each negative control fails its test, the live suite passes on an ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- Property tests use `proptest`, tagged `// Feature: bounded-bulk-writes, Property N: <title>`.
- Tasks 3, 4 and 5 build on 2; task 6 on 4 and 5; tasks 7 and 8 on 3 to 6.
- The contract tests share one body between the stores, so the in-memory store's model and DSQL answer the same cases.
- DSQL's limits were observed on a live cluster. The live suite asserts them apart from the budgets, so a change in DSQL fails a test rather than a purge.
