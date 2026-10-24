# Implementation Plan

## Overview

First, show the defect with live tests that fail on the current code. Then:
- add the cell and value tables;
- write each image's cells at its version and flip the run's row to it;
- clean up below the version, and match queries to it;
- read values once;
- copy the old index at startup;
- align the specs and documentation, and prove it all on an ephemeral cluster.

## Tasks

- [x] 1. Exploration, before the fix
  - [x] 1.1 Write the design's two exploratory live tests: a first apply and an update, each past 3,000 rows within the
    search-attribute limits
    - _Requirements: 1.1, 1.2_
  - [x] 1.2 Run them on the current code on an ephemeral cluster, and confirm each fails as the design predicts
    - _Requirements: 1.1, 1.2_

- [x] 2. The schema
  - [x] 2.1 Add the migrations for `execution_visibility_attr_cell`, its lookup index,
    `execution_visibility_attr_value` and the copy's progress row, `execution_visibility_attr_copy`, at the next free
    numbers
    - _Requirements: 2.2, 2.5_
  - [x] 2.2 Bring the schema contract, the release's schema target and the bootstrap tests' table list up to date
    - _Requirements: 2.6_

- [x] 3. The apply
  - [x] 3.1 Build the image's cells and its values, each value encoded once
    - _Requirements: 2.5_
  - [x] 3.2 Add the planner: count the cells and values below the version in the first transaction, and choose one
    transaction or three steps within the 2,500-row budget
    - _Requirements: 2.1, 2.3_
  - [x] 3.3 Write the single transaction: cells at the version, the guarded row, the values, the deletes below the
    version and the deltas
    - _Requirements: 2.2, 2.3, 3.1_
  - [x] 3.4 Write the three steps: cell batches outside the stripe lock, the flip under it, and cleanup by key in
    transactions of at most 2,500 rows
    - _Requirements: 2.1, 2.3, 2.4_
  - [x] 3.5 Write cells by statements of at most 128 rows
    - _Requirements: 2.3_
  - [x] 3.6 Take tombstones and the trait's single-attribute writes through the new layout
    - _Requirements: 2.2, 3.4_

- [x] 4. Match every query predicate, and Count grouped by a custom attribute, to the run's current version
  - _Requirements: 2.2, 3.3_

- [x] 5. Read the component hydration and the used-versions seed from the value table at the row's version
  - _Requirements: 2.5, 2.7_

- [x] 6. The copy
  - [x] 6.1 Add the copy: the progress row read `FOR UPDATE`, pages of runs from the stored position, each run's old
    rows at its version and one value per attribute, at most 2,500 rows a transaction with the position written in it,
    completion, and conflicted transactions run again
    - _Requirements: 2.6_
  - [x] 6.2 Call it inside `build_dsql_stack`, before the services that apply images, so both engines copy before
    anything applies an image
    - _Requirements: 2.6_

- [x] 7. Write property tests for Properties 1 to 5
  - **Property 1: Any image within the limits applies**
  - **Property 2: A query matches one image**
  - **Property 3: Cleanup never touches the current image**
  - **Property 4: Values read as before**
  - **Property 5: The copy keeps every answer**
  - **Validates: Requirements 2.1-2.7**

- [x] 8. Write the unit tests listed in the design
  - _Requirements: 2.2-2.6_

- [x] 9. Align the specs and documentation listed in the design
  - _Requirements: 2.5, 2.6_

- [x] 10. Run the live tests, the `EXPLAIN` checks and the negative controls on an ephemeral cluster
  - _Requirements: 2.1-2.7_

- [x] 11. Add a `fixed` fragment saying that a run whose search attributes need thousands of index rows now appears in
  List and Count; and a `changed` fragment saying that the first start after upgrading copies the visibility attribute
  index before serving, which takes longer the more attributes the cluster holds
  - _Requirements: 2.1, 2.6_

- [x] 12. Checkpoint:
  - the exploration tests pass;
  - each negative control fails its test;
  - the live tests pass on an ephemeral cluster;
  - the full bar of root `AGENTS.md` §10.4 passes.

## Notes

- Property tests use `proptest` where their inputs are generated.
- The live runs follow `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- The in-memory store needs no change.
- The old table stays, unread, until a later migration drops it after a full release with the copy's completion
  recorded.
