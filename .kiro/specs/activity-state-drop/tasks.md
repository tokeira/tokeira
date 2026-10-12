# Implementation Plan

## Overview

First show the defect, with tests that fail on the current code. Then:
- add the drop and teach the runner to replay it;
- stop the purge touching the table, and remove the row codec;
- move the schema contract to the drop;
- align the specs and documentation;
- prove it on an ephemeral cluster.

## Tasks

- [x] 1. Exploration, before the fix
  - [x] 1.1 Write the tests:
    - the offline test that the runner proves `DROP TABLE IF EXISTS activity_state;` idempotent;
    - the offline test that the purge's tables leave out `activity_state`;
    - the live test of the design's Testing Strategy.
    - _Requirements: 1.1-1.3_
  - [x] 1.2 Run them on the current code, the live test on an ephemeral cluster, and confirm each fails as 1.1 and 1.2 predict
    - _Requirements: 1.1, 1.2_
    - DONE. On the code before the fix, the runner refused to prove `DROP TABLE IF EXISTS activity_state;` idempotent, and the purge's tables included `activity_state`. On an ephemeral cluster, a schema migrated to the target still held the table and its three indexes.

- [x] 2. The drop
  - [x] 2.1 Add `V083__drop_activity_state.sql`, or the next free number if another migration lands first
    - _Requirements: 2.1_
  - [x] 2.2 Make `migration_is_idempotent` accept `DROP TABLE IF EXISTS`
    - _Requirements: 2.2_

- [x] 3. Remove `activity_state` from the purge's tables
  - _Requirements: 2.3, 3.3_

- [x] 4. Remove the `activity_state` row codec and its tests, and the comments that cite its layout
  - _Requirements: 2.4_

- [x] 5. Move the schema contract to the drop:
  - storage's `schema-contract.toml` and build-info's copy;
  - `schema-baseline.lock`;
  - build-info's contract test and the runner's migration-count test;
  - the migration paragraph in storage's `AGENTS.md`.
  - _Requirements: 2.5_

- [x] 6. Write property tests for Properties 1, 2 and 3
  - **Property 1: Migrating to the target leaves no activity state table**
  - **Property 2: The drop replays as nothing**
  - **Property 3: A run's deletion purges what it owns, and never `activity_state`**
  - **Validates: Requirements 2.1, 2.2, 2.3, 3.1, 3.2, 3.3**

- [x] 7. Write the unit tests listed in the design
  - _Requirements: 2.2, 2.5_

- [x] 8. Align the specs and documentation listed in the design
  - _Requirements: 2.3, 2.4_

- [x] 9. Say "Upgrade with every node stopped." once in the release that carries the drop
  - _Requirements: 2.6_

- [x] 10. Checkpoint: the exploration tests pass, each negative control fails its test, the live test passes on an ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes
  - DONE at `213e076f`. The exploration tests pass. Properties 1 and 2 pass on an ephemeral cluster, through both runner paths, with 4,000 rows written before each drop. Property 3 passes offline, and through the purge's live tests after the drop. Each of the six negative controls failed its tests: four offline and two live. The bar passed (3,870 tests passed, 2 skipped).

## Notes

- Property tests use `proptest` where their inputs are generated, tagged `// Feature: activity-state-drop, Property N: <title>`.
- The live run follows `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- The code lands in a release after the first one whose distributed nodes check their schema at startup ([distributed-schema-startup-check](../distributed-schema-startup-check/bugfix.md)), so that release's nodes refuse the dropped schema.
