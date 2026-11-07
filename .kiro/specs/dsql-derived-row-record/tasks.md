# Implementation Plan

## Overview

The record lands in one change. In order:

1. the schema;
2. the record's encoding and decisions, as pure functions with their property tests;
3. the commit paths;
4. the writers outside a commit;
5. the live proof on a throwaway cluster;
6. the specs and documentation the change affects.

The live statement budget's current pins are the "before" counts. The same test, run on the change, gives the "after".

## Tasks

- [x] 1. The schema
  - [x] 1.1 Add `V133__workflow_hot_derived_kinds.sql` and `V134__workflow_hot_derived_kinds_seq.sql`, each one
    `ALTER TABLE workflow_hot ADD COLUMN IF NOT EXISTS …` statement. Move `schema-contract.toml`, the baseline lock,
    the build information and the migration-count tests to V134.
    - _Requirements: 5.1, 5.2_
  - [x] 1.2 Name V133–V134 and the next schema change, V135, in `crates/tokeira-storage/AGENTS.md`'s migration list
    - _Requirements: 5.1_

- [x] 2. The record
  - [x] 2.1 Add `crates/tokeira-storage/src/dsql/run_repository/derived_record.rs`:
    - `DerivedKinds`, with `after_commit`, `contains` and `to_column`;
    - `read_record`, `may_hold`, `must_forget` and `forget`.
    - _Requirements: 1.4, 1.5, 2.3, 2.4, 2.5, 3.1, 5.2, 5.3_
  - [x] 2.2 Write the property test for Property 4
    - **Property 4: The record reads valid only under a matching stamp**
    - **Validates: Requirements 5.2, 5.3, 5.4**
    - DONE: Properties 1–4 live in `derived_record_tests.rs`, over a model of one run's hot row and derived rows whose
      writers decide through the real record functions and `plan_operations`.

- [x] 3. The commit paths
  - [x] 3.1 Replace the four commit-side hot-row locks with `LOCK_HOT_SQL` and pass the record each reads on as
    `prior_derived`. An absent run passes `None`. The four are `do_commit_guarded`, `check_closing`, the successor
    commit and each publication participant.
    - _Requirements: 1.6, 1.7_
  - [x] 3.2 Add `prior_derived` to `write_transition`, `write_transition_with_intents`, `write_prepared_transition`
    and `write_transition_mode`. Derive the wanted workflow dispatch row and activity dispatch rows once, before the
    hot-row upsert, and write the record through `insert_workflow_hot` and `WORKFLOW_HOT_UPSERT_SQL`.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.6_
  - [x] 3.3 Give `maintain_operations` `may_hold` and make it return `OperationRows { upserted, remain }`. Skip the
    operation read when the kind is ruled out.
    - _Requirements: 1.1, 2.5_
    - DONE: `plan_operations` wraps `close_batch::operation_changes` and computes `remain`; `outbound::maintain`
      returns it to the commit.
  - [x] 3.4 Give `workflow_dispatch::maintain` the wanted row and `may_hold`. Skip the delete when the kind is ruled
    out and no row is wanted.
    - _Requirements: 1.3_
    - DONE: the commit calls `workflow_dispatch::write`; `maintain` derives the row and calls `write` with
      `may_hold = true` for the repair and reset materialization.
  - [x] 3.5 Give `activity_dispatch::maintain` the wanted set and `may_hold`. Skip `existing_page` and upsert every
    wanted row when the kind is ruled out.
    - _Requirements: 1.2_
  - [x] 3.6 Write the property tests for Properties 2 and 3
    - **Property 2: A commit with a record leaves the same rows as one without**
    - **Property 3: Every commit writes an exact, valid record**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.2, 2.3, 2.4, 2.5, 4.2**

- [x] 4. The writers outside a commit
  - [x] 4.1 Workflow dispatch repair: read the record with its hot read, and call `forget` when it upserts and
    `must_forget` holds
    - _Requirements: 3.1, 3.4_
  - [x] 4.2 Activity dispatch repair: read the record with its `FOR UPDATE` hot read, and call `forget` when a page
    upserts and `must_forget` holds
    - _Requirements: 3.1, 3.4_
  - [x] 4.3 Outbound reconciliation: call `forget` when `maintain_operations` upserted
    - _Requirements: 3.1, 3.5_
  - [x] 4.4 The stopped-cluster outbound upgrade: call `forget` in each transaction that writes operation rows
    - _Requirements: 3.1, 3.5, 5.4_
  - [x] 4.5 Reset materialization: create the hot row with an unknown record
    - _Requirements: 3.2_
  - [x] 4.6 Write the property test for Property 1
    - **Property 1: The record is sound under every writer**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.5, 4.1, 5.2, 5.3, 5.4**

- [x] 5. Checkpoint: the storage crate's unit and property tests pass, `cargo clippy -p tokeira-storage --all-targets`
  is clean, and the in-memory store's tests pass unchanged
  - _Requirements: 6.1, 6.2_

- [x] 6. The live proof
  - [x] 6.1 Write the DSQL integration tests of the design's Testing Strategy, each on its own:
    - a commit sequence for each kind;
    - an older-release commit;
    - the two repairs under a record that rules their kind out;
    - reset materialization and staged publication.
    - _Requirements: 1.1–1.7, 2.1–2.6, 3.1–3.5, 4.2, 5.3, 6.3_
    - DONE: `derived_record_live_tests.rs` holds the first three and the reset successor. The staged publication
      check is in `outbound_live_successor_reservation_and_interrupted_publication`, through
      `assert_exact_record`.
  - [x] 6.2 Pin the live statement budget's new counts, and assert each step's skipped statements are absent from its
    commit
    - **Property 5: A ruled-out kind costs no statement**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.6, 2.1, 6.3, 7.3**
    - DONE: each step's first commit is checked against what its prior state wants, and every kind must be ruled
      out somewhere in the lifecycle.
  - [x] 6.3 Run 6.1, 6.2 and the existing activity dispatch, workflow dispatch, outbound, bulk-write and large-value
    live suites on a throwaway cluster, deleted afterwards. Record the before and after counts here.
    - _Requirements: 6.4, 6.5, 7.1, 7.2_
    - DONE: every named suite passed on throwaway clusters, each deleted afterwards. The workflow dispatch suite's
      stale-row seeding now forgets the record, as every writer outside a commit must. The lifecycle fell from 175
      round trips to 142, as the design predicted; the heartbeat alone went from 9 to 6. Commit round trips per step
      (BEGIN and statements):

      | Step | Before | After |
      |---|---|---|
      | start | 17 | 17 |
      | workflow-task-start | 10 | 8 |
      | workflow-task-complete/schedule-two-activities | 12 | 9 |
      | activity-start | 11 | 9 |
      | activity-start/second | 10 | 8 |
      | activity-heartbeat | 9 | 6 |
      | activity-complete | 10 | 8 |
      | activity-fail/retry | 10 | 8 |
      | activity-start/retry | 10 | 9 |
      | activity-complete/second | 10 | 8 |
      | signal | 12 | 10 |
      | workflow-task-start/second | 10 | 8 |
      | workflow-task-complete/start-timer | 11 | 8 |
      | timer-fire+workflow-task-start | 11 + 10 | 9 + 8 |
      | workflow-task-complete/close | 12 | 9 |
      | Lifecycle | 175 | 142 |

- [x] 7. The specs and documentation
  - [x] 7.1 Update the activity-dispatch and workflow-dispatch designs and `docs/architecture/050-dsql-storage.md` as
    the design's "Specs and Documentation This Changes" lists. Add the changelog fragment.
    - _Requirements: 4.3_

- [x] 8. Checkpoint: the §10.4 bar passes
  - _Requirements: 6.1–6.5_
  - DONE: format, lint, check, tests, doctests and documentation all pass.

## Task Dependency Graph

```text
1.1 → 2.1 → 3.1 → 3.2 → {3.3, 3.4, 3.5} → 3.6
2.1 → 2.2
3.3 → 4.3, 4.4
3.4 → 4.1
3.5 → 4.2
3.2 → 4.5
{4.1–4.5} → 4.6 → 5 → 6.1 → 6.2 → 6.3 → 7.1 → 8
1.2 → 8
```

## Notes

- Property tests use `proptest` and are tagged `// Feature: dsql-derived-row-record, Property N: <title>`.
- Live runs follow `docs/testing/dsql-live-suites.md`, on a throwaway cluster deleted when the run ends.
- The migrations take V133 and V134 unless another change takes them first. They take the next free numbers when the
  change is written.
