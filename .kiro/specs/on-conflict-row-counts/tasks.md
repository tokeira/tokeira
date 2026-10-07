# Implementation Plan

## Overview

Show DSQL's miscount and its nine consequences on a DSQL cluster first, with tests that fail on the current code. Then make each operation count the rows its skipping statement returns, keep `rows_affected()` out of every function that runs one with a source check, and prove the result on a DSQL cluster again.

## Tasks

- [ ] 1. Exploration on DSQL, before the fix
  - [ ] 1.1 Write the live probe in `crates/tokeira-storage/tests/dsql_skipping_insert_counts.rs`: for `DO NOTHING` and for a `DO UPDATE … WHERE` whose condition is false, the count sqlx reads from the command tag and the rows `RETURNING` gives back
    - _Requirements: 1.1-1.9, 2.10_
  - [ ] 1.2 Write the live property tests for Properties 1 to 9, each through its store's API over generated starting rows, conflicting ones included
    - **Property 1: A shard lease is decided from what was written**
    - **Property 2: A controller takes only a slot it wrote**
    - **Property 3: A stored action id is a conflict**
    - **Property 4: Provenance answers what is stored**
    - **Property 5: Only the writer of a new configuration is told it applied**
    - **Property 6: A backfill returns what it copied**
    - **Property 7: A CHASM execution commits only with its pointer**
    - **Property 8: The visibility index follows the newest version**
    - **Property 9: The backlog metric counts what was written**
    - **Validates: Requirements 2.1-2.10**
  - [ ] 1.3 Run the probe, the new properties and the existing live suites on a DSQL cluster created for the run, on the current code. Confirm the probe shows the miscount for both clauses, and that the conflicting cases fail as 1.1 to 1.9 predict
    - _Requirements: 1.1-1.9_

- [ ] 2. Storage operations count returned rows
  - [ ] 2.1 Shard lease: the insert and the takeover `UPDATE` return `epoch`; `interpret_acquire` decides from the returned epochs and the rejected row; the read-back `SELECT` goes
    - _Requirements: 2.1, 2.10_
  - [ ] 2.2 Worker compute: the slot insert returns `slot` and the action insert returns `action_id`; `admit_controller` and `commit_decision` decide from them
    - _Requirements: 2.2, 2.3, 2.10_
  - [ ] 2.3 Provenance: the insert returns `token_digest`, and `put` reads the stored row only when none comes back
    - _Requirements: 2.4, 2.10_
  - [ ] 2.4 Task queue configuration: the create's insert and the compare-and-set `UPDATE` return `revision`, and `execute_cas` reports whether a row came back
    - _Requirements: 2.5, 2.10_
  - [ ] 2.5 CHASM: the backfill returns the number of rows its insert returns, and the persist commits only when the pointer's upsert returns `run_id`
    - _Requirements: 2.6, 2.7, 2.10_
  - [ ] 2.6 Backlog: each insert returns `key`, and the rows-written metric counts the returned rows
    - _Requirements: 2.9, 2.10_
  - [ ] 2.7 Write unit tests of `interpret_acquire` for each combination of inserted epoch, updated epoch and rejected row
    - _Requirements: 2.1_

- [ ] 3. Visibility projection: `upsert_execution_row`'s guarded upsert returns `run_key`, and the row is applied exactly when one comes back
  - _Requirements: 2.8, 2.10_

- [ ] 4. Source check
  - [ ] 4.1 Write `crates/tokeira-storage/tests/skipping_insert_counts.rs`: scan the workspace's `src` trees, find each function that runs a skipping statement inline or through a constant, and fail for each one that reads `rows_affected()`
    - _Requirements: 2.10_
  - [ ] 4.2 Write property tests for Property 10 on generated sources
    - **Property 10: No function decides from a skipping statement's command tag**
    - **Validates: Requirement 2.10**

- [ ] 5. List the new live suite and tests in `docs/testing/dsql-live-suites.md`, with the commands that run them serially

- [ ] 6. Checkpoint: the full bar of root `AGENTS.md` §10.4, and the probe, Properties 1 to 9 and the existing live suites green on a DSQL cluster created for the run and deleted after it, recorded with the date, revision and outcome, and when the cluster was created and deleted, without its identifier or endpoint

## Notes

- Property tests use `proptest`, tagged `// Feature: on-conflict-row-counts, Property N: <title>`. The live ones run few cases each, serially.
- A live test that passes with its gate unset is not evidence; only a run against DSQL counts.
- The projection's change is limited to `upsert_execution_row`'s statement and what it returns; its callers keep deciding from `applied` as today.
