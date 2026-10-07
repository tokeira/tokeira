# Implementation Plan

## Overview

Cover the row count of the conditional inserts, and the nine operations' answers that rest on it, with live tests on DSQL.

## Tasks

- [x] 1. Live checks of the row count and the operations
  - [x] 1.1 Write the probe of the shard lease's insert and the CHASM pointer's guarded upsert, in `crates/tokeira-storage/tests/dsql_on_conflict_counts.rs`
    - _Requirements: 1.1, 1.2_
  - [x] 1.2 Write property tests for Properties 1 to 9, each through its operation's store, with conflicting rows among the cases
    - **Property 1: A shard lease is decided from what was written**
    - **Property 2: A controller takes only a slot it wrote**
    - **Property 3: A stored action id is a conflict**
    - **Property 4: Provenance answers what is stored**
    - **Property 5: Only the writer of a new configuration is told it applied**
    - **Property 6: A backfill returns what it copied**
    - **Property 7: A CHASM execution commits only with its pointer**
    - **Property 8: The visibility row follows the newest version**
    - **Property 9: The backlog metric counts what was written**
    - **Validates: Requirements 1.1, 2.1-8.1, 9.1**

- [x] 2. Make the projection crate's `dsql-integration` feature enable the storage crate's, which provides the URL-based store its live tests build
  - _Requirements: 9.2_

- [x] 3. List the new live tests and their commands in `docs/testing/dsql-live-suites.md`
  - _Requirements: 9.3_

- [x] 4. Checkpoint: the full bar of root `AGENTS.md` §10.4, and every live test green on a DSQL cluster created for the run and deleted after it, recorded with the date, revision and outcome, without the cluster's identifier or endpoint
  - DONE 2026-10-08 at `2a4f8768`. The bar passed. On a DSQL cluster created for the run and deleted after it, 24 live tests passed: the probe and Properties 1 to 9, with the other tests of `dsql_shard_leasing`, `dsql_embedded_ownership`, the CHASM store's live tests and `dsql_projection_persistence`. With a fault patched into each operation, each of Properties 1 to 9 failed on DSQL while the probe passed.

## Notes

- Property tests are tagged `// Feature: on-conflict-row-counts, Property N: <title>`. Each live one checks every kind of stored row its property names on every run: every input in a small, finite set, or each kind with values `proptest` generates, unshrunk and unpersisted.
- A live test that passes with its gate unset is not evidence; only a run against DSQL counts.
