# Implementation Plan

## Overview

The fix lands in four steps, each a change of its own, in this order: a run's values, visibility, CHASM nodes and
worker invocation requests. Each step first shows the defect, with live tests that fail on the code before it. Then it:
- stores its values in parts;
- proves them on both stores and on an ephemeral cluster;
- aligns the specs and documentation it affects.

## Tasks

- [x] 1. The format
  - [x] 1.1 Add `crates/tokeira-storage/src/dsql/value_parts.rs`: the part size, splitting and joining
    - _Requirements: 2.1, 2.3_
  - [x] 1.2 Write property tests for Properties 1 and 2
    - **Property 1: Splitting and joining a value gives the value back**
    - **Property 2: A join refuses parts that don't fit together**
    - **Validates: Requirements 2.1, 2.3**

- [x] 2. Step 1: a run's values
  - [x] 2.1 Exploration: write step 1's live tests of the design's Exploratory Bug Condition Checking, run them on an
    ephemeral cluster on the code before the step, and confirm each fails with SQLSTATE 54000
    - _Requirements: 1.1, 1.2_
    - DONE. On the code before the step, on an ephemeral cluster, both tests failed: DSQL refused the commit of a
      history batch over 1 MiB, and the commit of a state over 1 MiB, each with its value limit ("datatype limit
      greater than 1048576 bytes not supported for bytea", SQLSTATE 54000).
  - [x] 2.2 Add `run_value_part` and the three count columns, with the migrations' bookkeeping
    - _Requirements: 2.1_
  - [x] 2.3 Make the commit write a split state and split batches, and remove the parts a state no longer uses
    - _Requirements: 2.1, 2.2, 3.1_
  - [x] 2.4 Make `load_run`, the other readers of a run's state and the history reads join split values, each in the
    transaction that reads the value's row
    - _Requirements: 2.3, 2.4, 3.4, 3.6_
  - [x] 2.5 Make a reset read, copy and write split values
    - _Requirements: 2.5, 3.3_
  - [x] 2.6 Add `run_value_part` to the purge's tables after history, as the last, and remove the record with its last
    page
    - _Requirements: 2.6, 3.6_
  - [x] 2.7 Write the tests of Properties 3–6 for a run's values, on both stores, and the live tests of the design
    - **Property 3: A stored value reads back whole**
    - **Property 4: A replaced value leaves only its own parts**
    - **Property 5: A reset copies values over the part size**
    - **Property 6: Removing a value removes its parts**
    - **Validates: Requirements 2.1-2.7, 3.1, 3.3, 3.6**
  - [x] 2.8 Add the release note
    - _Requirements: 2.8_
  - [x] 2.9 Align the specs and documentation the design lists for step 1
  - [x] 2.10 Checkpoint: the exploration tests pass, each negative control fails its test, the live tests pass on an
    ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes
    - DONE at `b8c3752c`. Properties 1 and 2 pass offline, and the contracts of Properties 3, 5 and 6 pass on the
      in-memory store. On ephemeral clusters, the nine large-value tests pass, the exploration tests among them, and so
      do the complete-repair and fenced-repair suites. On the same change before the latest merges to main, the
      bulk-write, history-event, projection-accumulator, placement, activity-input, transaction-limit,
      workflow-dispatch and lease-fence suites passed, and each of the eight negative controls failed its test. The
      bar passed (3,914 tests passed, 2 skipped).

- [ ] 3. Step 2: visibility, built on [projection-passes](../projection-passes/design.md)'s projector
  - [ ] 3.1 Exploration: write step 2's live tests and confirm each fails on the code before the step
    - _Requirements: 1.3_
  - [ ] 3.2 Add `projection_image_part`, `visibility_memo_part` and the two count columns, with the migrations'
    bookkeeping
    - _Requirements: 2.1_
  - [ ] 3.3 Write, read and delete an image's parts with the image, `load_run`'s accumulator seed included
    - _Requirements: 2.1-2.3, 2.6_
  - [ ] 3.4 Write and read a visibility row's memo in parts
    - _Requirements: 2.1-2.3_
  - [ ] 3.5 Write the tests of Properties 3, 4 and 6 for visibility
    - _Requirements: 2.1-2.4, 2.6, 2.7_
  - [ ] 3.6 Checkpoint, as 2.10

- [x] 4. Step 3: CHASM nodes
  - [x] 4.1 Exploration: write step 3's live test and confirm it fails on the code before the step
    - _Requirements: 1.4_
    - DONE. On the code before the step, on an ephemeral cluster, DSQL refused the start of an execution whose root
      node's data was over 1 MiB with its value limit ("datatype limit greater than 1048576 bytes not supported for
      bytea", SQLSTATE 54000).
  - [x] 4.2 Add `chasm_node_part` and `chasm_node.data_parts`, with the migrations' bookkeeping
    - _Requirements: 2.1_
  - [x] 4.3 Write, delete and read a node's data in parts
    - _Requirements: 2.1-2.3, 2.6_
  - [x] 4.4 Write the tests of Properties 3, 4 and 6 for CHASM nodes
    - _Requirements: 2.1-2.4, 2.6, 2.7_
  - [x] 4.5 Checkpoint, as 2.10
    - DONE with 5.5.

- [x] 5. Step 4: worker invocation requests
  - [x] 5.1 Exploration: write step 4's live test and confirm it fails on the code before the step
    - _Requirements: 1.5_
    - DONE. On the code before the step, on an ephemeral cluster, DSQL refused the decision that inserted an action
      with a 2 MiB request with the same value limit.
  - [x] 5.2 Add `worker_compute_action_part` and `worker_compute_action.request_parts`, with the migrations'
    bookkeeping
    - _Requirements: 2.1_
  - [x] 5.3 Write and read a request in parts
    - _Requirements: 2.1, 2.3_
  - [x] 5.4 Write the tests of Property 3 for requests
    - _Requirements: 2.1, 2.3, 2.4, 2.7_
  - [x] 5.5 Checkpoint, as 2.10
    - DONE at `38076002`. Property 3's contracts for node data and requests pass on the in-memory stores. On ephemeral
      clusters, the five large-value tests of node data and requests pass, the exploration tests among them, and so
      do the nine of a run's values, the CHASM store's live tests and the on-conflict suite. Each of seven negative
      controls failed its test: a node's first part written without its other parts, a shrinking node that keeps its
      old parts, a deletion that keeps an execution's parts, a load that joins parts outside its snapshot, a root scan
      that ignores parts, a request's first part written without its other parts, and a read of an action that
      doesn't join its request's parts. The bar passed (3,936 tests passed, 2 skipped).

## Notes

- Property tests use `proptest` where their inputs are generated, tagged
  `// Feature: large-value-parts, Property N: <title>`.
- Live runs follow `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- Each step's migrations take the next free numbers when its change is written.
