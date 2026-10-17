# Implementation Plan

## Overview

The fix lands in four steps, each a change of its own, in this order: a run's values, visibility, CHASM nodes and
worker invocation requests. Each step first shows the defect, with live tests that fail on the code before it. Then it:
- stores its values in parts;
- proves them on both stores and on an ephemeral cluster;
- aligns the specs and documentation it affects.

## Tasks

- [ ] 1. The format
  - [ ] 1.1 Add `crates/tokeira-storage/src/dsql/value_parts.rs`: the part size, splitting and joining
    - _Requirements: 2.1, 2.3_
  - [ ] 1.2 Write property tests for Properties 1 and 2
    - **Property 1: Splitting and joining a value gives the value back**
    - **Property 2: A join refuses parts that don't fit together**
    - **Validates: Requirements 2.1, 2.3**

- [ ] 2. Step 1: a run's values
  - [ ] 2.1 Exploration: write step 1's live tests of the design's Exploratory Bug Condition Checking, run them on an
    ephemeral cluster on the code before the step, and confirm each fails with SQLSTATE 54000
    - _Requirements: 1.1, 1.2_
  - [ ] 2.2 Add `run_value_part` and the three count columns, with the migrations' bookkeeping
    - _Requirements: 2.1_
  - [ ] 2.3 Make the commit write a split state and split batches, and remove the parts a state no longer uses
    - _Requirements: 2.1, 2.2, 3.1_
  - [ ] 2.4 Make `load_run`, the other readers of a run's state and the history reads join split values
    - _Requirements: 2.3, 2.4, 3.4_
  - [ ] 2.5 Make a reset read, copy and write split values
    - _Requirements: 2.5, 3.3_
  - [ ] 2.6 Add `run_value_part` to the purge's tables, before history
    - _Requirements: 2.6_
  - [ ] 2.7 Write the tests of Properties 3–6 for a run's values, on both stores, and the live tests of the design
    - **Property 3: A stored value reads back whole**
    - **Property 4: A replaced value leaves only its own parts**
    - **Property 5: A reset copies values over the part size**
    - **Property 6: Removing a value removes its parts**
    - **Validates: Requirements 2.1-2.7, 3.1, 3.3**
  - [ ] 2.8 Add the release note
    - _Requirements: 2.8_
  - [ ] 2.9 Align the specs and documentation the design lists for step 1
  - [ ] 2.10 Checkpoint: the exploration tests pass, each negative control fails its test, the live tests pass on an
    ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

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

- [ ] 4. Step 3: CHASM nodes
  - [ ] 4.1 Exploration: write step 3's live test and confirm it fails on the code before the step
    - _Requirements: 1.4_
  - [ ] 4.2 Add `chasm_node_part` and `chasm_node.data_parts`, with the migrations' bookkeeping
    - _Requirements: 2.1_
  - [ ] 4.3 Write, delete and read a node's data in parts
    - _Requirements: 2.1-2.3, 2.6_
  - [ ] 4.4 Write the tests of Properties 3, 4 and 6 for CHASM nodes
    - _Requirements: 2.1-2.4, 2.6, 2.7_
  - [ ] 4.5 Checkpoint, as 2.10

- [ ] 5. Step 4: worker invocation requests
  - [ ] 5.1 Exploration: write step 4's live test and confirm it fails on the code before the step
    - _Requirements: 1.5_
  - [ ] 5.2 Add `worker_compute_action_part` and `worker_compute_action.request_parts`, with the migrations'
    bookkeeping
    - _Requirements: 2.1_
  - [ ] 5.3 Write and read a request in parts
    - _Requirements: 2.1, 2.3_
  - [ ] 5.4 Write the tests of Property 3 for requests
    - _Requirements: 2.1, 2.3, 2.4, 2.7_
  - [ ] 5.5 Checkpoint, as 2.10

## Notes

- Property tests use `proptest` where their inputs are generated, tagged
  `// Feature: large-value-parts, Property N: <title>`.
- Live runs follow `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- Each step's migrations take the next free numbers when its change is written.
