# Implementation Plan

## Overview

First show each defect, with tests that fail on the current code. Then:
- refuse an update request for a paused, failing or closing workflow, before the lookup;
- keep a run's completed update ids, and fail a workflow task whose message names one of them;
- recover the ids of a run written before this change when it loads;
- prove it on both stores and an ephemeral cluster, and align the specs.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Write the runtime tests of the design's Exploratory Bug Condition Checking, on the in-memory store
    - _Requirements: 1.1-1.7_
  - [ ] 1.2 Run them on the current code and confirm each gives today's answer, as the design predicts
    - _Requirements: 1.1-1.7_

- [ ] 2. The refusals
  - [ ] 2.1 Refuse a failing run in `apply_update` with `Reject::UpdateWorkflowTaskFailing`, after the paused check and
    before the duplicate check and the limits
    - _Requirements: 2.3_
  - [ ] 2.2 In `update_workflow`, read the run's state once and refuse a running run that is paused, failing or closing,
    in that order, before the lookup and before the registry takes an entry. Map the kernel's paused and failing
    refusals to the same errors
    - _Requirements: 2.1-2.5, 3.1, 3.2_
  - [ ] 2.3 Add `UpdatePaused` and the update message of `WorkflowNotReady`, and map `UpdatePaused` at the edge to
    `FAILED_PRECONDITION` with v1.31.0's message
    - _Requirements: 2.2, 2.3_

- [ ] 3. Completed updates
  - [ ] 3.1 Add `completed_updates` to `WorkflowState` and the pure `completed_update_ids`. Add the id in each arm
    that writes or replays a completion
    - _Requirements: 2.6_
  - [ ] 3.2 In the message handler, look a message's update up in flight, then completed, then as today. Refuse every
    message type for a completed update with v1.31.0's invalid state transition message
    - _Requirements: 2.7, 3.4_
  - [ ] 3.3 Add `COMPLETED_UPDATES_SECTION` (5) to the state extension: written only when the set isn't empty, its ids
    strictly ascending
    - _Requirements: 2.6, 3.6_
  - [ ] 3.4 Recover the ids of an open run whose state has none and whose count is above zero when it loads: on DSQL in
    the load's read-only snapshot, with the history's parts; in memory under its lock
    - _Requirements: 2.8_

- [ ] 4. Write property tests for Properties 1 to 5
  - **Property 1: The refusals come first, in order**
  - **Property 2: A refused request leaves no trace**
  - **Property 3: The completed ids are the history's**
  - **Property 4: A message for a completed update fails the task**
  - **Property 5: A run from before this change recovers its ids**
  - **Validates: Requirements 2.1-2.8, 3.1, 3.4-3.6**

- [ ] 5. Write the unit tests and the live tests listed in the design
  - _Requirements: 2.2, 2.3, 2.6, 2.8_

- [ ] 6. Add the release note
  - _Requirements: 2.1-2.7_

- [ ] 7. Align the specs the design lists

- [ ] 8. Checkpoint: the exploration tests pass, each negative control fails its test, the live tests pass on an
  ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- Property tests use `proptest` where their inputs are generated, tagged
  `// Feature: update-refusals, Property N: <title>`.
- Live runs follow `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- The release already upgrades with every node stopped, and its notes say so.
