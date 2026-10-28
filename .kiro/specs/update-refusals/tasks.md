# Implementation Plan

## Overview

First show each defect, with tests that fail on the current code. Then:
- refuse an update request for a paused, failing or closing workflow, before the lookup;
- keep a run's completed update ids, and fail a workflow task whose message names one of them;
- recover the ids of a run written before this change when it loads;
- prove it on both stores and an ephemeral cluster, and align the specs.

## Tasks

- [x] 1. Exploration, before the fix
  - DONE: The seven exploration cases gave the predicted answers on the code before the fix: 7 of 7.
  - [x] 1.1 Write the runtime tests of the design's Exploratory Bug Condition Checking, on the in-memory store
    - _Requirements: 1.1-1.7_
    - DONE: The seven in-memory exploration tests cover every case in the design.
  - [x] 1.2 Run them on the current code and confirm each gives today's answer, as the design predicts
    - _Requirements: 1.1-1.7_
    - DONE: Before the fix, 7 of 7 exploration tests gave the predicted answers.

- [x] 2. The refusals
  - DONE: Kernel, runtime and edge tests pass, including the update-with-start and task-closure review regressions.
  - [x] 2.1 Refuse a failing run in `apply_update` with `Reject::UpdateWorkflowTaskFailing`, after the paused check and
    before the duplicate check and the limits
    - _Requirements: 2.3_
    - DONE: The attempt-2/attempt-3 boundary test and the lane-refusal regression pass.
  - [x] 2.2 In `update_workflow`, read the run's state once and refuse a running run that is paused, failing or closing,
    in that order, before the lookup and before the registry takes an entry. Map the kernel's paused and failing
    refusals to the same errors
    - _Requirements: 2.1-2.5, 3.1, 3.2_
    - DONE: Refusal-order and no-trace properties pass; review regressions cover admitted ids in update-with-start and
      close-attempt clearing after timeout, worker failure and server failure, preserving UnhandledCommand.
  - [x] 2.3 Add `UpdatePaused` and the update message of `WorkflowNotReady`, and map `UpdatePaused` at the edge to
    `FAILED_PRECONDITION` with v1.31.0's message
    - _Requirements: 2.2, 2.3_
    - DONE: Wire-error unit tests verify UpdatePaused and the update-specific WorkflowNotReady answer.

- [x] 3. Completed updates
  - DONE: Completion, protocol-message, codec and recovery tests pass, including the review fixes.
  - [x] 3.1 Add `completed_updates` to `WorkflowState` and the pure `completed_update_ids`. Add the id in each arm
    that writes or replays a completion
    - _Requirements: 2.6_
    - DONE: The history/replay property includes reset prefix forks; both completion-event forms are unit-tested.
  - [x] 3.2 In the message handler, look a message's update up in flight, then completed, then as today. Refuse every
    message type for a completed update with v1.31.0's invalid state transition message
    - _Requirements: 2.7, 3.4_
    - DONE: All three message types are refused after completion; Acceptance, Response, Response in one completion
      verifies the ProvisionallyCompleted state name.
  - [x] 3.3 Add `COMPLETED_UPDATES_SECTION` (5) to the state extension: written only when the set isn't empty, its ids
    strictly ascending
    - _Requirements: 2.6, 3.6_
    - DONE: Codec tests verify round-trips, omission when empty, ascending ids and rejection of malformed sections.
  - [x] 3.4 Recover the ids of an open run whose state has none and whose count is above zero when it loads: on DSQL in
    the load's read-only snapshot, with the history's parts; in memory under its lock
    - _Requirements: 2.8_
    - DONE: In-memory recovery properties and the DSQL live recovery test pass; criterion 2.8 records the known limits.

- [x] 4. Write property tests for Properties 1 to 5
  - **Property 1: The refusals come first, in order**
  - **Property 2: A refused request leaves no trace**
  - **Property 3: The completed ids are the history's**
  - **Property 4: A message for a completed update fails the task**
  - **Property 5: A run from before this change recovers its ids**
  - **Validates: Requirements 2.1-2.8, 3.1, 3.4-3.6**
  - DONE: Properties 1 to 5 pass, including reset prefix replay and the review regression tests.

- [x] 5. Write the unit tests and the live tests listed in the design
  - _Requirements: 2.2, 2.3, 2.6, 2.8_
  - DONE: The listed unit tests pass. Both live tests passed on an ephemeral Aurora DSQL cluster from a nextest
    archive: recovery on DSQL, and loading a closed run written before the change without reading its history.

- [x] 6. Add the release note
  - _Requirements: 2.1-2.7_
  - DONE: The update-refusals release fragment includes both known limits; the changie dry run passes.

- [x] 7. Align the specs the design lists
  - DONE: The runtime-update-lifecycle, signal-update-limits, speculative-wft and admitted-updates-after-restart specs
    are aligned; runtime-update-lifecycle Requirement 8 numbering and citations are corrected.

- [x] 8. Checkpoint: the exploration tests pass, each negative control fails its test, the live tests pass on an
  ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes
  - DONE: All eight design negative controls failed their tests: seven offline; control 7 (no recovery at load) ran
    live and failed live test 1. Both live tests pass. The nine review regressions fail with the fixes reverted byte
    for byte and pass after restoration; the full bar passes on the final head.

## Notes

- Property tests use `proptest` where their inputs are generated, tagged
  `// Feature: update-refusals, Property N: <title>`.
- Live runs follow `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- The release already upgrades with every node stopped, and its notes say so.
