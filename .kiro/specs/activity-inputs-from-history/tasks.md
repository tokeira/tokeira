# Implementation Plan

## Overview

First show the defect, with tests that fail on the current code. Then:
- add the one-lookup read of a scheduled event;
- retire the copies of an activity's input and header in the state, the dispatch rows and the backlog entries;
- write the state under a new envelope version;
- answer every start from the scheduled event;
- prove it on a live cluster.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Write the tests:
    - the in-memory test for Property 1;
    - the live DSQL test of the first example, six pending activities with inputs of about 200 KB each.
    - _Requirements: 1.1-1.4_
  - [ ] 1.2 Run them on the current code, in memory and on an ephemeral DSQL cluster, and confirm each fails as 1.1 to 1.4 predict
    - _Requirements: 1.1-1.4_

- [ ] 2. The scheduled-event read
  - [ ] 2.1 Add `RunRepository::read_history_event`:
    - its default implementation;
    - the DSQL one-batch lookup;
    - the in-memory search;
    - forwarding from the `Arc` implementation and the edge's `HistoryNotifyingRepository`.
    - _Requirements: 2.3_
  - [ ] 2.2 Write property tests for Property 3
    - **Property 3: One lookup finds the scheduled event**
    - **Validates: Requirements 2.3**

- [ ] 3. Retire the copies
  - [ ] 3.1 In the kernel:
    - add `RetiredField<T>`;
    - retire `ActivityState`'s input and header in their positions;
    - stop the kernel copying them at scheduling and at replay;
    - drop `DispatchOp::EnqueueActivityTask`'s input.
    - _Requirements: 2.1, 2.6_
  - [ ] 3.2 In storage and the runtime:
    - drop `DispatchableActivityTask`'s input;
    - write `input_data` empty and stop reading it;
    - retire `BacklogPayload::Activity`'s input;
    - make `SNAPSHOT_FORMAT_VERSION` 5.
    - _Requirements: 2.1, 2.6_
  - [ ] 3.3 In the state codec:
    - write the state under a new envelope version;
    - accept the previous version on decode;
    - widen `BlobFormatError`'s message.
    - _Requirements: 2.6, 2.7_
  - [ ] 3.4 Make the measured state the encoded state
    - _Requirements: 2.8, 3.6_

- [ ] 4. Answer starts from the scheduled event
  - [ ] 4.1 In `start_activity_task_inner`:
    - read the scheduled event after the checks that can refuse the start, and before the commit;
    - build the answer's input and header from the event;
    - return the task to its queue when the read fails or finds no event.
    - _Requirements: 2.2, 2.4, 3.1, 3.2_
  - [ ] 4.2 Make `original_activity_options` read the scheduled event with `read_history_event`
    - _Requirements: 3.4_
  - [ ] 4.3 Seed a scheduled event in the runtime tests that start an activity they seed
    - _Requirements: 3.2_

- [ ] 5. Write property tests for Properties 1, 2, 4 and 5
  - **Property 1: No copies of an activity's input or header**
  - **Property 2: A delivered task carries its scheduled event's input and header**
  - **Property 4: A start whose read fails changes nothing**
  - **Property 5: This release reads the previous release's data, and the previous release refuses this release's state**
  - **Validates: Requirements 2.1, 2.2, 2.4, 2.5, 2.6, 2.7**

- [ ] 6. Write the unit tests:
  - the read on both stores;
  - the start's order and its failed read;
  - the codec, on the previous release's frozen bytes and the new version;
  - a backlog entry and a dispatch row that the previous release wrote;
  - the snapshot version;
  - the measure.
  - _Requirements: 2.3, 2.4, 2.6, 2.7, 2.8_

- [ ] 7. Extend the live DSQL suite, with its rows in `docs/testing/dsql-live-suites.md`:
  - the first example;
  - a multi-event batch;
  - a reset successor's copied batch;
  - the previous release's dispatch rows and backlog entries.
  - _Requirements: 2.1-2.7_

- [ ] 8. Align the specs this changes:
  - runtime-activity-pump;
  - dsql-side-tables;
  - run-growth-limits criterion 2.9 and its design;
  - the Out of Scope of activity-state-writes and of bounded-bulk-writes;
  - history-pagination criterion 2.8.
  - _Requirements: 2.1, 2.3, 2.8_

- [ ] 9. Checkpoint: the exploration tests pass, each negative control fails its test, the live suite passes on an ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- Property tests use `proptest`, tagged `// Feature: activity-inputs-from-history, Property N: <title>`.
- Task 4 builds on tasks 2 and 3, and tasks 5 to 7 build on task 4.
- The release already says "Upgrade with every node stopped.", so this change adds no second note.
