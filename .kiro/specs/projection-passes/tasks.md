# Implementation Plan

## Overview

First show the defects, with tests that fail on the current code. Then:
- make the apply exact, which needs nothing else and lands first;
- give both stores the paged read and the named delete, and seed from the newer of image and applied value;
- replace the worker with the projector, owned by the partition's home;
- retire the checkpoint, align the specs, and prove it on an ephemeral cluster.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Write the tests of the design's Exploratory Bug Condition Checking
    - _Requirements: 1.1-1.3, 1.5, 1.6_
  - [ ] 1.2 Run them on the current code, the live ones on an ephemeral cluster, and confirm each fails as the design
    predicts
    - _Requirements: 1.1-1.3, 1.5, 1.6_

- [ ] 2. The exact apply
  - [ ] 2.1 Read the stored row by its full primary key inside the apply's transaction, and fail the apply when the read
    fails
    - _Requirements: 2.8, 1.6-1.8_
  - [ ] 2.2 Upsert the rollup deltas in the same transaction, and retry the whole transaction on a write conflict
    - _Requirements: 2.8, 1.5_
  - [ ] 2.3 Apply deletion tombstones the same way
    - _Requirements: 2.8_
  - [ ] 2.4 Return `ApplyOutcome` from `ProjectionSink::apply`, on both sinks, and ignore it in the CHASM adapter and
    the CHASM visibility repair
    - _Requirements: 2.9, 3.4_

- [ ] 3. The log interface
  - [ ] 3.1 Replace `read_from` with `read_page` and `delete_images`, on DSQL at most 100 named keys a statement
    - _Requirements: 2.1, 2.3_
  - [ ] 3.2 Keep the in-memory log in key order, with each run's latest image indexed, and carry it in snapshots as a
    list that restores in any order
    - _Requirements: 2.1, 3.5_
  - [ ] 3.3 Keep the latest image of an unseeded run in the in-memory `delete_images`
    - _Requirements: 2.4_

- [ ] 4. Seeding from the newer
  - [ ] 4.1 Read the run's applied value with its image in the DSQL load's snapshot, and seed from the newer
    - _Requirements: 2.4, 3.3_
  - [ ] 4.2 Validate the applied value as criterion 2.11 validates an image
    - _Requirements: 2.4_

- [ ] 5. The projector
  - [ ] 5.1 Replace `ProjectionWorker` with `Projector`: passes from the head, each run's newest image, and the deletion
    rule
    - _Requirements: 2.1-2.3_
  - [ ] 5.2 Back off on a failed read, apply or delete without ending, and pace idle passes at 1 s
    - _Requirements: 2.6, 2.7_
  - [ ] 5.3 Apply the runs of different stripes concurrently, at most one run of a stripe at a time
    - _Requirements: 2.10_
  - [ ] 5.4 Report the pass metrics, and remove the checkpoint metrics
    - _Requirements: 2.12_

- [ ] 6. Ownership
  - [ ] 6.1 Expose a partition's home acquisition from the runtime
    - _Requirements: 2.5_
  - [ ] 6.2 Replace the engine's spawn of one worker per partition with a supervisor that runs each held partition's
    projector under its acquisition's token
    - _Requirements: 2.5_

- [ ] 7. Remove the checkpoint methods, and the cursor codec they use, from both stores. Leave the table
  - _Requirements: 2.11_

- [ ] 8. Write property tests for Properties 1 to 4
  - **Property 1: Images converge**
  - **Property 2: Deleting is safe**
  - **Property 3: Every image is reached**
  - **Property 4: Count is exact under faults**
  - **Validates: Requirements 2.1-2.10**

- [ ] 9. Write the unit tests listed in the design
  - _Requirements: 2.3, 2.4, 2.6, 2.7, 2.12_

- [ ] 10. Align the specs and documentation listed in the design
  - _Requirements: 2.4, 2.11_

- [ ] 11. Run the live test and the negative controls on an ephemeral cluster, and record the `EXPLAIN` plans and the
  applies a second
  - _Requirements: 2.1-2.10_

- [ ] 12. Add the `fixed` fragment, and say "Upgrade with every node stopped." once in the release that first deletes
  images
  - _Requirements: 2.13_

- [ ] 13. Checkpoint: the exploration tests pass, each negative control fails its test, the live test passes on an
  ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- The code lands in three PRs: the exact apply (task 2); the log, seeding, projector, ownership and checkpoint removal
  (tasks 3-7); and concurrency by stripe (task 5.3), once live measurement has a serial baseline.
- The first PR that deletes images lands in a release whose schema every earlier release's nodes refuse.
- Property tests use `proptest` where their inputs are generated, tagged
  `// Feature: projection-passes, Property N: <title>`.
- The live runs follow `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
