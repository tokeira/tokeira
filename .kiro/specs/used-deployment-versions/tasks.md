# Implementation Plan

## Overview

First show the four defects with tests that fail on the current code. Then move the list's bookkeeping into the
kernel's completion, make storage copy the list into images, and align the specifications.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Write the tests of the design's Exploratory Bug Condition Checking, one per defect
    - _Requirements: 1.1-1.4_
  - [ ] 1.2 Run them on the current code and confirm each fails as the design predicts
    - _Requirements: 1.1-1.4_

- [ ] 2. The kernel
  - [ ] 2.1 Add `used_deployment_versions_len`, the length of Go's `json.Marshal` of a list
    - _Requirements: 2.3_
  - [ ] 2.2 Add `WorkflowState::add_used_deployment_version`, which appends a reported version when absent and, given
    a limit, trims the oldest until the list fits
    - _Requirements: 2.1, 2.3_
  - [ ] 2.3 Call it from the live completion with the completion's value limit, and from the replayed completion with
    none
    - _Requirements: 2.1, 2.2, 2.4_

- [ ] 3. Storage
  - [ ] 3.1 Stop deriving the list from the stored deployment version in `projection_context`, and stop folding in
    `prepare_workflow_projection`; insert the run's list into the image when it isn't empty
    - _Requirements: 2.2, 2.5, 2.6_
  - [ ] 3.2 Update the projection-accumulator oracle to the new image
    - _Requirements: 2.5, 3.1_

- [ ] 4. Write property tests for Properties 1 to 4
  - **Property 1: The list is the fold of the completions**
  - **Property 2: A changed list fits**
  - **Property 3: Replay doesn't trim**
  - **Property 4: Images carry the list**
  - **Validates: Requirements 2.1-2.5, 3.1**

- [ ] 5. Write the unit tests listed in the design
  - _Requirements: 2.1-2.6, 3.2-3.5_

- [ ] 6. Align [projection-accumulator](../projection-accumulator/bugfix.md): criteria 2.6 and 3.2-3.5, its table of
  deferred corrections, and its design's fold owner
  - _Requirements: 2.1-2.6_

- [ ] 7. Add the `fixed` fragment
  - _Requirements: 2.6_

- [ ] 8. Checkpoint: the exploration tests pass, each negative control fails its test, and the full bar of root
  `AGENTS.md` §10.4 passes

## Notes

- The code lands in one PR after this specification merges.
- Property tests use `proptest`, tagged `// Feature: used-deployment-versions, Property N: <title>`.
- Negative controls: append without trimming (Property 2 fails); trim on replay (Property 3 fails); keep the
  stored-version observation in `projection_context` (the inherited-version test fails); add the version before the
  drop of a speculative, rejection-only completion (the dropped-completion test fails).
