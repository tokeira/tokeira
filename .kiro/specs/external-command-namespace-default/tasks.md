# Implementation Plan

## Overview

First show the defect with tests that fail on the current code. Then default a signal's empty namespace to the workflow's own in the kernel, have the runtime look up the target namespace's name for the events that resolve a signal or a cancel request, and test both, with the child start's existing default.

## Tasks

- [ ] 1. Exploration, before the fix
  - [ ] 1.1 Write the runtime tests of the two examples: a signal and a cancel request, each to a workflow in the same namespace, naming no namespace
    - _Requirements: 1.1-1.3_
  - [ ] 1.2 Run them on the current code and confirm each fails as 1.1 to 1.3 predict
    - _Requirements: 1.1-1.3_

- [ ] 2. The signal's target
  - [ ] 2.1 Default a `SignalExternalWorkflowExecution` command's nil target namespace id to the workflow's own in the kernel, as the cancel handler does
    - _Requirements: 2.1, 2.2_

- [ ] 3. The resolving events' name
  - [ ] 3.1 Add `target_namespace` to `ExternalSignalResolvedRequest` and `ExternalCancelResolvedRequest`, and record it in the resolving event when the pending request has no name
    - _Requirements: 2.3, 3.3_
  - [ ] 3.2 Look up the target namespace's name in the publisher's signal and cancel delivery, and pass it in every resolution command they submit
    - _Requirements: 2.3, 3.4_

- [ ] 4. Write property tests for Properties 1, 2 and 3
  - **Property 1: An empty namespace means the workflow's own**
  - **Property 2: A resolving event names the target namespace**
  - **Property 3: A signal within a namespace reaches its workflow**
  - **Validates: Requirements 2.1, 2.2, 2.3, 3.2, 3.3, 3.4**

- [ ] 5. Write the unit tests listed in the design, including the child start's existing default
  - _Requirements: 2.1, 2.3, 3.1_

- [ ] 6. Align runtime-external-signals: criteria 3.3, 4.7 and 4.8, the glossary entries of the two resolution commands, and the design's paragraph on the dispatch op's namespace
  - _Requirements: 2.1, 2.3_

- [ ] 7. Checkpoint: the exploration tests pass, each negative control fails its test, and the full bar of root `AGENTS.md` §10.4 passes

## Notes

- Property tests use `proptest`, tagged `// Feature: external-command-namespace-default, Property N: <title>`.
- No live DSQL run is needed: the change touches no storage.
