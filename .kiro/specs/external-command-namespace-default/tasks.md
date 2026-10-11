# Implementation Plan

## Overview

First show the defect with tests that fail on the current code. Then default a signal's empty namespace to the workflow's own in the kernel, have the runtime look up the target namespace's name for the events that resolve a signal or a cancel request, and test both, with the child start's existing default.

## Tasks

- [x] 1. Exploration, before the fix
  - [x] 1.1 Write the runtime tests of the two examples: a signal and a cancel request, each to a workflow in the same namespace, naming no namespace
    - _Requirements: 1.1-1.3_
  - [x] 1.2 Run them on the current code and confirm each fails as 1.1 to 1.3 predict
    - _Requirements: 1.1-1.3_
    - DONE. On the code before the fix, the signal's initiated event recorded the nil namespace id, the signal never reached its workflow, and its resolving event was SignalExternalWorkflowExecutionFailed with `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`, the nil id and an empty name. The cancel request reached its workflow, but its resolving event had an empty name.

- [x] 2. The signal's target
  - [x] 2.1 Default a `SignalExternalWorkflowExecution` command's nil target namespace id to the workflow's own in the kernel, as the cancel handler does
    - _Requirements: 2.1, 2.2_

- [x] 3. The resolving events' name
  - [x] 3.1 Add `target_namespace` to `ExternalSignalResolvedRequest` and `ExternalCancelResolvedRequest`, and record it in the resolving event when the pending request has no name
    - _Requirements: 2.3, 3.3_
  - [x] 3.2 Look up the target namespace's name in the publisher's signal and cancel delivery, and pass it in every resolution command they submit
    - _Requirements: 2.3, 3.4_

- [x] 4. Write property tests for Properties 1, 2 and 3
  - **Property 1: An empty namespace means the workflow's own**
  - **Property 2: A resolving event names the target namespace**
  - **Property 3: A signal within a namespace reaches its workflow**
  - **Validates: Requirements 2.1, 2.2, 2.3, 3.2, 3.3, 3.4**

- [x] 5. Write the unit tests listed in the design, including the child start's existing default
  - _Requirements: 2.1, 2.3, 3.1_

- [x] 6. Align runtime-external-signals: criteria 3.3, 4.7 and 4.8, the glossary entries of the two resolution commands, and the design's paragraph on the dispatch op's namespace
  - _Requirements: 2.1, 2.3_

- [x] 7. Checkpoint: the exploration tests pass, each negative control fails its test, and the full bar of root `AGENTS.md` §10.4 passes
  - DONE at `90f260b7`. The exploration tests and Properties 1 to 3 pass. Each of the six negative controls failed its tests. The bar passed (3,842 tests passed, 2 skipped).

## Notes

- Property tests use `proptest`, tagged `// Feature: external-command-namespace-default, Property N: <title>`.
- No live DSQL run is needed: the change touches no storage.
