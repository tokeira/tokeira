# Implementation Plan

## Overview

Make history reads honour their limits on both repositories, read whole histories explicitly in pages, bound DSQL's statements, page the edge's history calls as v1.31.0 does, and have single-fact readers read only that fact.

## Tasks

- [ ] 1. Storage
  - [ ] 1.1 Remove `effective_history_limit` and `DEFAULT_HISTORY_PAGE_SIZE`, so the DSQL repository honours any limit
    - _Requirements: 2.1_
  - [ ] 1.2 Add `read_history_to_end` and `read_attributed_history_to_end` to `RunRepository` as default methods that read pages of `HISTORY_READ_PAGE` events until a short page
    - _Requirements: 2.2_
  - [ ] 1.3 Read DSQL history batches in statements of 64 with `LIMIT`, continuing after the last batch read
    - _Requirements: 2.3_
  - [ ] 1.4 Write a property test for Property 1 on the in-memory repository, and a unit test that the DSQL batch statement carries `LIMIT`
    - **Property 1: Pages concatenate to the history**
    - **Validates: Requirements 2.1, 2.2, 3.1**

- [ ] 2. Edge
  - [ ] 2.1 Use the effective page size, 256 for 0 or less or above 256, in both history calls
    - _Requirements: 2.4_
  - [ ] 2.2 Keep GetWorkflowExecutionHistory's full-page token rule, now on a finite limit, with `wait_new_event` and the close-event filter unchanged
    - _Requirements: 2.5, 3.2_
  - [ ] 2.3 Page GetWorkflowExecutionHistoryReverse from the run's last event in state, and leave the token empty on the page that includes event 1
    - _Requirements: 2.6, 3.4_
  - [ ] 2.4 Take the last event id from run state in `read_last_event_id`
    - _Requirements: 2.7_
  - [ ] 2.5 Read to the end in reset validation, batch reset, batch reset target resolution and the direct query's poll response
    - _Requirements: 2.9_
  - [ ] 2.6 Write property tests for Properties 2 and 3, and unit tests for the effective page size
    - **Property 2: Forward pages follow v1.31.0's page size**
    - **Property 3: Reverse pages start at the last event**
    - **Validates: Requirements 2.4, 2.5, 2.6**

- [ ] 3. Runtime
  - [ ] 3.1 Read each matching activity's scheduled event by id in `original_activity_options`
    - _Requirements: 2.8_
  - [ ] 3.2 Read to the end in `update_lifecycle_snapshot`
    - _Requirements: 2.9_
  - [ ] 3.3 Write unit tests for Property 4
    - **Property 4: Single facts come from state or by id**
    - **Validates: Requirements 2.7, 2.8**

- [ ] 4. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: history-pagination, Property N: <title>`.
- Test code that reads a whole history moves to the to-end methods, or to a finite limit.
- Paging poll responses is out of scope (see the design).
