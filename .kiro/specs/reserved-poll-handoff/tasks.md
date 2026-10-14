# Implementation Plan

## Tasks

- [x] 1. Exploration: write the four tests of the design's Testing Strategy and run them on the code before the fix;
  each fails there
  - _Requirements: 1.1-1.3_

- [x] 2. Keep the reservation's waiter id, and re-park a returned reservation under it
  - _Requirements: 1.4, 2.3_

- [x] 3. Make the poll loop wait only for its hand-off while reserved, and make its takes and exits atomic with
  leaving the wait list
  - _Requirements: 2.1, 2.2, 2.4_

- [x] 4. Checkpoint: the four tests pass, the broker's and runtime's suites pass, and the full bar of root
  `AGENTS.md` §10.4 passes
  - _Requirements: 3.1-3.3_

- [x] 5. Move the reservation and the hand-off into the start's lane: the lane reserves a parked poll as it takes a
  start that asks for one, hands it the first workflow task after the commit whether or not the caller still waits,
  and returns it when the start doesn't commit
  - _Requirements: 2.5, 3.2, 3.4, 3.5_

- [x] 6. Lane tests for Property 3, including a home lost after the commit, each failing under its negative control
  - _Requirements: 2.5, 3.2, 3.4_

- [x] 7. Tests for Requirement 3.5: the start's decision to ask for a poll, and runtime tests that an eager and a
  delayed start leave a parked poll free
  - _Requirements: 3.5_

- [x] 8. A property test of Property 1 over generated interleavings, failing on the broker before the fix
  - _Requirements: 2.1, 2.2_

- [x] 9. Checkpoint: the broker's, lane's and runtime's suites pass, and the full bar of root `AGENTS.md` §10.4 passes
  - _Requirements: 3.1-3.5_
