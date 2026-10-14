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
