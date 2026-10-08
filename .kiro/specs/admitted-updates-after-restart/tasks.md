# Implementation Plan

## Overview

Show the four defects first with tests that fail on the current code. Then:
- record history-admitted updates with the run;
- forget lost updates before each command, in the kernel crate, from the lane;
- re-admit a retry of a lost update;
- schedule follow-up tasks only for held updates;
- count in flight from the run's own state.

## Tasks

- [x] 1. Exploration, before the fix
  - [x] 1.1 Write runtime tests on the in-memory store, with a new runtime over the same store as the restart:
    - a lost update's retry is answered without an outcome;
    - a worker's empty completion of the post-restart speculative task is followed by another;
    - a run's advice counts its lost updates;
    - after a reset that reapplies ten updates, an eleventh is admitted.
    - _Requirements: 1.1-1.4_
  - [x] 1.2 Run them on the current code and confirm each fails as 1.1 to 1.4 predict
    - _Requirements: 1.1-1.4_
    - DONE: each failed as predicted. The retry was answered admitted, with no outcome, after its wait; a speculative task followed the empty one; the advice suggested continuing as new for too many updates; and the eleventh update was admitted.

- [x] 2. History-admitted updates stored with the run
  - [x] 2.1 Add `WorkflowState::history_admitted_updates`. Fill it where the kernel records or replays a WorkflowExecutionUpdateAdmitted event, and keep it within `admitted_updates` at the end of every transition.
    - _Requirements: 2.6_
  - [x] 2.2 Add `HISTORY_ADMITTED_UPDATES_SECTION`, tag 4, to the state extension. Write it when the set isn't empty, after the sections before it, and apply it on decode.
    - _Requirements: 2.6, 3.4_
  - [x] 2.3 Write property tests for Properties 5 and 6
    - **Property 5: History admission is stored**
    - **Property 6: History admission follows the run**
    - **Validates: Requirements 2.6, 3.4**

- [x] 3. Forgetting lost updates
  - [x] 3.1 Add `forget_lost_updates` to the kernel. It removes lost ids, and an unstarted speculative task left with nothing to deliver.
    - _Requirements: 2.1, 2.3_
  - [x] 3.2 Call it in the lane before every `kernel.apply`, with the registry as `held`. Keep `in_flight_request_bytes` on update commands, and drop the held-update counts.
    - _Requirements: 2.1, 2.5_
  - [x] 3.3 Write property tests for Property 1
    - **Property 1: The run keeps only the admitted updates it can deliver**
    - **Validates: Requirements 2.1, 2.3**

- [x] 4. Re-admitting a retry
  - [x] 4.1 Make `update_workflow`'s dedupe send a lost id to the kernel as `readmit`, and have `apply_update` admit it as a new update
    - _Requirements: 2.2_
  - [x] 4.2 Write property tests for Property 2 through the runtime
    - **Property 2: A lost update's retry is a new update**
    - **Validates: Requirement 2.2**

- [x] 5. Follow-up tasks and counting
  - [x] 5.1 Schedule a follow-up speculative task only when an admitted id outside the history-admitted set remains, at both sites
    - _Requirements: 2.4, 3.2_
  - [x] 5.2 Count in flight as `admitted_updates.len() + pending_updates.len()` in the update limits' admission check and a completion's re-admission slots, and remove `held_updates` from the update and completion requests
    - _Requirements: 2.5_
  - [x] 5.3 Write property tests for Properties 3 and 4
    - **Property 3: A follow-up task carries something**
    - **Property 4: In flight is held, history-admitted and accepted**
    - **Validates: Requirements 2.4, 2.5**

- [x] 6. Checkpoint: the exploration tests pass, and the full bar of root `AGENTS.md` §10.4
  - DONE 2026-10-08 at `fbc4c0e8`. The exploration tests and Properties 1 to 6 pass, and the bar passed (3,743 tests passed, 2 skipped). Each negative control failed its test except a retry the runtime sends without `readmit`, which no runtime test can tell apart: the run then answers a duplicate, and the task already pending delivers the retry's own request. The kernel half of that control fails `a_readmitted_update_is_admitted_anew`.

## Notes

- Property tests use `proptest`, tagged `// Feature: admitted-updates-after-restart, Property N: <title>`.
- The kernel makes every decision. The lane supplies the one fact only it can see at the moment of the command: which admitted updates the registry holds.
- `signal-update-limits`' held-update count is replaced by the run's own count. Its tests of held updates keep their cases, against the new count.
