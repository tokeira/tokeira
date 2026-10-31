# Implementation Plan

## Overview

First show the defects, with tests that fail on the current code. Then:
- give both stores a commit that writes a close and its successor in one transaction;
- carry each close's successor plan to its lane, build and apply the successor there, and commit the pair;
- fall back to today's separate start when DSQL refuses the pair for its size;
- retire the separate starts, align the specs and documentation, and prove it on an ephemeral cluster.

## Tasks

- [x] 1. Exploration, before the fix
  - DONE: The three exploration tests reproduced criteria 1.1–1.3 before the fix and now pass with the pair commit.
  - [x] 1.1 Write the design's three offline exploration tests
    - _Requirements: 1.1-1.3_
  - [x] 1.2 Run them on the current code and confirm each fails as 1.1 to 1.3 predict
    - _Requirements: 1.1-1.3_

- [x] 2. The pair commit
  - DONE: Added the fenced pair API, the DSQL transaction with size classification, and indivisible in-memory publication.
  - [x] 2.1 Add the pair commit to `RunRepository`, with its result and size refusal
    - _Requirements: 2.1-2.3_
  - [x] 2.2 Write it on DSQL: one fence read, both runs' checks, the pointer condition, both write sets, and a size
    refusal answered as such
    - _Requirements: 2.1-2.3, 2.6_
  - [x] 2.3 Write it in memory under the store's one lock, with the same checks and pointer condition
    - _Requirements: 2.1-2.3_

- [x] 3. The runtime
  - DONE: Carried successor plans into the lane, applied and committed both runs, ordered post-commit effects, and retained only the size-refusal separate start.
    The server's history-notifying repository forwards the pair and wakes both histories only after application.
  - [x] 3.1 Carry each close's successor plan in the lane's message: read the started event before submitting a retry
    completion, and pass the plans `submit_workflow_timeout` already reads
    - _Requirements: 2.4_
  - [x] 3.2 In the lane, build the successor's `StartRequest` after the kernel applies the close, apply it to
    `LoadedRun::Absent`, fail the close on a rejection, and commit the pair
    - _Requirements: 2.1, 2.4, 2.8, 3.1, 3.3_
  - [x] 3.3 Do the successor's post-commit work after the close's, before the reply
    - _Requirements: 2.5_
  - [x] 3.4 On a size refusal, commit the close alone and, once it applies, submit the successor's start to its lane
    past the drain check
    - _Requirements: 2.6, 2.7_
  - [x] 3.5 Stop starting successors after a close everywhere else: the lane's spawned continue-as-new start,
    `start_retry_successor`, `start_cron_successor` and the run timeout's two successor starts
    - _Requirements: 2.1, 2.7, 3.2_

- [x] 4. Write property tests for Properties 1 to 5
  - DONE: Properties 1–5 cover injected cuts, pointer states, builder preservation, draining size fallback and rejected successors; all four offline controls fail their covering tests.
  - **Property 1: A close and its successor commit together**
  - **Property 2: The pointer moves only from the closing run**
  - **Property 3: The successor is today's**
  - **Property 4: The fallback keeps the close**
  - **Property 5: A rejected successor fails the close**
  - **Validates: Requirements 2.1-2.4, 2.6, 2.8, 3.3**

- [x] 5. Write the unit tests listed in the design
  - DONE: Unit tests cover statement/commit size refusals, the first fence read, and conflict retries retaining successor identity.
  - _Requirements: 2.2, 2.6_

- [x] 6. Align the specs and documentation listed in the design
  - DONE: Aligned the listed compatibility specifications and architecture, readiness and live-suite documentation.
  - _Requirements: 2.1, 2.5, 2.6_

- [x] 7. Run the live test and the negative controls on an ephemeral cluster
  - DONE: Reviewed the recorded live DSQL pass (555.219 seconds) and both live controls' intended failures:
    omitting the successor writes leaves no successor row, and bypassing the pointer condition overwrites another
    run's pointer. The runtime and storage sources are unchanged from that live-verified implementation.
  - _Requirements: 2.1-2.3, 2.6_

- [x] 8. Add a `fixed` fragment saying that a continue-as-new, retry or cron successor is written with its run's close,
  so a crash or a drain no longer ends the chain
  - DONE: Generated the fixed fragment with changie; the patch-release dry run passes.
  - _Requirements: 2.1_

- [x] 9. Checkpoint: the exploration tests pass, each negative control fails its test, the live test passes on an
  ephemeral cluster, the corpus tiers for continue-as-new, retry and cron pass, and the full bar of root `AGENTS.md`
  §10.4 passes
  - DONE (2026-10-31): The six-command bar passes, including 4,032 workspace tests (the two explicitly ignored Rust
    SDK integration tests remain skipped). All 12 corpus cases pass without skips across continue-as-new, workflow
    retry, cron and cron client suites, using the pinned v1.31.0 conformance corpus. The history-wrapper regression
    fails before the forwarding fix and passes with it. The exploration and recorded negative-control evidence has
    been reconciled with the unchanged runtime/storage implementation.

## Notes

- Property tests use `proptest` where their inputs are generated, tagged `// Feature: atomic-successors, Property N:
  <title>`.
- The live run follows `docs/testing/dsql-live-suites.md`, on an ephemeral cluster deleted when the run ends.
- The change adds no migration and needs no upgrade note of its own.
