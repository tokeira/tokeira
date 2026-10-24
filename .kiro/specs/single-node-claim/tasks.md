# Implementation Plan

## Overview

First show the defect, with a test that fails on the current code. Then:
- let a coordinated DSQL director honour an ownership gate;
- take the single-node claim at distributed startup, refusing before anything starts while another node holds it;
- renew it, stop when it is lost, and release it on a clean shutdown;
- align the specs and documentation, and prove it on an ephemeral cluster.

## Tasks

- [x] 1. Exploration, before the fix
  - [x] 1.1 Write the design's live test of two nodes without a controller on one cluster
    - _Requirements: 1.1, 1.2_
  - [x] 1.2 Run it on the current code, on an ephemeral cluster with two ephemeral DynamoDB tables, and confirm that
    both nodes serve and the second holds every lease
    - _Requirements: 1.1, 1.2_
    - DONE. On the code before the fix, on an ephemeral cluster with two ephemeral DynamoDB tables, the test failed as
      predicted: node B started while node A served, both served, and B held every lease.

- [x] 2. Let `DsqlConnectionDirector::install_ownership_gate` and `acquire_shutdown_control` accept the coordinated
  reservoir's director, with its checkout path honouring the gate as the embedded one does
  - _Requirements: 2.6_

- [x] 3. The claim
  - [x] 3.1 Add the claim's module to `tokeira-engine`: its name, identity, owner and timings, and its decisions as
    pure functions over a clock and the repository's outcomes
    - _Requirements: 2.1, 2.2, 2.4_
  - [x] 3.2 Acquire it in `build_and_serve`'s DSQL branch after `check_distributed_schema` and before
    `build_dsql_stack` when no controller endpoint is configured; try a busy claim again for 80 s, then refuse and
    shut the store down
    - _Requirements: 2.1-2.3, 3.1, 3.3_
  - [x] 3.3 Install the gate on the store's director, renew from acquisition onwards, and wait out the quiescence of
    an expired takeover before `build_dsql_stack`
    - _Requirements: 2.4, 2.5_
  - [x] 3.4 Fence or close the gate when the claim is lost, start the node's drain, and exit with an error
    - _Requirements: 2.6_
  - [x] 3.5 Release the claim after the drain, and on a failed start, except during a quiescence; make `run_from_cli`
    wait for the release before the process exits
    - _Requirements: 2.7, 2.8_

- [x] 4. Write property tests for Properties 1 to 4, and check Property 5 in the live test
  - **Property 1: A start follows the claim**
  - **Property 2: One node admits work at a time**
  - **Property 3: A lost claim stops the node**
  - **Property 4: A release never shortens a wait**
  - **Property 5: A refused start leaves nothing started**
  - **Validates: Requirements 2.1-2.8**

- [x] 5. Write the unit tests listed in the design
  - _Requirements: 2.2_

- [x] 6. Align [dsql-performance-fixes](../dsql-performance-fixes/requirements.md) (criteria 1.1 and 1.2 and a link
  from Requirement 1), and update the documentation:
  - the deployment configuration guide: one node without a controller per DSQL cluster, and what a refusal means;
  - the ECS guide: only one `tokeirad` serves on DSQL, and the others refuse;
  - the live test's section in `docs/testing/dsql-live-suites.md`.
  - _Requirements: 2.2, 2.6_

- [x] 7. Run the live test and the negative controls on an ephemeral cluster with two ephemeral DynamoDB tables
  - _Requirements: 2.1-2.8_
  - DONE. On an ephemeral cluster with two ephemeral DynamoDB tables, the live test passed twice, and so did the
    distributed-schema tests (3 of 3) and the embedded-ownership test (1 of 1). Each live negative control failed its
    test: the claim skipped, a refusal that leaves its listener bound, a refusal that leaves the store running, and a
    busy attempt dropped without rollback. The managed embedded DSQL lifecycle test also passed, on a cluster it
    created and destroyed.

- [x] 8. Add a `changed` fragment saying that a node without a controller refuses to start while another holds its
  DSQL cluster
  - _Requirements: 2.2_

- [x] 9. Checkpoint: the exploration test passes, each negative control fails its test, the live test passes on an
  ephemeral cluster, and the full bar of root `AGENTS.md` §10.4 passes
  - DONE at `83f7729d`. The exploration test passes. Properties 1 to 4 pass offline, and each offline negative control
    failed its property: a busy claim taken as acquired, an expired takeover without the wait, a release during the
    wait, and a gate open past the deadline. The live results are task 7's. The bar passed (3,940 tests passed, 2
    skipped).
  - After review, at `e31b45df`: Property 2 counts every close, and a fixed schedule reaches its quiescence check on
    every run. With the simulated admission deadline 30 s past the hold's expiry, that schedule fails, and the four
    controls above still fail. The bar passed again (3,941 tests passed, 2 skipped).

## Notes

- Property tests use `proptest`, tagged `// Feature: single-node-claim, Property N: <title>`.
- The live run follows `docs/testing/dsql-live-suites.md`: an ephemeral cluster and two ephemeral DynamoDB tables,
  deleted when the run ends. Its crash step waits out a full quiescence, so it takes about 7 minutes.
- The release already says "Upgrade with every node stopped.", so this change adds no second note.
