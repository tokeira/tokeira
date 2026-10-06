# Implementation Plan

## Overview

Check each commit's growth in the stores against limits the runtime hands them, and terminate a run over a limit through its lane as v1.31.0 does. Bound buffered events on every path, and truncate an activity's stored failure.

## Tasks

- [x] 1. Limits and the event count
  - [x] 1.1 Add v1.31.0's growth, buffered event and stored activity failure values, `RunGrowthLimits` and `RunLimitExceeded` to `tokeira_kernel::limits`
    - _Requirements: 2.10_
  - [x] 1.2 Mark each transition's finishing events in the kernel's builder, and add `events_numbered_at_close` and `growth_limits` to `Transition`
    - _Requirements: 2.2_
  - [x] 1.3 Write unit tests for the finishing events of each kind against v1.31.0's numbering
    - _Requirements: 2.2_

- [x] 2. Checks in the stores
  - [x] 2.1 Add the shared check function: history size, count and measured state for a commit that leaves an existing run open, then the batch, logging above a warn limit
    - _Requirements: 2.1-2.4, 2.6, 2.9, 2.10_
  - [x] 2.2 Run it in DSQL's `write_transition` before the first write, encoding the state once, and in the in-memory store's commit before its first change
    - _Requirements: 2.1-2.4, 2.6_
  - [x] 2.3 Write property tests for Property 1, and unit tests for the measured state and a run's first write over the batch limit
    - **Property 1: Growth checks match v1.31.0's**
    - **Validates: Requirements 2.1-2.4, 2.6, 2.9**

- [x] 3. Termination
  - [x] 3.1 Resolve the growth limits in the runtime and set them on the lane's commits and the activity writes' commits
    - _Requirements: 2.4, 2.10_
  - [x] 3.2 Terminate the run in the lane on a terminating breach, in the same activation, with every post-commit step, and reply with the breach's error
    - _Requirements: 2.1-2.3, 2.5, 2.6_
  - [x] 3.3 Terminate the run through its lane on a breach in the activity writes (`terminate_on_growth_breach`, with the runtime's lanes on `ActivityRetryDeps`), dropping the task in the poll's start
    - _Requirements: 2.4, 2.5_
  - [x] 3.4 Map `RunLimitExceeded` to `InvalidArgument` at the edge
    - _Requirements: 2.1-2.3, 2.6_
  - [x] 3.5 Write property tests for Property 2, and a unit test of the count on a run seeded at the limit
    - **Property 2: A terminating breach terminates the run as v1.31.0 does**
    - **Validates: Requirements 2.1-2.3, 2.5, 2.6**

- [x] 4. Buffered events
  - [x] 4.1 Run `enforce_buffered_event_limit` at the end of every transition but an activity's start, with the size limit measured by the buffered events' payloads
    - _Requirements: 2.7_
  - [x] 4.2 Write property tests for Property 3
    - **Property 3: Buffered events are bounded on every path**
    - **Validates: Requirement 2.7**

- [x] 5. Stored activity failures
  - [x] 5.1 Move the `TruncateWithDepth` port and the server failure builder to `tokeira-proto`, with the non-retryable flag
    - _Requirements: 2.8_
  - [x] 5.2 Truncate a failure over the stored activity failure limit in `commit_activity_retry`
    - _Requirements: 2.8_
  - [x] 5.3 Write property tests for Property 4
    - **Property 4: Stored activity failures are truncated as v1.31.0 truncates them**
    - **Validates: Requirement 2.8**

- [x] 6. Wire `limit.historySize.error`, `limit.historyCount.error`, `limit.mutableStateSize.error`, their warn keys, `system.transactionSizeLimit` and `limit.mutableStateActivityFailureSize.error` for the Temporal functional harness, and classify `history.maximumBufferedEventsSizeInBytes` as kernel-excluded, as `conformance-config-override`'s key table records: the harness's overrides in the runtime's limits, its key registry, and the compatibility ledger
  - _Harness only: `conformance-config-override`_

- [x] 7. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: run-growth-limits, Property N: <title>`.
- The stores measure what they store. The kernel measures buffered events by their payloads, since it holds domain events.
- With the three growth keys wired, no key in the harness's registry is `NotEnforced`. The disposition stays for any key the corpus overrides later. The registry's and `tokeira-conformance-control`'s tests, which reject `limit.mutableStateSize.error` and `limit.historySize.error` as not enforced, drop that case, and the registry's test checks that the newly wired keys are accepted.
