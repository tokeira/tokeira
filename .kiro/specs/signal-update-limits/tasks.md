# Implementation Plan

## Overview

Count a run's signals in the kernel and store the count in the state extension. Refuse a signal at the limit, and give each caller v1.31.0's answer. Check an update's in-flight count, total and in-flight payload in the kernel, in v1.31.0's order, with request sizes from the edge and the held updates' count and request bytes from the lane. Count a reset run's copied signals and completed updates.

## Tasks

- [ ] 1. Limits and counts in the kernel
  - [ ] 1.1 Add `MAXIMUM_SIGNALS_PER_EXECUTION`, `UpdateLimits`, `UpdateLimitExceeded`, `Reject::SignalLimitExceeded` and `Reject::UpdateLimitExceeded` to `tokeira_kernel`
    - _Requirements: 2.1, 2.9-2.11, 2.14_
  - [ ] 1.2 Add `WorkflowState::signal_count`; count signals where the kernel admits them and in `replay_history_prefix`; refuse a signal at the limit in `apply_signal`
    - _Requirements: 2.1, 2.4, 2.6, 2.7_
  - [ ] 1.3 Check the in-flight count, the total and the in-flight payload in `apply_update`, and the total in a completion's resurrection arms; count the copied history's completed updates in `apply_replayed_event`
    - _Requirements: 2.7, 2.9-2.13_
  - [ ] 1.4 Write property tests for Properties 1, 4, 5 and 6, and for the kernel's part of Property 2
    - **Property 1: The signal count is the run's recorded signals**
    - **Property 2: A signal is refused exactly at the limit**
    - **Property 4: Update admission matches v1.31.0's**
    - **Property 5: Updates are counted as v1.31.0 counts them**
    - **Property 6: Resurrecting an update respects the total limit**
    - **Validates: Requirements 2.1, 2.4, 2.6, 2.7, 2.9-2.13**

- [ ] 2. Storing the count
  - [ ] 2.1 Add `SIGNAL_COUNT_SECTION`, tag 2, to the state extension: written when the count isn't zero, after the heartbeat section, and applied on decode
    - _Requirements: 2.8, 3.5_
  - [ ] 2.2 Write property tests for Property 3, and a unit test that a state stored without the section counts from zero
    - **Property 3: The stored count round-trips**
    - **Validates: Requirements 2.8, 3.5**

- [ ] 3. Signals in the runtime
  - [ ] 3.1 Answer as a duplicate a SignalWorkflowExecution or batch signal refused at the limit whose request id the run already applied; skip that lookup for SignalWithStart; let the closing check stand aside for a run at the limit
    - _Requirements: 2.1-2.3_
  - [ ] 3.2 Resolve a sender whose signal the target refused at the limit: as signaled when the target already applied it, otherwise as failed with `SIGNAL_COUNT_LIMIT_EXCEEDED`
    - _Requirements: 2.5_
  - [ ] 3.3 Write property tests for Property 2's answers through the runtime on the in-memory store
    - **Property 2: A signal is refused exactly at the limit**
    - **Validates: Requirements 2.1-2.5**

- [ ] 4. Updates in the runtime and the edge
  - [ ] 4.1 Measure each update request's encoded size at the edge, and keep it in the update registry
    - _Requirements: 2.11_
  - [ ] 4.2 Resolve `UpdateLimits` in the runtime and put them on each update command and on a completion's limits; set `held_updates` and `in_flight_request_bytes` in the lane from the registry, for the admitted updates of the state it loaded, on an update command and `held_updates` on a completion
    - _Requirements: 2.9-2.14_
  - [ ] 4.3 Map the rejects to `InvalidArgument`, to `ResourceExhausted` with cause `CONCURRENT_LIMIT` and scope `NAMESPACE`, and to `FailedPrecondition`, in ExecuteMultiOperation too
    - _Requirements: 2.1, 2.9-2.13_
  - [ ] 4.4 Write the engine's tests of each answer through the in-process gRPC endpoint, and the runtime's tests of the total limit and a resurrection on a seeded run, and of a run whose ten admitted updates lost their requests in a restart still admitting a new update
    - _Requirements: 2.1-2.5, 2.9-2.13_

- [ ] 5. Wire `history.maxInFlightUpdates` for the Temporal functional harness, and classify `history.maximumSignalsPerExecution` as kernel-excluded, as `conformance-config-override`'s key table records: the harness's overrides in the runtime's update limits, its key registry, and the compatibility ledger with the configuration doc generated from it
  - _Harness only: `conformance-config-override`_

- [ ] 6. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: signal-update-limits, Property N: <title>`.
- The kernel holds the counts and makes every decision. The lane supplies the facts that only it can see at the moment of the check: how many of the run's admitted updates it holds, and their request bytes.
- `history.maxTotalUpdates` was wired for the continue-as-new advice; the same override now sets the total limit too.
