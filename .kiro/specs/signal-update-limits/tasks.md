# Implementation Plan

## Overview

Count a run's signals in the kernel and store the count in the state extension. Refuse a signal at the limit, and give each caller v1.31.0's answer, a repeated signal's success included, whether its run is closed, closing or at the limit. Resolve a signal to another workflow that can't be delivered with v1.31.0's cause. Check an update's in-flight count, total and in-flight payload in the kernel, in v1.31.0's order, with request sizes from the edge and the held updates' count and request bytes from the lane. Count a reset run's copied signals and completed updates.

## Tasks

- [x] 1. Limits and counts in the kernel
  - [x] 1.1 Add `MAXIMUM_SIGNALS_PER_EXECUTION`, `UpdateLimits`, `UpdateLimitExceeded`, `Reject::SignalLimitExceeded` and `Reject::UpdateLimitExceeded` to `tokeira_kernel`
    - _Requirements: 2.1, 2.9-2.11, 2.14_
  - [x] 1.2 Add `WorkflowState::signal_count`; count signals where the kernel admits them and in `replay_history_prefix`; refuse a signal at the limit in `apply_signal`
    - _Requirements: 2.1, 2.4, 2.6, 2.7_
  - [x] 1.3 Check the in-flight count, the total and the in-flight payload in `apply_update`, and the total in a completion's resurrection arms; count the copied history's completed updates in `apply_replayed_event`
    - _Requirements: 2.7, 2.9-2.13_
  - [x] 1.4 Write property tests for Properties 1, 4, 5 and 6, and for the kernel's part of Property 2
    - **Property 1: The signal count is the run's recorded signals**
    - **Property 2: A client's signal is answered as v1.31.0 answers it**
    - **Property 4: Update admission matches v1.31.0's**
    - **Property 5: Updates are counted as v1.31.0 counts them**
    - **Property 6: Resurrecting an update respects the total limit**
    - **Validates: Requirements 2.1, 2.4, 2.6, 2.7, 2.9-2.13**

- [x] 2. Storing the count
  - [x] 2.1 Add `SIGNAL_COUNT_SECTION`, tag 2, to the state extension: written when the count isn't zero, after the heartbeat section, and applied on decode
    - _Requirements: 2.8, 3.5_
  - [x] 2.2 Write property tests for Property 3, and a unit test that a state stored without the section counts from zero
    - **Property 3: The stored count round-trips**
    - **Validates: Requirements 2.8, 3.5**

- [x] 3. Signals in the runtime
  - [x] 3.1 Answer as a duplicate a SignalWorkflowExecution or batch signal refused because the run is closed, closing or at the limit, when the run already applied its request id; skip those lookups for SignalWithStart; let the closing check stand aside for a run at the limit
    - _Requirements: 2.1-2.3_
  - [x] 3.2 Resolve a sender whose signal can't be delivered with v1.31.0's cause: `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND` for a self-signal and a missing or closed target, `SIGNAL_COUNT_LIMIT_EXCEEDED` for a target at the limit, and as signaled when the target already applied the signal
    - _Requirements: 2.5_
  - [x] 3.3 Write property tests for Properties 2 and 7 through the runtime on the in-memory store
    - **Property 2: A client's signal is answered as v1.31.0 answers it**
    - **Property 7: A signal to another workflow resolves as v1.31.0 resolves it**
    - **Validates: Requirements 2.1-2.5**

- [x] 4. Updates in the runtime and the edge
  - [x] 4.1 Measure each update request's encoded size at the edge, and keep it in the update registry
    - _Requirements: 2.11_
  - [x] 4.2 Resolve `UpdateLimits` in the runtime and put them on each update command and on a completion's limits; set `held_updates` and `in_flight_request_bytes` in the lane from the registry, for the admitted updates of the state it loaded, on an update command and `held_updates` on a completion
    - _Requirements: 2.9-2.14_
  - [x] 4.3 Map the rejects to `InvalidArgument`, to `ResourceExhausted` with cause `CONCURRENT_LIMIT` and scope `NAMESPACE`, and to `FailedPrecondition`, in ExecuteMultiOperation too
    - _Requirements: 2.1, 2.9-2.13_
  - [x] 4.4 Write the engine's tests of each answer through the in-process gRPC endpoint, and the runtime's tests of the total limit and a resurrection on a seeded run, and of a run whose ten admitted updates lost their requests in a restart still admitting a new update
    - _Requirements: 2.1-2.5, 2.9-2.13_

- [x] 5. Wire `history.maxInFlightUpdates` for the Temporal functional harness, and classify `history.maximumSignalsPerExecution` as kernel-excluded, as `conformance-config-override`'s key table records: the harness's overrides in the runtime's update limits, its key registry, and the compatibility ledger with the configuration doc generated from it
  - _Harness only: `conformance-config-override`_

- [x] 6. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: signal-update-limits, Property N: <title>`.
- The kernel holds the counts and makes every decision. The lane supplies the facts that only it can see at the moment of the check: how many of the run's admitted updates it holds, and their request bytes.
- `history.maxTotalUpdates` was wired for the continue-as-new advice; the same override now sets the total limit too.
