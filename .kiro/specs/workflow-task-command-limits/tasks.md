# Implementation Plan

## Overview

Measure what each command of a workflow task completion carries at the edge, check it in the kernel where v1.31.0 checks it, and terminate the workflow for a payload over its limit as v1.31.0 does. Check query results at the edge.

## Tasks

- [x] 1. Limits and measurement
  - [x] 1.1 Move v1.31.0's blob, memo, search attribute and pending Nexus values into `tokeira_kernel::limits`, add them to `WorkflowTaskCompletionLimits`, resolve them in the runtime, and use them in the edge's `payload_limits`
    - _Requirements: 2.13_
  - [x] 1.2 Add `CommandPayloadSizes` and `payload_size.rs`, the size arithmetic for the run's stored payloads, memo and search attributes
    - _Requirements: 2.12_
  - [x] 1.3 Measure each proto command in `respond_completed_request_to_edge`, keep its sizes with it through the leftover-message splice, and pass them through the runtime to the kernel
    - _Requirements: 2.12, 2.13_
  - [x] 1.4 Append the four missing causes to `WorkflowTaskFailedCause`, with their names, proto mapping and history serialization
    - _Requirements: 2.14_
  - [x] 1.5 Write property tests for Property 3
    - **Property 3: Sizes match v1.31.0's measurements**
    - **Validates: Requirement 2.12**

- [x] 2. Checks in the kernel
  - [x] 2.1 Check each command's payload where v1.31.0 does, with `Reject::CommandExceedsLimit`
    - _Requirements: 2.1, 2.9_
  - [x] 2.2 Check ContinueAsNewWorkflowExecution's and StartChildWorkflowExecution's memo and search attribute sizes after the input
    - _Requirements: 2.2_
  - [x] 2.3 Check an upsert's and ModifyWorkflowProperties' upserted fields, then the merged search attributes or memo
    - _Requirements: 2.3, 2.4_
  - [x] 2.4 Check the search attribute key count on upserts, ContinueAsNewWorkflowExecution and StartChildWorkflowExecution, and put it before the edge's registered-key check on upserts
    - _Requirements: 2.5_
  - [x] 2.5 Check each protocol message's body before it is applied
    - _Requirements: 2.6_
  - [x] 2.6 Check pending Nexus operations
    - _Requirements: 2.7_
  - [x] 2.7 Write property tests for Property 1, and unit tests for each command at and one byte over each limit
    - **Property 1: Command limits match v1.31.0's**
    - **Validates: Requirements 2.1-2.7, 2.9**

- [x] 3. Termination
  - [x] 3.1 Add `terminate_reason` to `WorkflowTaskFailedRequest`: record the failure, flush the buffered events and terminate the run
    - _Requirements: 2.8_
  - [x] 3.2 Handle `CommandExceedsLimit` in the runtime's invalid-command seam, with the drop on later attempts
    - _Requirements: 2.8, 2.10_
  - [x] 3.3 Write property tests for Property 2
    - **Property 2: A terminating failure is recorded as v1.31.0 records it**
    - **Validates: Requirements 2.8, 2.10**

- [x] 4. Query results
  - [x] 4.1 Fail a query whose answer in a completion is over the blob size limit with `InvalidArgument` (`QueryResultDto::ResultTooLarge`, `QueryResult::ResultTooLarge`), and replace RespondQueryTaskCompleted's oversized result with a failed result
    - _Requirements: 2.11_
  - [x] 4.2 Write unit tests for both paths at the limit and one byte over
    - _Requirements: 2.11_

- [x] 5. Wire `component.nexusoperations.limit.operation.concurrency` for the Temporal functional harness, as `conformance-config-override`'s key table records: the harness's override in the runtime's limits, `Wired` in its key registry, and the compatibility ledger
  - _Harness only: `conformance-config-override`_

- [x] 6. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: workflow-task-command-limits, Property N: <title>`.
- The edge measures the proto completion before converting it. The kernel measures only the run's stored values.
