# Implementation Plan

## Overview

Check every payload a request carries against v1.31.0's limits in the edge's gRPC handlers: refuse oversized client calls, and record oversized worker responses as non-retryable server failures.

## Tasks

- [ ] 1. Limits
  - [ ] 1.1 Add `crates/tokeira-edge/src/grpc/payload_limits.rs` with v1.31.0's values behind accessors, `check_blob_size`, `check_memo_size`, `check_search_attribute_count` and `check_search_attribute_sizes`
    - _Requirements: 2.3, 2.10, 2.11, 2.12_
  - [ ] 1.2 Add `server_failure` and `truncate_failure`, a port of `TruncateWithDepth`
    - _Requirements: 2.6_
  - [ ] 1.3 Write property tests for Properties 1 and 2
    - **Property 1: The limits match v1.31.0's**
    - **Property 2: Truncation matches v1.31.0's**
    - **Validates: Requirements 2.3, 2.6, 2.10**

- [ ] 2. Client calls
  - [ ] 2.1 Check StartWorkflowExecution, the start in ExecuteMultiOperation, and SignalWithStartWorkflowExecution: search attributes, input, memo, then the signal input
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 2.2 Check SignalWorkflowExecution's input and QueryWorkflow's arguments
    - _Requirements: 2.1, 2.4_
  - [ ] 2.3 Check StartActivityExecution's input and search attributes, and the cancellation and termination reasons
    - _Requirements: 2.3, 2.9_
  - [ ] 2.4 Write unit tests for each call at its limit and one byte over
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.9_

- [ ] 3. Worker responses
  - [ ] 3.1 Turn RespondActivityTaskFailed's body, by task token and by id, into helpers that keep the workflow and standalone paths
    - _Requirements: 2.11_
  - [ ] 3.2 Fail the activity for an oversized completion result or cancellation details
    - _Requirements: 2.5_
  - [ ] 3.3 Fail the activity for oversized heartbeat details, and answer `cancel_requested`
    - _Requirements: 2.7_
  - [ ] 3.4 In RespondActivityTaskFailed, drop oversized last heartbeat details and replace an oversized failure, listing both in the response
    - _Requirements: 2.6, 2.8_
  - [ ] 3.5 In RespondWorkflowTaskFailed, replace an oversized failure
    - _Requirements: 2.6_
  - [ ] 3.6 Write a property test for Property 3, and unit tests for standalone activities
    - **Property 3: Oversized worker responses become failures**
    - **Validates: Requirements 2.5, 2.6, 2.7, 2.8, 2.11**

- [ ] 4. Wire the seven keys for the Temporal functional harness, as `conformance-config-override`'s key table records: the harness's overrides in the accessors, and `Wired` in its key registry
  - _Harness only: `conformance-config-override`_

- [ ] 5. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: payload-admission-limits, Property N: <title>`.
- Sizes come from `prost::Message::encoded_len` on the proto request, before translation.
