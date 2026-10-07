# Implementation Plan

## Overview

Check every payload a request carries against v1.31.0's limits at the edge, on the proto request: refuse oversized client calls, and record oversized worker responses as non-retryable server failures.

## Tasks

- [x] 1. Limits
  - [x] 1.1 Add `crates/tokeira-edge/src/grpc/payload_limits.rs` with v1.31.0's values behind accessors, `blob_exceeds_limit`, `standalone_blob_exceeds_limit`, `memo_exceeds_limit`, `check_search_attribute_count` and `check_search_attribute_sizes`, and `check_blob` and `check_start_payloads` for client calls
    - _Requirements: 2.3, 2.10, 2.11, 2.12_
  - [x] 1.2 Add `server_failure` and `truncate_failure`, which cuts a failure down to fit a limit, with `oversized_failure`, `oversized_replacement` and `limit_activity_failure` to apply them
    - _Requirements: 2.6_
  - [x] 1.3 Write property tests for Properties 1 and 2
    - **Property 1: The limits match v1.31.0's**
    - **Property 2: A truncated failure fits its limit**
    - **Validates: Requirements 2.3, 2.6, 2.10**

- [x] 2. Client calls
  - [x] 2.1 Check StartWorkflowExecution, the start in ExecuteMultiOperation, and SignalWithStartWorkflowExecution: search attributes, input, memo, then the signal input. The checks sit in the translation functions, so ExecuteMultiOperation's start, which reuses StartWorkflowExecution's, reports the error for that operation
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 2.2 Check SignalWorkflowExecution's input and QueryWorkflow's arguments, in their translation functions
    - _Requirements: 2.1, 2.4_
  - [x] 2.3 Check StartActivityExecution's input and search attributes, and the cancellation and termination reasons
    - _Requirements: 2.3, 2.9_
  - [x] 2.4 Write unit tests for each call at its limit and one byte over
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.9_

- [x] 3. Worker responses
  - [x] 3.1 Turn RespondActivityTaskFailed's body, by task token and by id, into helpers that keep the workflow and standalone paths (`fail_activity_task` and `fail_activity_task_by_id`)
    - _Requirements: 2.11_
  - [x] 3.2 Fail the activity for an oversized completion result or cancellation details, and classify a top-level server failure by its own non-retryable flag, as v1.31.0's `isRetryable` does, so that the failure closes the activity
    - _Requirements: 2.5_
  - [x] 3.3 Fail the activity for oversized heartbeat details, and answer `cancel_requested`
    - _Requirements: 2.7_
  - [x] 3.4 In RespondActivityTaskFailed, drop oversized last heartbeat details and replace an oversized failure, listing both in the response
    - _Requirements: 2.6, 2.8_
  - [x] 3.5 In RespondWorkflowTaskFailed, replace an oversized failure
    - _Requirements: 2.6_
  - [x] 3.6 Write a property test for Property 3 in `crates/tokeira-engine/tests/payload_admission_limits.rs`, through the in-process gRPC endpoint, and tests for cancellation details, RespondWorkflowTaskFailed and standalone activities
    - **Property 3: Oversized worker responses become failures**
    - **Validates: Requirements 2.5, 2.6, 2.7, 2.8, 2.11**

- [x] 4. Wire the seven keys for the Temporal functional harness, as `conformance-config-override`'s key table records: the harness's overrides in the accessors, `Wired` in its key registry, the compatibility ledger and the configuration doc generated from it, and the readiness doc's note and table row on the three standalone harness exclusions
  - _Harness only: `conformance-config-override`_

- [x] 5. Checkpoint: the full bar of root `AGENTS.md` §10.4

## Notes

- Property tests use `proptest`, tagged `// Feature: payload-admission-limits, Property N: <title>`.
- Sizes come from `prost::Message::encoded_len` on the proto request's fields, before they are converted.
