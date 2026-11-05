# Implementation Plan: Workflow Task Completion API Conformance

## Overview

Complete `RespondWorkflowTaskCompleted` field handling and return-new-workflow-task semantics without moving workflow semantics out of the kernel/runtime path.

## Tasks

- [x] 1. Preserve completion metadata
  - [x] 1.1 Update `crates/tokeira-edge/src/grpc/translate.rs`
    - Account for every field of `RespondWorkflowTaskCompletedRequest` in the v1.62.11 proto (20 fields), not just those in `UNSUPPORTED_FIELDS.md`.
    - Preserve `sdk_metadata`, `metering_metadata`, current `deployment_options`, `versioning_behavior`, `sticky_attributes`, and `capabilities`; accept deprecated `binary_checksum` / `worker_version_stamp` / `deployment` for back-compat only.
    - Treat `resource_id` as routing envelope; leave `worker_instance_key` / `worker_control_task_queue` default.
    - Do not use `TryFrom`; follow existing free-function translation.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1, 2.2, 2.3_
  - [x] 1.2 Update kernel request/event models
    - Add supported metadata to `WorkflowTaskCompletedRequest` and `HistoryEventKind::WorkflowTaskCompleted` (sdk metadata, metering, deployment options, versioning behavior).
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 1.3 Refresh `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`
    - Add the current `deployment_options`, `resource_id`, `worker_instance_key`, `worker_control_task_queue`, and `capabilities` with their target policy/owner; mark the deprecated trio as back-compat-only.
    - _Requirements: 1.3, 1.4, 1.5, 1.6_

- [x] 2. Implement sticky, metering, and deployment/versioning preservation
  - [x] 2.1 Add validation and translation in `WorkflowService::respond_workflow_task_completed`
    - Validate sticky fields and the `versioning_behavior` enum, pass accepted values to runtime/kernel DTOs, and reject unknown `versioning_behavior` as `INVALID_ARGUMENT`.
    - _Requirements: 2.1, 2.2, 2.4, 4.3_
  - [x] 2.2 Persist sticky and deployment/versioning state
    - Update kernel state and history metadata without adding I/O to the kernel.
    - _Requirements: 1.3, 2.1, 2.2_
  - [x] 2.3 Apply sticky routing in runtime dispatch
    - Subsequent WFT dispatch honors accepted sticky metadata. Deployment/versioning routing application is deferred to `worker-deployments` and is NOT implemented here.
    - _Requirements: 2.1_
  - [x] 2.4 Verify error and metric mapping
    - Update `errors.rs`, `grpc/errors.rs`, and `grpc_error_code` if new variants are required.
    - _Requirements: 4.1, 4.2, 4.3_

- [x] 3. Implement return-new-WFT semantics
  - [x] 3.1 Extend runtime completion response path
    - Return an immediately available WFT only after durable scheduling/start.
    - Preserve existing inline query and eager activity behavior.
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 4. Add required tests
  - [x] 4.1 Property test: Metadata Fidelity
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 4.2 Property test: Sticky Routing and Versioning Preservation
    - _Requirements: 2.1, 2.2, 2.4_
  - [x] 4.3 Property test: Return-New-WFT Safety
    - _Requirements: 3.2, 3.3_
  - [x] 4.4 Restart/recovery test: Completion Routing Metadata
    - Verify sticky and deployment/versioning metadata reload; sticky affects subsequent dispatch.
    - _Requirements: 2.1, 2.2_
  - [x] 4.5 Deprecated-field acceptance test
    - Verify `binary_checksum` / `worker_version_stamp` / `deployment` are accepted but drive no new behavior.
    - _Requirements: 1.4_

- [x] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-runtime`.
  - Run `cargo test -p tokeira-kernel`.

- [ ] 6. Start completion-returned tasks atomically
  - [x] 6.1 Before production edits, park worker B on the normal queue while worker A
    completes with return-new enabled. Synchronize the old publisher/claim gap and
    assert only A receives the new task, including a force-new local-activity
    heartbeat with no sticky poll. Record failure on `main`; with return-new false,
    B must receive ordinary work. Retain a negative control that restores the gap.
  - [x] 6.2 Add explicit inline operands/results across completion command, pure
    builder, runtime, edge port and adapter. Start Normal tasks in the completion
    transition with no enqueue/row/offer. Apply the guarded build-ID mismatch,
    old-version stamp handling, deployment transition and failed-completion cases.
    Introduce transition-time scope operands here; edge-eager-dispatch task 11
    reuses the admission contract for activities.
  - [x] 6.3 Include a speculative update-only follow-up in the same committed
    pending-state result, with virtual history and no dispatch. Test rejection-only
    drop/reset-history, materialization, timeout and reload; document the existing
    Tokeira durability mapping to Temporal's post-persistence speculative start.
  - [x] 6.4 Remove the completion's inline `try_claim_workflow_task` call. Preserve
    actual scheduled queue, incremental history, advice, query barriers, heartbeat
    update non-redelivery, new update attachment and scoped provenance. Test a lost
    response stays started until timeout and can then recover through normal polls.
  - [x] 6.5 Required PBT — revised Property 3, at least 100 generated completion,
    routing, failure, poll and owner-reload traces, tagged
    `// Feature: api-conformance-wft-completion, Property 3`.
    Assert one start per logical incarnation and zero rows/offers for inline work
    across memory/DSQL derivation; use committed-state and publication-call oracles.
  - [ ] 6.6 Integrate transient-wft task C.2's heartbeat-limit reproduction/controls;
    orchestrator runs `TestWorkflowTaskTestSuite` including both heartbeat tests,
    `TestEagerWorkflowTestSuite` and `TestStickyTqTestSuite`. Run root §10.4 for the
    combined change. The timeout successor follows Requirement 3 but is not returned;
    test the exact sticky reapplication, update/query barriers and successor timeout
    specified in Item C.
  - _Requirements: 1.4, 2.1–2.3, 3.1–3.11, 4.1–4.3_

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.3"] },
    { "id": 1, "tasks": ["1.2", "2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3", "2.4", "3.1"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3", "4.4", "4.5"] },
    { "id": 4, "tasks": ["5"] },
    { "id": 5, "tasks": ["6.1"] },
    { "id": 6, "tasks": ["6.2", "6.3", "6.4"] },
    { "id": 7, "tasks": ["6.5", "6.6"] }
  ]
}
```

## Notes

- Field accounting is anchored to the v1.62.11 proto (20 fields on `RespondWorkflowTaskCompletedRequest`), not to `UNSUPPORTED_FIELDS.md`, which task 1.3 refreshes.
- `deployment_options` (field 17) is the current worker deployment/versioning field; `binary_checksum`, `worker_version_stamp`, and `deployment` are deprecated and accepted for back-compat only.
- This spec preserves and threads `deployment_options` / `versioning_behavior` into history/state. **Applying** them to dispatch routing is owned by `worker-deployments` and is explicitly out of scope.
- `capabilities` (speculative-WFT discard) is preserved for the `speculative-wft` feature; `messages` is the update protocol transport shared with `api-conformance-update-lifecycle`.
- Task 6 supersedes task 3.1’s post-completion scheduling/start and the old Property 3; prior checked tasks remain historical. Inline real tasks start in the completion, preserving query/update barriers.
- Property tests are required, not optional.
