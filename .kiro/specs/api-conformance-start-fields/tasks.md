# Implementation Plan: Start Field API Conformance

## Overview

Implement full external start field handling across `StartWorkflowExecution` and `SignalWithStartWorkflowExecution`, including durable delayed start, callbacks, metadata, links, versioning override, cron, and current-run conflict policies.

## Tasks

- [x] 1. Audit and model start fields
  - [x] 1.1 Add a field-policy table test in `crates/tokeira-edge/src/translate/to_internal.rs`
    - Cover every field of `StartWorkflowExecutionRequest` (29) and `SignalWithStartWorkflowExecutionRequest` (26 — field numbers run to 27 but number 21 is `reserved` in the vendored v1.62.11 proto) in the v1.62.11 proto — not just the fields currently listed in `UNSUPPORTED_FIELDS.md`. Include the v1.62 additions `on_conflict_options`, `priority`, `eager_worker_deployment_options`, `time_skipping_config`, and the deprecated `control`.
    - _Requirements: 1.1-1.15_
  - [x] 1.2 Add validation/error mappings for invalid internal-only and unsupported fields
    - Verify `errors.rs`, `grpc/errors.rs`, and `grpc_error_code` map malformed/internal-only start input, a SignalWithStart `FAIL` conflict policy, and `time_skipping_config` behavioural requests to `INVALID_ARGUMENT`.
    - _Requirements: 1.12, 1.14, 1.15, 2.5_
  - [x] 1.3 Refresh `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`
    - Add the four missing v1.62 fields (`on_conflict_options`, `priority`, `eager_worker_deployment_options`, `time_skipping_config`) with their target policy/owner so the doc matches the proto. Remove fields this spec moves to supported.
    - _Requirements: 1.9, 1.10, 1.11, 1.12_

- [x] 2. Translate all external start fields
  - [x] 2.1 Update `start_request` translation
    - Map reuse/conflict enum values; default an unset conflict policy to `FAIL` for `StartWorkflowExecution`.
    - Preserve `request_eager_execution`.
    - Translate delay, callbacks, metadata, links, versioning override, priority, `on_conflict_options`, and client cron into internal DTOs.
    - Reject internal-only fields, a `time_skipping_config` behavioural request, and malformed values; ignore deprecated `control`.
    - _Requirements: 1.1-1.15, 3.3_
  - [x] 2.2 Update `signal_with_start_request` translation
    - Reuse the same start-field helper as normal start.
    - Default an unset conflict policy to `USE_EXISTING` and reject `FAIL` as `INVALID_ARGUMENT`.
    - Ensure validation happens before start/signal mutation.
    - _Requirements: 2.1, 2.3, 2.4, 2.5_

- [ ] 3. Add durable start model fields
  - [x] 3.1 Extend `StartRequest`, `WorkflowExecutionStarted`, and run state
    - Add conflict policies, delayed-start metadata, callback registrations, user metadata, links, versioning override, priority, `on_conflict_options`, and cron policy.
    - Replace the fieldless `CompletionCallback` placeholder with a representable struct (callback spec/URL, trigger, registration time, state, attempt count, last-attempt failure); use `#[serde(default)]` so existing persisted runs deserialize.
    - Keep the kernel pure.
    - _Requirements: 3.1, 3.2, 4.1_
  - [x] 3.2 Update history serializer and describe projection for newly supported fields
    - Include priority threading into `WorkflowExecutionInfo.priority`.
    - _Requirements: 3.1, 3.3, 1.10_
  - [x] 3.3 Add storage/current-execution conflict handling
    - Enforce reuse/conflict policies in the current-execution index for memory and DSQL-backed state.
    - _Requirements: 1.1, 3.4_
  - [x] 3.4 Add delayed-start and cron runtime scheduling
    - Store durable timer entries and schedule the first WFT or next cron run only when the timer fires.
    - Status: delayed-start runtime scheduling, client cron first-WFT backoff, and cron continue-as-new successor scheduling are implemented.
    - _Requirements: 1.3, 1.8, 3.4_
  - [x] 3.5 Add terminal callback dispatch
    - Persist callback state in the kernel and dispatch callbacks exactly once from runtime-derived effects after terminal transitions.
    - _Requirements: 1.4, 3.4_
  - [x] 3.6 Apply versioning override to dispatch routing
    - Persist override in run state and use it when choosing WFT routing/versioning metadata.
    - _Requirements: 1.7, 3.4_
  - [x] 3.7 Render registered callbacks in DescribeWorkflowExecution
    - Extend the describe DTO in `crates/tokeira-edge/src/translate/mod.rs` with a callbacks list and add a `callback_info_to_proto` builder in `crates/tokeira-edge/src/grpc/translate.rs`.
    - Populate `DescribeWorkflowExecutionResponse.callbacks` with one `CallbackInfo` per registered callback from the loaded run snapshot; emit an empty list when there are none.
    - Update the describe construction sites (`apps/tokeirad/src/lib.rs`, `crates/tokeira-edge/tests/grpc_new_endpoints.rs`) to pass callback state through.
    - _Requirements: 4.2, 4.3, 4.4_
  - [x] 3.8 Apply `on_conflict_options` under USE_EXISTING
    - When the conflict policy resolves to `USE_EXISTING` and `on_conflict_options` is set, add the corresponding history event to the running workflow; a nil/empty value is a no-op.
    - _Requirements: 1.9_

- [x] 4. Add required tests
  - [x] 4.1 Property test: No Silent Drop
    - _Requirements: 1.3-1.14, 2.3, 2.5_
  - [x] 4.2 Property test: Start/SignalWithStart Parity
    - _Requirements: 2.1, 3.1_
  - [x] 4.3 Property test: Durable Start Metadata
    - _Requirements: 3.1, 3.2_
  - [x] 4.4 Property test: Callback Describe Fidelity
    - **Property 4: Callback Describe Fidelity**
    - Generate runs with varying registered callbacks; assert one `CallbackInfo` per registration and an empty list when none.
    - _Requirements: 4.2, 4.3_
  - [x] 4.5 Property test: Conflict-Policy Defaulting
    - **Property 5: Conflict-Policy Defaulting**
    - Assert Start defaults unset conflict policy to `FAIL`, SignalWithStart defaults to `USE_EXISTING`, and SignalWithStart with explicit `FAIL` is rejected as `INVALID_ARGUMENT`.
    - _Requirements: 1.1, 2.5_
  - [x] 4.6 Restart/recovery tests
    - Verify delayed starts, cron state, terminal callbacks, and versioning override routing survive repository reload.
    - _Requirements: 3.4, 4.5_

- [x] 5. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo test -p tokeira-kernel`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["3.1", "3.2", "3.3"] },
    { "id": 3, "tasks": ["3.4", "3.5", "3.6", "3.7", "3.8"] },
    { "id": 4, "tasks": ["4.1", "4.2", "4.3", "4.4", "4.5", "4.6"] },
    { "id": 5, "tasks": ["5"] }
  ]
}
```

## Notes

- This spec owns the completion-callback lifecycle end to end: registration persistence, the representable `CompletionCallback` shape, terminal dispatch, and `DescribeWorkflowExecution.callbacks` rendering. `api-conformance-workflow-describe` emits an empty `callbacks` list until task 3.7 lands.
- This spec also owns `priority` (authored at start, threaded into describe) and `on_conflict_options` (part of completing the conflict-policy contract). `eager_worker_deployment_options` is owned by `worker-deployments`; `time_skipping_config` is a test-server feature and is rejected, not implemented; deprecated `control` is ignored.
- Conflict-policy defaulting differs by RPC: Start defaults to `FAIL`; SignalWithStart defaults to `USE_EXISTING` and rejects `FAIL` (matches Temporal v1.31.0). Parity covers every other start field.
- Field accounting is anchored to the v1.62.11 proto (29 Start fields; 26 SignalWithStart fields — its field numbers run to 27 but number 21 is `reserved`, and `workflow_id_conflict_policy` is out of order at 22), not to `UNSUPPORTED_FIELDS.md`, which is refreshed by task 1.3.
- The `CompletionCallback` field additions use `#[serde(default)]` so existing persisted runs deserialize without migration.
- Property tests are required, not optional; they are externally-visible correctness contracts for the start surface.
- Server-internal `continued_failure` / `last_completion_result` remain `INVALID_ARGUMENT` for external clients.
