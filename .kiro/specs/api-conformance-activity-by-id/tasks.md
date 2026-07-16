# Implementation Plan: Activity ById API Conformance

## Overview

Complete the workflow-scoped activity API surface. Tasks 1–10 record the already-landed ById and
single-id option-update work; Tasks 11 onward close the v1.31.0 activity-control and bulk-option gaps.
Property-based tests validate all 15 correctness properties from the revised design.

## Tasks

- [x] 1. Add `ActivityNotFound` / `ActivityNotStarted` error variants and ById resolution helper
  - [x] 1.1 Add `ActivityNotFound` and `ActivityNotStarted` variants to `EdgeError` in `crates/tokeira-edge/src/errors.rs`
    - Add `ActivityNotFound` and `ActivityNotStarted` variants with `namespace`, `workflow_id`, `activity_id` fields
    - Map `ActivityNotFound` to `StatusCode::NOT_FOUND` in `status_code()`
    - Add `"activity_not_found"` in `action_name()`
    - Map `ActivityNotStarted` to `StatusCode::PRECONDITION_FAILED`
    - Add `"activity_not_started"` in `action_name()`
    - Add gRPC status mapping for both variants in `crates/tokeira-edge/src/grpc/errors.rs`: `ActivityNotFound` → `NOT_FOUND`, `ActivityNotStarted` → `FAILED_PRECONDITION`
    - Verify both variants are handled in the `grpc_error_code` metric path in `workflow_service.rs`
    - _Requirements: 1.2, 1.3, 1.6, 7.3, 8.3_

  - [x] 1.2 Implement runtime-owned activity token resolution
    - Add `resolve_activity_token(&self, run_key: RunKey, activity_id: &str) -> Result<ActivityTaskToken, ActivityTokenResolutionError>` to the concrete runtime and edge `WorkflowRuntimeApi` abstraction
    - Reuse `resolve_execution_run_key` in the edge only for execution resolution
    - Validate non-empty `run_id` parses as a valid `RunId` before calling `resolve_execution_run_key`; return `INVALID_ARGUMENT` on parse failure
    - Runtime token resolution loads run state via `repo.load_run`, returns a typed not-found error if absent or if `activity_id` is missing
    - Runtime token resolution verifies `started_event_id` is present for completion/failure/cancel handlers; return a typed not-started error if the activity is scheduled but not started
    - Map typed runtime resolution errors in the edge handlers: `RunNotFound` → `EdgeError::WorkflowNotFound`, `ActivityNotFound` → `EdgeError::ActivityNotFound`, `ActivityNotStarted` → `EdgeError::ActivityNotStarted`
    - Allow `RecordActivityTaskHeartbeatById` to bypass heartbeat delegation and return `cancel_requested = false` for scheduled-but-not-started activities
    - Construct `ActivityTaskToken` in the runtime with `run_key`, `activity_id`, `schedule_event_id`, `attempt`, and current `shard_epoch`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.4, 8.1, 8.2, 8.3, 8.4_

  - [x] 1.3 Write property tests for ById resolution (P1, P2, P3)
    - **Property 1: ById Resolution Equivalence** — generate random identifier tuples, verify resolution produces same `RunKey` as `resolve_execution_run_key`
    - **Property 2: Not-Found Propagation** — generate non-existent identifiers, verify `NOT_FOUND` before activity mutation delegation
    - **Property 3: Runtime Token Construction Fidelity** — generate random started `ActivityState` instances, verify all five token fields match runtime state
    - Add malformed non-empty `run_id` cases that assert `INVALID_ARGUMENT`
    - Add scheduled-but-not-started cases that assert `FAILED_PRECONDITION` for completion/failure/cancel handlers and success with `cancel_requested = false` for heartbeat
    - Add absent-run cases that assert runtime `RunNotFound` maps to workflow `NOT_FOUND`
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.4, 7.3, 8.1, 8.2, 8.3, 8.4**

- [x] 2. Add ById edge DTOs and proto translation
  - [x] 2.1 Define ById request/response DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - `RespondActivityTaskCompletedByIdRequest` (namespace, workflow_id, run_id, activity_id, result, identity)
    - `RespondActivityTaskFailedByIdRequest` (namespace, workflow_id, run_id, activity_id, failure, failure_error_type, is_non_retryable, identity)
    - `RespondActivityTaskCanceledByIdRequest` (namespace, workflow_id, run_id, activity_id, details, identity)
    - `RecordActivityTaskHeartbeatByIdRequest` (namespace, workflow_id, run_id, activity_id, details, identity)
    - Add `details: Option<Payloads>` to the existing token-based `RecordActivityTaskHeartbeatRequest`
    - Use the existing `UpdateActivityOptionsRequest` / `UpdateActivityOptionsResponse` / `ActivityOptions` DTOs; do not redefine them
    - `RespondActivityTaskCanceledRequest` / `RespondActivityTaskCanceledResponse` (token-based)
    - _Requirements: 1.1, 2.1, 2.5, 2.6, 3.1, 4.1, 5.1, 6.1, 7.1, 7.2_

  - [x] 2.2 Implement proto-to-DTO translation for ById requests in `crates/tokeira-edge/src/grpc/translate.rs`
    - Add free translation functions following the existing pattern used by `respond_activity_completed_to_edge`
    - Add `respond_activity_completed_by_id_to_edge`
    - Add `respond_activity_failed_by_id_to_edge`
    - Add `respond_activity_canceled_by_id_to_edge`
    - Add `record_activity_heartbeat_by_id_to_edge`
    - Update existing `record_heartbeat_to_edge` to preserve token-based heartbeat `details`
    - Add `update_activity_options_to_edge`
    - Add `respond_activity_canceled_to_edge` for the token-based cancel path
    - Return `ProtoConversionError::MissingField` for missing required fields
    - _Requirements: 1.1, 2.1, 2.5, 2.6, 3.1, 4.1, 5.1, 6.1, 6.4, 7.1_

  - [x] 2.3 Write unit tests for proto translation
    - Test each ById proto request correctly maps to the edge DTO
    - Test missing required fields produce `INVALID_ARGUMENT`
    - Test empty `run_id` is preserved as `None`
    - _Requirements: 1.4, 6.4_

- [x] 3. Implement `cancel_activity_task` in the runtime and edge adapter
  - [x] 3.1 Add `cancel_activity_task` methods at both runtime API layers
    - Add `TokeiraRuntime::cancel_activity_task(...) -> Result<CommitResult>`
    - Submit `Command::ActivityResolved(ActivityResolvedRequest { resolution: ActivityResolution::Canceled { details }, ... })`
    - Follow the same validate → submit → commit pattern as `complete_activity_task`
    - Add `WorkflowRuntimeApi::cancel_activity_task(...) -> Result<WorkflowMutationOutcome>`
    - Add the `RuntimeAdapter` implementation that calls the concrete runtime and converts `CommitResult` to `WorkflowMutationOutcome`
    - _Requirements: 5.1, 6.1_

  - [x] 3.2 Widen the runtime heartbeat API to accept heartbeat details
    - Change `WorkflowRuntimeApi::record_activity_heartbeat` and `TokeiraRuntime::record_activity_heartbeat` to accept `details: Option<Payloads>`
    - Thread token-based and ById heartbeat `details` through the same runtime method
    - If the heartbeat store does not yet persist details, keep the payload accepted at the runtime boundary and document the storage follow-up in code
    - _Requirements: 2.5, 2.6_

  - [x] 3.3 Write property test for cancel delegation equivalence (P6)
    - **Property 6: ById-to-Token Delegation Equivalence (Cancel)** — for any valid cancellation request, verify runtime receives `ActivityResolution::Canceled { details }` with correct details
    - Verify cancellation details are persisted into the resulting `ActivityTaskCanceled` history attributes rather than being dropped at the edge/runtime boundary
    - **Validates: Requirements 5.1, 6.1**

- [x] 4. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement token-based `RespondActivityTaskCanceled` handler
  - [x] 5.1 Wire `respond_activity_task_canceled` in `WorkflowService`
    - Replace `Status::unimplemented` stub with: decode token → delegate to `cancel_activity_task` → notify history lane
    - Follow the same pattern as `respond_activity_task_completed` and `respond_activity_task_failed`
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 5.2 Wire gRPC handler in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Translate proto request → edge DTO, call `workflow_service.respond_activity_task_canceled`, translate response
    - Return `INVALID_ARGUMENT` for malformed tokens
    - _Requirements: 6.1, 6.4_

  - [x] 5.3 Write property test for malformed token rejection (P9)
    - **Property 9: Malformed Token Rejection** — generate random byte sequences that don't deserialize to valid `ActivityTaskToken`, verify `INVALID_ARGUMENT` status
    - **Validates: Requirements 6.4**

- [x] 6. Implement ById activity handlers
  - [x] 6.1 Implement `record_activity_task_heartbeat_by_id` in `WorkflowService`
    - Resolve execution to `RunKey`, then call `runtime.resolve_activity_token(run_key, activity_id)`
    - If token resolution succeeds, delegate to `runtime.record_activity_heartbeat(token, details)`
    - If token resolution returns `ActivityTokenResolutionError::ActivityNotStarted`, return `cancel_requested = false` immediately without runtime heartbeat delegation
    - Propagate identity per design (non-empty → `Some(WorkerIdentity(...))`, empty → `None`)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 9.1, 9.2_

  - [x] 6.2 Implement `respond_activity_task_completed_by_id` in `WorkflowService`
    - Resolve execution to `RunKey`, call `runtime.resolve_activity_token(run_key, activity_id)`, delegate to `runtime.complete_activity_task(token, result, worker_identity)`, then notify history lane
    - _Requirements: 3.1, 3.2, 3.3, 9.1, 9.2_

  - [x] 6.3 Implement `respond_activity_task_failed_by_id` in `WorkflowService`
    - Resolve execution to `RunKey`, call `runtime.resolve_activity_token(run_key, activity_id)`, delegate to `runtime.fail_activity_task(token, failure, failure_error_type, is_non_retryable, worker_identity)`, then notify history lane
    - _Requirements: 4.1, 4.2, 4.3, 9.1, 9.2_

  - [x] 6.4 Implement `respond_activity_task_canceled_by_id` in `WorkflowService`
    - Resolve execution to `RunKey`, call `runtime.resolve_activity_token(run_key, activity_id)`, delegate to `runtime.cancel_activity_task(token, details, worker_identity)`, then notify history lane
    - _Requirements: 5.1, 5.2, 5.3, 9.1, 9.2_

  - [x] 6.5 Write property tests for delegation equivalence (P4, P5, P7, P8)
    - **Property 4: ById-to-Token Delegation Equivalence (Completion)** — verify runtime receives same `(token, result, worker_identity)` as token-based path
    - **Property 5: ById-to-Token Delegation Equivalence (Failure)** — verify runtime receives same `(token, failure, failure_error_type, is_non_retryable, worker_identity)` as token-based path
    - **Property 7: Heartbeat Delegation and Cancel Flag** — verify correctly constructed token and heartbeat details are passed, and `cancel_requested` flag matches runtime's heartbeat store
    - **Property 8: Identity Propagation** — generate random identity strings (empty and non-empty), verify propagation as `Some(WorkerIdentity(...))` or `None`
    - Add terminal-state duplicate response cases that assert the correct gRPC error and no second terminal history event
    - Add scheduled-but-not-started heartbeat cases that assert a successful response with `cancel_requested = false`
    - **Validates: Requirements 2.1, 2.2, 2.5, 2.6, 3.1, 4.1, 5.1, 9.1, 9.2**

- [x] 7. Wire ById gRPC handlers
  - [x] 7.1 Add gRPC handler stubs for all ById RPCs in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - `record_activity_task_heartbeat_by_id` → translate proto → call workflow_service → translate response
    - `respond_activity_task_completed_by_id` → translate proto → call workflow_service → translate response
    - `respond_activity_task_failed_by_id` → translate proto → call workflow_service → translate response
    - `respond_activity_task_canceled_by_id` → translate proto → call workflow_service → translate response
    - _Requirements: 1.1, 2.1, 3.1, 4.1, 5.1_

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement `UpdateActivityOptions` edge handler
  - [x] 9.1 Implement `update_activity_options` in `WorkflowService`
    - Restrict this handler to `ActivityTarget::Id`; extract `activity_id` from the proto request and construct `ActivityTarget::Id(activity_id)`
    - Resolve execution to `RunKey`; do not use runtime-owned activity token resolution because this RPC updates pending activity options and does not require `started_event_id`
    - Submit the existing update command for the target activity and map missing activity/run errors to `NOT_FOUND`
    - Allow scheduled-but-not-yet-started activities to be updated
    - Add a match arm for `ActivityTarget::Type` and `ActivityTarget::MatchAll` that returns `tonic::Status::unimplemented` with a descriptive message
    - Use the existing `UpdateActivityOptionsRequest` edge DTO and map to the existing kernel `UpdateActivityOptionsRequest` using `FieldChange<T>`; no new types are needed
    - Submit `Command::UpdateActivityOptions(UpdateActivityOptionsRequest { ... })` via runtime
    - Return `UpdateActivityOptionsResponse` with the updated `ActivityOptions`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.6_

  - [x] 9.2 Wire `update_activity_options` gRPC handler in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Translate proto request → edge DTO, call `workflow_service.update_activity_options`, translate response
    - _Requirements: 7.1_

  - [x] 9.3 Write property test for UpdateActivityOptions field application (P10)
    - **Property 10: UpdateActivityOptions Field Application** — generate random option subsets, verify kernel applies exactly the specified fields and response reflects new values
    - **Validates: Requirements 7.1, 7.2, 7.4, 7.6**

- [x] 10. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Correct the deterministic activity-management model
  - [x] 11.1 Record the owner gate required by the functional-conformance handover before changing
    activity-management shape.
    - The existing reset request cannot represent `keep_paused`, jitter, restore-original options,
      or durable next-instance heartbeat reset; do not emulate these through multiple edge commits.
    - The owner approved this revised `api-conformance-activity-by-id` spec as the active work order;
      the completed Feature-11 spec remains a frozen implementation record.
    - _Requirements: 10.1–10.11, 11.1–11.14, 12.1–12.9_
  - [x] 11.2 Make pause idempotent and distinguish scheduled from running attempts.
    - A scheduled activity is fenced; a running activity remains completable and is held only if it
      reaches a retryable failure.
    - _Requirements: 10.1, 10.2, 10.3_
  - [x] 11.3 Carry reset flags and next-instance heartbeat-reset intent in authoritative state.
    - Apply `keep_paused`, jitter, restore-original options, and running-vs-scheduled dispatch in one
      fenced transition.
    - _Requirements: 11.1–11.7, 11.10_
  - [x] 11.4 Make unpause of an already-unpaused activity a successful no-op, apply reset flags, and
    bump the stamp for every actual unpause, including a running activity.
    - _Requirements: 12.1–12.5, 12.8, 12.9_

- [x] 12. Add runtime-owned selector resolution and activity-control APIs
  - [x] 12.1 Resolve id/type selectors against one loaded authoritative run.
    - Reject an empty match set before mutation; do not expose partial multi-activity commits.
    - _Requirements: 7.3, 7.5, 10.6, 10.7, 11.8, 11.9, 12.6, 12.7_
  - [x] 12.2 Add `pause_activities`, `unpause_activities`, and `reset_activities` to the runtime API
    and production adapter.
    - _Requirements: 10.1–10.11, 11.1–11.14, 12.1–12.9_
  - [x] 12.3 Return durable pause and reset flags from the raw runtime heartbeat transition.
    - Preserve `cancel_requested`; populate `activity_paused` and `activity_reset` independently.
    - Keep the shared attempt validator used by heartbeat and completion: matching post-reset tokens
      remain valid, while a token whose attempt differs from authoritative state is stale.
    - _Requirements: 10.5, 11.11, 11.12_
  - [x] 12.4 Correct retry preparation for paused and reset activities.
    - A retryable failure while paused increments the attempt, clears started-attempt fields, and
      suppresses dispatch while retaining the pause.
    - When the next attempt is prepared, consume heartbeat-reset intent and clear both reset flags so
      a later retry cannot repeat the reset side effects.
    - _Requirements: 10.4, 10.9–10.11, 11.13, 11.14_

- [x] 13. Complete public gRPC translation and handlers
  - [x] 13.1 Add exhaustive proto translation for Pause, Unpause, and Reset request selectors/flags.
    - _Requirements: 10.1–10.11, 11.1–11.14, 12.1–12.9_
  - [x] 13.2 Replace all three public stubs with translate → resolve → runtime-delegate handlers.
    - _Requirements: 10.1–10.11, 11.1–11.14, 12.1–12.9_
  - [x] 13.3 Complete UpdateActivityOptions retry-policy, type-target, and restore-original behavior.
    - _Requirements: 7.1–7.8_

- [ ] 14. Required property tests for activity-control semantics
  - [x] 14.1 Property test: Property 11 — pause lifecycle fidelity (minimum 100 cases).
    - Tag: `// Feature: api-conformance-activity-by-id, Property 11: pause lifecycle fidelity`
    - _Requirements: 10.1–10.4, 10.6, 10.8–10.11_
  - [ ] 14.2 Property test: Property 12 — heartbeat flag and validator fidelity (minimum 100 cases).
    - Tag: `// Feature: api-conformance-activity-by-id, Property 12: heartbeat flag and validator fidelity`
    - _Requirements: 10.5, 11.11, 11.12_
  - [x] 14.3 Property test: Property 13 — reset lifecycle fidelity (minimum 100 cases).
    - Tag: `// Feature: api-conformance-activity-by-id, Property 13: reset lifecycle fidelity`
    - _Requirements: 11.1–11.8, 11.10, 11.13, 11.14_
  - [x] 14.4 Property test: Property 14 — unpause lifecycle fidelity (minimum 100 cases).
    - Tag: `// Feature: api-conformance-activity-by-id, Property 14: unpause lifecycle fidelity`
    - _Requirements: 12.1–12.6, 12.8, 12.9_
  - [ ] 14.5 Property test: Property 15 — rejection preserves activity state (minimum 100 cases).
    - Tag: `// Feature: api-conformance-activity-by-id, Property 15: rejection preserves activity state`
    - _Requirements: 7.3, 7.8, 10.7, 11.9, 12.7_

- [ ] 15. Checkpoint: focused crates compile, lint, and tests pass
  - Run nightly formatting plus focused kernel/runtime/edge checks and tests.

- [x] 16. Functional conformance
  - [x] 16.1 Run `TestActivityApiResetClientTestSuite` twice against fresh `tokeirad` processes.
    - _Requirements: 10.1–10.11, 11.1–11.14, 12.1–12.9_
  - [x] 16.2 Run the UpdateActivityOptions and batch activity-options suites twice.
    - _Requirements: 7.1–7.8_

## Notes

- All property tests are required — they validate externally-visible correctness contracts.
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document (P1–P15)
- Unit tests validate specific examples and edge cases
- The completed Feature-11 spec remains frozen; Tasks 11–16 and the associated requirements/design
  in this owning spec supersede its stale behavioral assumptions without rewriting its history.
- The kernel already has `UpdateActivityOptionsRequest` with `FieldChange<T>` for the implemented
  single-id subset; completing retry-policy, type targeting, and restore-original behavior may extend
  the deterministic request shape.
- The runtime already has `ActivityResolution::Canceled` — only the new `cancel_activity_task` method is needed
- All handlers follow the established edge pattern: translate → resolve → delegate → notify
- Heartbeat, completion, failure, and cancellation retain their shared attempt validator. Temporal
  v1.31.0 does not define a reset exception: matching tokens remain valid and mismatched tokens are stale.
- Tasks 11–16 supersede the old `ImplementedSubset` assumption without rewriting the completed
  implementation history in Tasks 1–10.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "2.2"] },
    { "id": 2, "tasks": ["1.3", "2.3", "3.1", "3.2"] },
    { "id": 3, "tasks": ["3.3", "5.1"] },
    { "id": 4, "tasks": ["5.2", "5.3", "6.1", "6.2", "6.3", "6.4"] },
    { "id": 5, "tasks": ["6.5", "7.1"] },
    { "id": 6, "tasks": ["9.1"] },
    { "id": 7, "tasks": ["9.2", "9.3"] },
    { "id": 8, "tasks": ["11.1"] },
    { "id": 9, "tasks": ["11.2", "11.3", "11.4"] },
    { "id": 10, "tasks": ["12.1", "12.2", "12.3", "12.4"] },
    { "id": 11, "tasks": ["13.1", "13.2", "13.3"] },
    { "id": 12, "tasks": ["14.1", "14.2", "14.3", "14.4", "14.5"] },
    { "id": 13, "tasks": ["15"] },
    { "id": 14, "tasks": ["16.1", "16.2"] }
  ]
}
```
