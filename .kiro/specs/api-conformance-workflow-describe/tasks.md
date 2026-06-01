# Implementation Plan: Workflow Describe API Conformance

## Overview

Move `DescribeWorkflowExecution` to `Implemented`: extend the describe DTOs and proto translation to populate all eight response fields from one consistent run snapshot, add the kernel metadata needed for `cancel_requested` and durable root execution, and cover every section plus the expected error paths with unit, property, and integration tests. Fields owned by other named features are surfaced from their state once it exists and remain truthfully empty until then.

## Tasks

- [x] 1. Kernel: retain describe metadata
  - [x] 1.1 Add backward-compatible describe fields to `WorkflowState`
    - Add `cancel_requested: bool` (serializable, `#[serde(default)]`, defaulted false) to `crates/tokeira-kernel/src/state.rs`.
    - Add `root_workflow_id: Option<WorkflowId>` and `root_run_id: Option<RunId>` (serializable, `#[serde(default)]`) to `WorkflowState`.
    - Set it true when `HistoryEventKind::WorkflowExecutionCancelRequested` is applied in `crates/tokeira-kernel/src/kernel.rs`.
    - Set it true in `BasicKernel::apply_replayed_event` when replaying `HistoryEventKind::WorkflowExecutionCancelRequested`, so live and replayed state agree.
    - Initialize `cancel_requested` false in every `WorkflowState` construction site (start, signal-with-start, continue-as-new/reset successor).
    - Existing persisted runs without these fields SHALL deserialize with defaults (`cancel_requested = false`, `root_* = None`).
    - No I/O, async, metrics, or storage; this is pure serializable state.
    - _Requirements: 2.5, 7.4_
  - [x] 1.2 Property test: cancel flag tracks cancel-requested events
    - **Property:** Applying `WorkflowExecutionCancelRequested` sets `cancel_requested`; replaying history that contains `WorkflowExecutionCancelRequested` reconstructs `cancel_requested = true`; runs without it keep the flag false.
    - Add to `crates/tokeira-kernel/tests/property_tests.rs`.
    - **Validates: Requirements 7.4**
  - [x] 1.3 Populate durable root execution fields at start
    - Extend `StartRequest` and `SignalWithStartRequest` in `crates/tokeira-kernel/src/command.rs` with optional `root_workflow_id` and `root_run_id`.
    - Extend `HistoryEventKind::WorkflowExecutionStarted` in `crates/tokeira-kernel/src/event.rs` with defaulted `root_workflow_id: Option<WorkflowId>` and `root_run_id: Option<RunId>` fields. These are kernel-internal envelope fields; the upstream start-event proto has no root-execution slot, so the public history serializer does not emit a fabricated proto field for them.
    - In `apply_start`, set root to the supplied values when present; otherwise set root to the new run's own workflow ID and run ID.
    - In `apply_signal_with_start`, set the new `WorkflowState` root fields the same way and emit the same `WorkflowExecutionStarted` root fields. Signal-with-start is top-level in the current runtime; if a future child signal-with-start path is introduced, it must thread the parent's stored root through the same request fields.
    - Emit the root fields on every `WorkflowExecutionStarted` and restore them in `BasicKernel::replay_history_prefix` by destructuring the start event; do not rely on `ReplayContext` for root identity.
    - During replay, when the event has no root fields, default root to self (`ctx.workflow_id`, `ctx.run_id`) regardless of parent fields. This matches Temporal v1.31.0 started-event apply semantics.
    - In runtime child-start code, read the parent's stored root once and pass it into the child's `StartRequest`.
    - In continue-as-new/reset/retry successor construction (including `crates/tokeira-runtime/src/lane.rs`), pass the predecessor's stored root only when the predecessor has a parent. If the predecessor has no parent, author no root and let the kernel's event-root-or-self rule make the successor its own root.
    - **Property:** for a W1 -> W2 -> W3 child chain, all three runs report root = W1 both before and after replaying each run's history; a top-level run reports itself; a signal-with-start run reports itself before and after replay; an old-style parent-less start event with no root fields replays to root = self; an old-style child start event with parent fields but no root fields also replays to root = self.
    - **Validates: Requirements 2.5**
  - [x] 1.4 Serde migration test for existing persisted state
    - Add a deserialization fixture or round-trip test proving older `WorkflowState` documents without `cancel_requested`, `root_workflow_id`, and `root_run_id` deserialize with defaults.
    - **Validates: Requirements 2.5, 7.4**

- [x] 2. Edge DTOs: extend the describe description model
  - [x] 2.1 Extend describe DTOs in `crates/tokeira-edge/src/translate/mod.rs`
    - Add `ExecutionConfigDescription` (task queue, three timeouts, optional user metadata).
    - Extend `WorkflowExecutionDescription` with `execution_time`, parent namespace/execution, root execution, `first_run_id`, `execution_config`, `pending_nexus_operations`, and an extended-info section (`execution_expiration_time`, `run_expiration_time`, `cancel_requested`, `original_start_time`). Keep `pause_info`.
    - Extend `PendingActivityDescription` with `last_failure`, `paused`, and `pause_info`.
    - Add `PendingNexusOperationDescription` (endpoint, service, operation, scheduled time, scheduled event id, schedule-to-close timeout, state, optional operation token).
    - Keep `callbacks` modeled as an empty list while kernel `CompletionCallback` remains a fieldless placeholder; add a comment explaining why placeholder callbacks are not surfaced.
    - _Requirements: 1.1, 1.5, 2.2, 3.3, 3.4, 6.1, 6.2, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

- [x] 3. Edge translation: populate all eight response fields
  - [x] 3.1 Add `execution_config_to_proto` and populate field 1
    - Build `WorkflowExecutionConfig` (task queue, timeouts); leave `user_metadata` default with a comment naming `api-conformance-start-fields`.
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_
  - [x] 3.2 Extend `workflow_execution_info_from_description` for field 2
    - Populate `execution_time`, `execution_duration` (closed only), parent namespace/execution, root execution from stored root fields, `first_run_id`.
    - Leave versioning/deployment/priority/auto-reset/external-payload fields default with comments naming owning features.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_
  - [x] 3.3 Extend `pending_activity_to_proto` for field 3
    - Populate `last_failure`, `paused`, and `pause_info` from the activity description.
    - Leave heartbeat/retry-timing and deprecated build-id fields default.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_
  - [x] 3.4 Add `pending_nexus_operation_to_proto` and populate field 7
    - Build `PendingNexusOperationInfo` from kernel pending Nexus state; mirror `operation_token` into deprecated `operation_id`.
    - Leave attempt/cancellation/block-reason fields default.
    - _Requirements: 6.1, 6.2, 6.3_
  - [x] 3.5 Extend the `workflow_extended_info` builder for field 8
    - Keep `pause_info`; add `execution_expiration_time`, `run_expiration_time`, `cancel_requested`, `original_start_time`.
    - Always emit `workflow_extended_info` for real runs because `original_start_time` and `cancel_requested` are always defined.
    - Leave `last_reset_time`, `reset_run_id`, `request_id_infos` default with comments naming owning features.
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_
  - [x] 3.6 Preserve `pending_children`, `pending_workflow_task`, and empty `callbacks`
    - Keep existing child and WFT builders; leave `original_scheduled_time` default.
    - Emit `callbacks` as an empty list even when placeholder `CompletionCallback` entries exist in kernel state, because the placeholder carries no representable callback URL, trigger, state, or timing data.
    - _Requirements: 4.1, 4.2, 5.1, 5.2_

- [x] 4. Wire the snapshot construction sites
  - [x] 4.1 Populate the new fields in `apps/tokeirad/src/lib.rs` `describe_execution`
    - Build execution config, parent/root/first-run/timing fields, pending Nexus operations, extended info, and activity last-failure/pause from the loaded `WorkflowState` - all from the single snapshot.
    - _Requirements: 8.1, 8.2, 8.3_
  - [x] 4.2 Update the test resolver in `crates/tokeira-edge/tests/grpc_new_endpoints.rs`
    - Mirror the new field population so integration tests exercise the real builder.
    - _Requirements: 8.1_

- [x] 5. Handler and error mapping
  - [x] 5.1 Thread `run_id` through describe request handling
    - In `crates/tokeira-edge/src/grpc/translate.rs`, read `DescribeWorkflowExecutionRequest.execution.run_id`, validate non-empty values before resolution, and map malformed values to `INVALID_ARGUMENT`.
    - Add `run_id: Option<String>` to `DescribeWorkflowExecutionRequest` in `crates/tokeira-edge/src/translate/mod.rs`.
    - Add `run_id: Option<&str>` (or `Option<RunId>`) to `ExecutionResolver::describe_execution`.
    - Update `InMemoryExecutionResolver` and `StoreExecutionResolver` so a supplied valid run ID resolves exactly that run, while an absent run ID keeps current-run behavior.
    - Add a test asserting exact-run lookup with a valid historical run ID and current-run fallback when `run_id` is empty.
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_
  - [x] 5.2 Verify gRPC mapping and metrics
    - Confirm `grpc/errors.rs` maps the expected errors and `grpc_error_code` emits `invalid_argument` and `not_found`; confirm success metrics labels.
    - _Requirements: 10.1, 10.2, 10.3_

- [x] 6. Tests
  - [x] 6.1 Unit tests for each response section in `crates/tokeira-edge/src/grpc/translate.rs`
    - Execution config; full execution info (parent/root/first-run/timing, closed-run duration); pending activity last-failure/pause; pending Nexus; extended info expiration/cancel/original-start and always-emitted behavior for real runs.
    - Assert `workflow_extended_info` is always emitted for real runs with `original_start_time` and `cancel_requested`.
    - Assert `callbacks` empty even when placeholder kernel callbacks exist, and assert versioning/user-metadata default.
    - _Requirements: 1.1-1.5, 2.1-2.7, 3.1-3.6, 6.1-6.3, 7.1-7.7_
  - [x] 6.2 Property tests for snapshot fidelity
    - **Property 1:** Single Snapshot Consistency.
    - **Property 2:** Pending Activity Fidelity.
    - **Property 3:** Pending Nexus Fidelity.
    - **Property 4:** Extended Info Derivation.
    - **Property 5:** No Invented Identifiers.
    - _Requirements: 3.1-3.4, 5.2, 6.1, 6.2, 7.2, 7.3, 7.4, 7.7, 8.1, 8.2, 8.3_
  - [x] 6.3 Integration tests in `crates/tokeira-edge/tests/grpc_new_endpoints.rs`
    - Start a workflow, schedule an activity, assert the activity and pending WFT appear together (Requirement 8.4).
    - Assert a plain running workflow with no timeouts/pause/cancel emits `workflow_extended_info` carrying `original_start_time`, `cancel_requested = false`, and no `pause_info`.
    - Assert a closed run reports `execution_duration`.
    - _Requirements: 8.4, 7.7, 2.3_
  - [x] 6.4 gRPC error and metrics tests
    - **Property 6:** Expected Error Mapping (malformed run id, exact-run not found, current-run not found, metrics labels).
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2_

- [x] 7. Update UNSUPPORTED_FIELDS.md
  - [x] 7.1 Revise the `DescribeWorkflowExecutionResponse` entry
    - Remove fields now populated; retain only owned-elsewhere fields with their owning feature named (user_metadata, versioning, callbacks, reset/request-id, priority, external payloads, activity heartbeat fields).
    - _Requirements: 1.5, 2.7, 3.5, 6.3, 7.6_

- [x] 8. Checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo test -p tokeira-kernel`.
  - Run `cargo test -p tokeira-edge`.
  - Run `cargo lint`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "1.4", "3.1", "3.2", "3.3", "3.4", "3.5", "3.6"] },
    { "id": 2, "tasks": ["4.1", "4.2", "5.1"] },
    { "id": 3, "tasks": ["5.2", "6.1", "6.2", "6.3", "6.4", "7.1"] },
    { "id": 4, "tasks": ["8"] }
  ]
}
```

## Notes

- Target state is `Implemented`: every one of the eight response fields is accounted for, populated from authoritative state or truthfully empty with a named owning feature. The RPC never returns `UNIMPLEMENTED`.
- Owned-elsewhere fields (user metadata, versioning/deployment, callbacks, reset/request-id, priority, external payloads, activity heartbeat timing) are modeled as `Option`/empty in the DTO so they surface automatically the moment their owning feature lands - no further describe work required.
- Kernel changes are limited to serializable describe metadata (`cancel_requested` and stored root fields); the kernel stays pure (no I/O, async, metrics, or storage).
- Property tests are required, not optional. They are externally-visible correctness contracts for the describe surface.
- Follow the existing free-function translation pattern in `grpc/translate.rs`; do not introduce `TryFrom` conversions.
