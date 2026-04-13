# Implementation Plan: Atomic SignalWithStart and Workflow ID Conflict Resolution

## Overview

Implement atomic signal-with-start and workflow ID conflict resolution across three layers: kernel (pure, no I/O), runtime (async, conflict resolution), and edge (gRPC translation, policy migration). Tasks are ordered kernel-first so each layer builds on the previous one.

## Tasks

- [x] 1. Add policy enums and `SignalWithStartRequest` to the kernel
  - [x] 1.1 Add `WorkflowIdConflictPolicy` and `WorkflowIdReusePolicy` enums to `tokeira-kernel/src/command.rs`
    - Add `WorkflowIdConflictPolicy` with variants `Fail`, `UseExisting`, `TerminateExisting`
    - Add `WorkflowIdReusePolicy` with variants `AllowDuplicate`, `AllowDuplicateFailedOnly`, `RejectDuplicate`
    - Both enums derive `Clone, Copy, Debug, PartialEq, Eq`
    - _Requirements: 2.1, 3.1_

  - [x] 1.2 Add `SignalWithStartRequest` struct to `tokeira-kernel/src/command.rs`
    - Include all `StartRequest` fields plus `signal_name: String`, `signal_input: Payloads`
    - Signal header is out of scope — `HistoryEventKind::WorkflowExecutionSignaled` does not carry a header field
    - _Requirements: 1.1_

  - [x] 1.3 Add `Command::SignalWithStart(SignalWithStartRequest)` variant to the `Command` enum
    - _Requirements: 1.1_

  - [x] 1.4 Add conflict policy fields to `StartRequest`
    - Add `conflict_policy: WorkflowIdConflictPolicy` and `reuse_policy: WorkflowIdReusePolicy` fields
    - Update ALL `StartRequest` construction sites discovered by compiler fallout — this includes golden tests, property tests, `to_internal.rs`, runtime publishers/lane helpers, and any other test modules. Supply default values: `Fail` for conflict, `AllowDuplicate` for reuse.
    - _Requirements: 4.3_

- [x] 2. Implement `apply_signal_with_start` on `BasicKernel`
  - [x] 2.1 Add `apply_signal_with_start` method to `BasicKernel` in `tokeira-kernel/src/kernel.rs`
    - Only handle `LoadedRun::Absent` — reject `LoadedRun::Existing` with `Reject::RunAlreadyExists`
    - Reuse the same `WorkflowState` initialization as `apply_start`
    - Produce 3 events: `WorkflowExecutionStarted` (event_id=1), `WorkflowExecutionSignaled` (event_id=2), `WorkflowTaskScheduled` (event_id=3)
    - Set `next_state.status = Running`, populate `pending_workflow_task`, emit `DispatchOp::EnqueueWorkflowTask`, `RequestDedupeOp`, and `ProjectionOp::UpsertExecution`
    - _Requirements: 1.2, 1.3, 5.1_

  - [x] 2.2 Add dispatch arm for `Command::SignalWithStart` in the `apply` method
    - Route to `self.apply_signal_with_start(loaded, req)`
    - _Requirements: 1.1_

  - [x] 2.3 Export `SignalWithStartRequest`, `WorkflowIdConflictPolicy`, and `WorkflowIdReusePolicy` from `tokeira-kernel/src/lib.rs`
    - _Requirements: 1.1, 2.1, 3.1_

- [x] 3. Checkpoint — Kernel compiles and existing tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Add kernel golden tests for signal-with-start
  - [x] 4.1 Add golden test `signal_with_start_from_absent` in `tokeira-kernel/tests/golden_tests.rs`
    - Construct a `SignalWithStartRequest`, apply to `LoadedRun::Absent`, verify 3 events with correct kinds and event_ids, verify `next_state.status == Running`, verify dispatch ops
    - _Requirements: 1.2, 1.3, 5.1_

  - [x] 4.2 Add golden test `reject_signal_with_start_on_existing_run` in `tokeira-kernel/tests/golden_tests.rs`
    - Apply `SignalWithStartRequest` to `LoadedRun::Existing(running_state)`, verify `Err(Reject::RunAlreadyExists)`
    - _Requirements: 1.2_

  - [ ]* 4.3 Write property test for three-event history structure (Property 1)
    - **Property 1: Three-event history structure**
    - Generate random `SignalWithStartRequest` via proptest, apply to `LoadedRun::Absent`, verify exactly 3 events: `Started(1) → Signaled(2) → WFTScheduled(3)`
    - Use `#![cfg_attr(miri, ignore)]` and `proptest! { #[test] }` with 100+ cases
    - **Validates: Requirements 1.2, 5.1**

  - [ ]* 4.4 Write property test for field pass-through (Property 2)
    - **Property 2: Field pass-through**
    - Generate random `SignalWithStartRequest`, apply to `Absent`, verify `WorkflowExecutionSignaled` contains exact `signal_name` and `signal_input` from request, and `WorkflowExecutionStarted` contains exact `workflow_type`, `task_queue`, `input`
    - **Validates: Requirements 5.2, 5.3**

- [x] 5. Implement runtime conflict resolution
  - [x] 5.1 Add `resolve_conflict` function and `ConflictResolution` enum to `tokeira-runtime/src/runtime.rs`
    - Add `ConflictResolution` enum with variants: `Absent`, `UseExisting { run_key, run_id }`, `TerminateAndStart { run_key }`, `ClosedAllowReuse`, `Rejected { message }`
    - Implement `resolve_conflict(existing_status: Option<ExecutionStatus>, run_key, run_id, conflict_policy, reuse_policy) -> ConflictResolution` matching the state transition table from the design
    - The runtime calls this AFTER the two-step resolve → load: first `repo.resolve_execution()` to get `Option<RunKey>`, then `repo.load_run()` to get `ExecutionStatus`
    - For closed workflows, treat `Failed`, `Cancelled`, `Terminated`, `TimedOut` as failed statuses for `AllowDuplicateFailedOnly`
    - _Requirements: 2.1–2.7, 3.1–3.6_

  - [x] 5.2 Add `SignalWithStartResult` and `StartWorkflowResult` enums to `tokeira-runtime/src/runtime.rs`
    - `SignalWithStartResult` variants: `Started { run_id }`, `Signaled { run_id }` (for UseExisting path)
    - `StartWorkflowResult` variants: `Created { run_id }`, `AlreadyRunning { run_id }` (for UseExisting on plain Start)
    - _Requirements: 2.3, 2.5_

  - [x] 5.3 Add `signal_with_start_workflow` method to `TokeiraRuntime`
    - Accept `SignalWithStartRequest` with policies
    - Call `resolve_conflict` on the loaded run
    - `Absent` / `ClosedAllowReuse` → kernel `apply_signal_with_start` → commit → `Started`
    - `UseExisting` → kernel `apply_signal` → commit → `Signaled`
    - `TerminateAndStart` → terminate existing (first commit) → kernel `apply_signal_with_start` (second commit) → `Started`. Do not return success until both commits complete.
    - `Rejected` → return error
    - _Requirements: 1.4, 2.2–2.6, 3.2–3.5_

  - [ ]* 5.4 Write property test for conflict resolution correctness (Property 3)
    - **Property 3: Conflict resolution correctness**
    - Generate random `(ExecutionStatus, WorkflowIdConflictPolicy, WorkflowIdReusePolicy)` tuples, verify `resolve_conflict` output matches the state transition table
    - **Validates: Requirements 2.1–2.7, 3.1–3.6**

- [x] 6. Checkpoint — Runtime compiles and existing tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Add edge policy extraction and migration
  - [x] 7.1 Add policy extraction and migration functions in `tokeira-edge/src/grpc/translate.rs`
    - Add `extract_conflict_policy(proto_value: i32) -> WorkflowIdConflictPolicy` — map proto enum values, default to `Fail`
    - Add `extract_reuse_policy(proto_value: i32) -> WorkflowIdReusePolicy` — map proto enum values, default to `AllowDuplicate`
    - Add `migrate_reuse_policy(reuse: &mut WorkflowIdReusePolicy, conflict: &mut WorkflowIdConflictPolicy)` — migrate deprecated `TERMINATE_IF_RUNNING` to `TerminateExisting` + `AllowDuplicate`
    - _Requirements: 4.1, 4.2_

  - [x] 7.2 Update `signal_with_start_request_to_edge` in `grpc/translate.rs` to extract policies from proto
    - Extract `workflow_id_conflict_policy` and `workflow_id_reuse_policy` from the proto request
    - Apply migration, apply defaults (`UseExisting` for conflict on SignalWithStart)
    - Thread policies through to the edge DTO
    - _Requirements: 4.1, 4.2, 2.7_

  - [x] 7.3 Update `start_request_to_edge` in `grpc/translate.rs` to extract policies from proto
    - Extract and migrate policies, default conflict to `Fail` for Start
    - _Requirements: 4.1, 4.2, 2.7_

  - [x] 7.4 Add policy fields to `SignalWithStartWorkflowExecutionRequest` and `StartWorkflowExecutionRequest` edge DTOs in `translate/mod.rs`
    - Add `conflict_policy: WorkflowIdConflictPolicy` and `reuse_policy: WorkflowIdReusePolicy` to both DTOs
    - _Requirements: 4.3_

  - [ ]* 7.5 Write property test for policy migration (Property 4)
    - **Property 4: Policy migration**
    - Generate random proto policy values including deprecated `TERMINATE_IF_RUNNING`, verify migration produces `TerminateExisting` + `AllowDuplicate`
    - **Validates: Requirements 4.2**

- [x] 8. Wire edge to runtime for signal-with-start
  - [x] 8.1 Add `signal_with_start_workflow` method to `WorkflowRuntimeApi` trait in `tokeira-edge/src/workflow_service.rs`
    - Accept a kernel `SignalWithStartRequest`, return `Result<SignalWithStartResult>`
    - _Requirements: 4.4_

  - [x] 8.2 Implement `signal_with_start_workflow` on `RuntimeAdapter` in `grpc/runtime_adapter.rs`
    - Delegate to `self.runtime.signal_with_start_workflow(req)`
    - _Requirements: 4.4_

  - [x] 8.3 Rewrite `signal_with_start_workflow_execution` in `WorkflowService` to use the new runtime method
    - Replace the current two-step start+signal logic with: build `SignalWithStartRequest` from edge DTO (including policies), call `self.runtime.signal_with_start_workflow(req)`, map `SignalWithStartResult` to `SignalWithStartWorkflowExecutionResponse`
    - The runtime now owns the conflict resolution branching — the edge method becomes a thin translation layer
    - _Requirements: 1.4, 2.5, 2.6, 4.4_

  - [x] 8.4 Update `to_internal.rs` with `signal_with_start_request` conversion function
    - Add `pub fn signal_with_start_request(req, request_id) -> SignalWithStartRequest` that builds the kernel request from the edge DTO, including policies
    - _Requirements: 4.3_

- [x] 9. Checkpoint — Full stack compiles and all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 10. Add runtime and edge tests
  - [ ]* 10.1 Write runtime unit tests for `signal_with_start_workflow`
    - Test absent workflow → `Started` with 3-event history
    - Test running workflow + `UseExisting` → `Signaled` with signal delivered
    - Test running workflow + `Fail` → error
    - Test running workflow + `TerminateExisting` → old terminated, new started
    - Test closed completed + `AllowDuplicate` → new run
    - Test closed completed + `AllowDuplicateFailedOnly` → error
    - Test closed failed + `AllowDuplicateFailedOnly` → new run
    - Test closed + `RejectDuplicate` → error
    - _Requirements: 2.2–2.6, 3.2–3.5_

  - [ ]* 10.2 Write edge unit tests for policy extraction and migration
    - Test `extract_conflict_policy` maps all proto values correctly
    - Test `extract_reuse_policy` maps all proto values correctly
    - Test `migrate_reuse_policy` converts `TERMINATE_IF_RUNNING` correctly
    - Test default policies: `UseExisting` for SignalWithStart, `Fail` for Start
    - _Requirements: 4.1, 4.2, 2.7_

- [ ] 11. Final checkpoint — All tests pass
  - Run `cargo test` and `cargo lint` to verify all existing and new tests pass.
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- The kernel is pure — `apply_signal_with_start` only handles `LoadedRun::Absent`; the runtime owns all conflict resolution branching
- Policy migration (`TERMINATE_IF_RUNNING` → `TerminateExisting` + `AllowDuplicate`) happens at the edge layer
- Default policies: `UseExisting` for SignalWithStart conflict, `Fail` for Start conflict, `AllowDuplicate` for reuse
