# Implementation Plan: Child Workflow Orchestration

## Overview

Wire the runtime's `DispatchPublisher` to handle the three child workflow dispatch ops (`StartChildWorkflow`, `TerminateChild`, `CancelChild`) and deliver child resolution results back to the parent workflow. This replaces the current stub logging in `RuntimeDispatchPublisher` with working implementations. The central change is extending `StartRequest`, `WorkflowState`, and `DispatchOp::StartChildWorkflow` with parent identity fields, extending `RuntimeDispatchPublisher` with lane access for cross-run command submission, and adding child resolution detection in the lane's post-commit path.

## Tasks

- [x] 1. Extend kernel types with parent identity fields
  - [x] 1.1 Add `parent_run_key` and `parent_workflow_id` to `StartRequest` in `tokeira/crates/tokeira-kernel/src/command.rs`
    - Add `pub parent_run_key: Option<RunKey>` and `pub parent_workflow_id: Option<WorkflowId>` fields
    - _Requirements: 6.1_

  - [x] 1.2 Add `parent_run_key`, `parent_workflow_id`, `close_result`, and `close_failure` to `WorkflowState` in `tokeira/crates/tokeira-kernel/src/state.rs`
    - Add `pub parent_run_key: Option<RunKey>` and `pub parent_workflow_id: Option<WorkflowId>` fields
    - Add `pub close_result: Option<Payloads>` and `pub close_failure: Option<String>` fields
    - _Requirements: 6.1, 6.2, 9.1, 9.2_

  - [x] 1.3 Update `apply_start` in `tokeira/crates/tokeira-kernel/src/kernel.rs` to populate parent fields
    - Set `parent_run_key` and `parent_workflow_id` on the initial `WorkflowState` from the `StartRequest`
    - Set `close_result: None` and `close_failure: None` on the initial state
    - _Requirements: 6.1, 9.5_

  - [x] 1.3a Update kernel `close()` path to populate close details
    - In the `CompleteWorkflow` handler, set `close_result = Some(result)` on the state before closing
    - In the `FailWorkflow` handler, set `close_failure = Some(message)` on the state before closing
    - For all other close paths (Cancel, Terminate, TimedOut, ContinuedAsNew), leave both as `None`
    - _Requirements: 9.3, 9.4, 9.5_

  - [x] 1.4 Add `parent_run_key`, `parent_workflow_id`, and `initiated_event_id` to `DispatchOp::StartChildWorkflow` in `tokeira/crates/tokeira-kernel/src/transition.rs`
    - Add `parent_run_key: RunKey`, `parent_workflow_id: WorkflowId`, `initiated_event_id: i64` fields to the variant
    - _Requirements: 1.6_

  - [x] 1.5 Update `apply_workflow_command` for `WorkflowCommand::StartChildWorkflow` in `tokeira/crates/tokeira-kernel/src/kernel.rs`
    - Populate `parent_run_key` from `builder.state.run_key`, `parent_workflow_id` from `builder.state.workflow_id.clone()`, and `initiated_event_id` from the return value of `builder.emit(...)` in the `DispatchOp::StartChildWorkflow` push
    - _Requirements: 1.6_

  - [x] 1.6 Fix all compilation errors from the new fields
    - Update all `StartRequest` construction sites to include `parent_run_key: None` and `parent_workflow_id: None` (tests, runtime, examples)
    - Update all `WorkflowState` construction sites to include `parent_run_key: None` and `parent_workflow_id: None` (tests, `sample_state` helpers)
    - Update all `DispatchOp::StartChildWorkflow` match arms and construction sites
    - _Requirements: 6.1_

- [x] 2. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Extend `DispatchPublisher` trait and `RuntimeDispatchPublisher` with lane access
  - [x] 3.1 Add `submit_to_run` method to `DispatchPublisher` trait in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - Add `async fn submit_to_run(&self, run_key: RunKey, command: Command) -> Result<CommitResult>` to the trait
    - This is used for child resolution delivery from the lane's post-commit path
    - _Requirements: 4.1, 4.2_

  - [x] 3.2 Add `lanes`, `lane_count`, and `repo` fields to `RuntimeDispatchPublisher` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Add `lanes: Vec<LaneHandle>`, `lane_count: usize`, and `repo: Arc<R>` fields
    - Update `RuntimeDispatchPublisher::new` to accept and store these
    - Add a private `fn pick_lane(&self, run_key: RunKey) -> &LaneHandle` helper
    - Add a private `async fn resolve_child_run_key(&self, namespace_id, child_workflow_id, child_run_id) -> Result<Option<RunKey>>` helper using `repo.resolve_execution`
    - _Requirements: 1.1, 2.1, 3.1, 4.1, 10.1, 10.2_

  - [x] 3.3 Implement `submit_to_run` for `RuntimeDispatchPublisher`
    - Route via `self.pick_lane(run_key).submit(run_key, command).await`
    - _Requirements: 4.1, 4.2_

  - [x] 3.4 Update `TokeiraRuntime::new` to pass `lanes.clone()` and `lane_count` to `RuntimeDispatchPublisher::new`
    - The lanes must be created first, then cloned into each publisher
    - This requires restructuring `new` so lanes are created in two passes: first create the lane handles, then create publishers referencing all lanes, then wire publishers into lanes
    - Alternatively, create publishers with empty lanes first and update them after lane creation (if the publisher is `Clone` and uses `Arc`)
    - _Requirements: 1.1_

  - [x] 3.5 Update `MockPublisher` in lane tests to implement `submit_to_run`
    - Add a default implementation or mock that returns `CommitResult::Applied` with a dummy state
    - _Requirements: 4.1_

  - [x] 3.6 Fix all compilation errors from the updated `RuntimeDispatchPublisher::new` signature
    - Update all call sites in tests and runtime code
    - _Requirements: 1.1_

- [x] 4. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement `StartChildWorkflow` dispatch handling in publisher
  - [x] 5.1 Implement the `StartChildWorkflow` match arm in `RuntimeDispatchPublisher::publish`
    - Construct a `StartRequest` for the child with: fresh `RunKey::new()` and `RunId::new()`, fields from the dispatch op (`namespace_id`, `child_workflow_id` as `workflow_id`, `workflow_type`, `task_queue`, `input`), `parent_run_key: Some(parent_run_key)`, `parent_workflow_id: Some(parent_workflow_id)`, `workflow_task_timeout: Duration::seconds(10)`, defaults for `memo`, `search_attributes`, `retry_policy` (None), `workflow_execution_timeout` (None), `workflow_run_timeout` (None), `attempt: 1`, `continued_execution_run_id: None`, `first_execution_run_id: None`
    - Submit `Command::Start` to the child's lane via `self.pick_lane(child_run_key).submit(...)`
    - On `CommitResult::Applied`: build `ChildStartResult::Started { child_run_id, workflow_type }`
    - On `CommitResult::Conflict` or `Err`: build `ChildStartResult::Failed { cause }` with error description
    - On `CommitResult::Duplicate`: build `ChildStartResult::Failed { cause: "duplicate start request" }` — we don't know the actual child RunId, so treat as failure; the sweeper (Feature 11) will reconcile if needed
    - Submit `Command::ChildStartConfirmed(ChildStartConfirmedRequest { child_workflow_id, initiated_event_id, result, now })` to the parent's lane via `self.pick_lane(parent_run_key).submit(...)`
    - On confirmation delivery failure: log at warn level
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 6.1, 7.1, 7.2, 8.1, 8.2, 8.3_

  - [x] 5.2 Write property test for child StartRequest construction (Property 1)
    - **Property 1: Child StartRequest construction**
    - Generate random `DispatchOp::StartChildWorkflow` values with random `namespace_id`, `workflow_type`, `task_queue`, `input`, `parent_run_key`, `parent_workflow_id`, and `initiated_event_id`
    - Use a mock lane that captures the `Command::Start` submitted to it
    - Verify all field mappings: `workflow_id == child_workflow_id`, `namespace_id`, `workflow_type`, `task_queue`, `input` match, `parent_run_key == Some(parent_run_key)`, `parent_workflow_id == Some(parent_workflow_id)`, `run_key` and `run_id` are freshly generated (not equal to parent's), `workflow_task_timeout == 10s`, defaults for memo/search_attributes/retry_policy/timeouts, `attempt == 1`, `continued_execution_run_id == None`, `first_execution_run_id == None`
    - **Validates: Requirements 1.1, 1.2, 1.3, 6.1, 8.1, 8.2, 8.3**

  - [x] 5.3 Write property test for successful child start confirmation (Property 2)
    - **Property 2: Successful child start produces Started confirmation**
    - Generate random dispatch ops; mock child lane returns `CommitResult::Applied`
    - Mock parent lane captures the `Command::ChildStartConfirmed`
    - Verify `Started` variant with correct `child_run_id`, `workflow_type`, and `initiated_event_id`
    - **Validates: Requirements 1.4, 1.6**

  - [x] 5.4 Write property test for failed child start confirmation (Property 3)
    - **Property 3: Failed child start produces Failed confirmation**
    - Generate random dispatch ops; mock child lane returns an error
    - Mock parent lane captures the `Command::ChildStartConfirmed`
    - Verify `Failed` variant with non-empty cause and correct `initiated_event_id`
    - **Validates: Requirements 1.5, 1.6, 7.1**

- [x] 6. Implement `TerminateChild` and `CancelChild` dispatch handling in publisher
  - [x] 6.1 Implement the `TerminateChild` match arm in `RuntimeDispatchPublisher::publish`
    - Resolve the child's `RunKey` via `self.resolve_child_run_key(namespace_id, &child_workflow_id, child_run_id)` using the repo
    - If resolution returns `None`: log at debug level (child not found, harmless no-op) and skip
    - Build `Command::Terminate(TerminateRequest { reason, ... })` with identity `"parent-close-policy"`
    - Submit the command to the child's lane via `self.pick_lane(child_run_key).submit(...)`
    - On kernel rejection (contains "kernel rejected") or not-found: log at debug level (harmless no-op)
    - On other errors: log at warn level
    - _Requirements: 2.1, 2.2, 2.3, 5.1, 5.3, 5.4, 7.4, 10.2, 10.3_

  - [x] 6.2 Implement the `CancelChild` match arm in `RuntimeDispatchPublisher::publish`
    - Build `Command::Cancel(CancelRequest { reason, ... })`
    - Same error handling pattern as `TerminateChild`
    - _Requirements: 3.1, 3.2, 3.3, 5.2, 5.3, 5.4, 7.4_

  - [x] 6.3 Write property test for TerminateChild and CancelChild dispatch (Property 4)
    - **Property 4: TerminateChild and CancelChild dispatch correct commands**
    - Generate random `TerminateChild` and `CancelChild` dispatch ops with random `child_run_id` and `reason`
    - Mock lanes capture submitted commands
    - Verify correct `Command::Terminate` or `Command::Cancel` is submitted with matching `reason`
    - **Validates: Requirements 2.1, 3.1, 5.1, 5.2**

  - [x] 6.4 Write property test for dispatch continues after individual failures (Property 6)
    - **Property 6: Dispatch continues after individual failures**
    - Generate random batches of `TerminateChild`/`CancelChild` ops and random failure patterns
    - Mock lanes configured to fail on specific ops
    - Verify all non-failing ops are still dispatched
    - **Validates: Requirements 5.3, 5.4, 7.4**

- [x] 7. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement child resolution detection in lane post-commit path
  - [x] 8.1 Add child resolution detection in `run_activation` in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - After a successful commit and dispatch op publication, check if `new_state.closed_at.is_some()` and `new_state.parent_run_key.is_some()` and `new_state.parent_workflow_id.is_some()`
    - If so, map `new_state.status` to the appropriate `ChildResolution` variant using close details:
      - `ExecutionStatus::Completed` → `ChildResolution::Completed { result: new_state.close_result.unwrap_or_default() }`
      - `ExecutionStatus::Failed` → `ChildResolution::Failed { failure: new_state.close_failure.unwrap_or("child workflow failed".into()) }`
      - `ExecutionStatus::Cancelled` → `ChildResolution::Canceled`
      - `ExecutionStatus::Terminated` → `ChildResolution::Terminated`
      - `ExecutionStatus::TimedOut` → `ChildResolution::TimedOut`
    - Build `Command::ChildResolved(ChildResolvedRequest { child_workflow_id: new_state.workflow_id, resolution, now })` and submit via `publisher.submit_to_run(parent_run_key, command)`
    - On failure: log at warn level (fire-and-forget)
    - On kernel rejection (parent closed or absent): log at debug level
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 7.3_

  - [x] 8.2 Write property test for child resolution delivery (Property 5)
    - **Property 5: Child resolution delivers correct mapping and routing**
    - Generate random terminal `ExecutionStatus` values and random parent identity (`parent_run_key`, `parent_workflow_id`)
    - Mock publisher captures the `Command::ChildResolved` submitted via `submit_to_run`
    - Verify correct `ChildResolution` variant, `child_workflow_id`, and routing to `parent_run_key`
    - **Validates: Requirements 4.1, 4.2, 4.5**

  - [x] 8.3 Write unit test: no resolution for non-child runs
    - Close a run with `parent_run_key = None`
    - Verify no `Command::ChildResolved` is submitted
    - _Requirements: 4.2_

- [x] 9. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Write property test for parent identity round-trip durability (Property 7)
  - [x] 10.1 Write property test for parent identity round-trip (Property 7)
    - **Property 7: Parent identity round-trip durability**
    - Generate random `RunKey` and `WorkflowId` values as parent identity
    - Create a child workflow with these as `parent_run_key` and `parent_workflow_id` in the `StartRequest`
    - Commit via `InMemoryStore`, reload the state
    - Verify `parent_run_key` and `parent_workflow_id` are preserved
    - **Validates: Requirements 6.2**

- [x] 11. Unit tests for edge cases
  - [x] 11.1 Write unit test: TerminateChild on closed child is no-op
    - Mock child lane returns kernel rejection (`Reject::RunClosed`)
    - Verify no error propagated, debug-level log expected
    - _Requirements: 2.2_

  - [x] 11.2 Write unit test: CancelChild on absent child is no-op
    - Mock child lane returns not-found error
    - Verify no error propagated, debug-level log expected
    - _Requirements: 3.2, 3.3_

  - [x] 11.3 Write unit test: ChildResolved when parent is closed is no-op
    - Mock parent lane returns kernel rejection (`Reject::RunClosed`)
    - Verify no error propagated, warn-level log expected
    - _Requirements: 4.3_

  - [x] 11.4 Write unit test: ChildStartConfirmed delivery failure is logged
    - Mock parent lane returns error on confirmation delivery
    - Verify error is logged at warn level, no crash
    - _Requirements: 7.2_

  - [x] 11.5 Write unit test: Duplicate child start treated as failure
    - Mock child lane returns `CommitResult::Duplicate`
    - Verify `ChildStartResult::Failed { cause: "duplicate start request" }` is sent to parent
    - _Requirements: 1.5_

- [x] 12. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Update re-exports in `lib.rs`
  - Ensure any new public types are re-exported from `tokeira/crates/tokeira-runtime/src/lib.rs`
  - _Requirements: 1.1_

- [x] 14. Integration tests
  - [x] 14.1 Write integration test: happy path child workflow lifecycle
    - Start a parent workflow via `TokeiraRuntime` with `InMemoryStore`
    - Complete a workflow task with a `WorkflowCommand::StartChildWorkflow` command
    - Verify the child run is created and the parent receives `ChildStartConfirmed::Started`
    - Complete the child workflow, verify the parent receives `ChildResolved::Completed`
    - _Requirements: 1.1, 1.4, 4.1, 4.5_

  - [x] 14.2 Write integration test: parent close policy Terminate
    - Start a parent with a child that has `ParentClosePolicy::Terminate`
    - Confirm the child start, then close the parent
    - Verify the child receives a `Command::Terminate` and closes
    - _Requirements: 2.1, 5.1_

  - [x] 14.3 Write integration test: parent close policy RequestCancel
    - Same as above but with `ParentClosePolicy::RequestCancel`
    - Verify the child receives a `Command::Cancel`
    - _Requirements: 3.1, 5.2_

  - [x] 14.4 Write integration test: child start failure delivers Failed confirmation
    - Configure the runtime so the child start fails (e.g., workflow ID already exists)
    - Verify the parent receives `ChildStartConfirmed::Failed`
    - _Requirements: 1.5, 7.1_

- [x] 15. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property tests are required (not optional) per project convention
- Each property test references a specific correctness property from the design document
- The implementation uses Rust with `proptest` for property-based testing, consistent with existing `tokeira-runtime` test infrastructure
- Property tests should run a minimum of 100 iterations (proptest default is 256)
- Tag format for property tests: `// Feature: runtime-child-workflows, Property N: <title>`
- The `RuntimeDispatchPublisher` restructuring (adding lane access) is the most structurally complex change — it requires lanes to be created before publishers, or a two-phase initialization
- Child resolution detection lives in the lane's post-commit path, not in the publisher's `publish` method, because the lane has access to `new_state` after commit
- `TerminateChild` and `CancelChild` use `child_run_id` (a `RunId`), which needs to be resolved to a `RunKey` for lane routing — this may require `repo.resolve_execution` or storing the child's `RunKey` in the dispatch op
