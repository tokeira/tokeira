# Implementation Plan: Worker Versioning and Deployment Routing

## Overview

Thread deployment and build_id version metadata through the Tokeira runtime's poll and dispatch paths. The InMemoryBroker already routes by exact QueueKey (including deployment/build_id), so the work is: (1) add a WorkerRegistry, (2) extend WorkflowState/ActivityState with version fields, (3) populate QueueKey in kernel dispatch ops from state, (4) propagate version metadata through the edge layer, and (5) fix activity retry version preservation.

## Tasks

- [x] 1. Add WorkerRegistry to tokeira-runtime
  - [x] 1.1 Create WorkerRegistry types and implementation
    - Create `WorkerRegistrationKey` struct with `worker_identity`, `namespace_id`, `task_queue` fields
    - Create `WorkerVersionMetadata` struct with `deployment: Option<DeploymentId>`, `build_id: Option<BuildId>`
    - Implement `WorkerRegistry` with `Arc<Mutex<HashMap<WorkerRegistrationKey, WorkerVersionMetadata>>>`
    - Implement `register(&self, key, metadata)` and `lookup(&self, key) -> WorkerVersionMetadata` (returns default None/None for unregistered)
    - Add the new module to `tokeira-runtime/src/lib.rs`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 1.2 Write property test for WorkerRegistry round-trip (Property 1)
    - **Property 1: Worker registration round-trip**
    - Generate random `(WorkerIdentity, NamespaceId, TaskQueueName, Option<DeploymentId>, Option<BuildId>)` tuples
    - Register, lookup, verify equality; re-register with different metadata, verify overwrite
    - Verify unregistered workers return `(None, None)`
    - **Validates: Requirements 1.2, 1.3, 1.4, 1.5**

  - [x] 1.3 Wire WorkerRegistry into TokeiraRuntime
    - Add `worker_registry: WorkerRegistry` field to `TokeiraRuntime`
    - Initialize in `new_with_nexus` constructor
    - Add `pub fn register_worker(...)` method delegating to `WorkerRegistry::register`
    - Add `pub fn worker_registry(&self) -> WorkerRegistry` accessor
    - _Requirements: 1.1, 1.2_

- [x] 2. Extend kernel state with version fields
  - [x] 2.1 Add deployment and build_id to StartRequest and WorkflowState
    - Add `pub deployment: Option<DeploymentId>` and `pub build_id: Option<BuildId>` to `StartRequest` in `tokeira-kernel/src/command.rs`
    - Add `pub deployment: Option<DeploymentId>` and `pub build_id: Option<BuildId>` to `WorkflowState` in `tokeira-kernel/src/state.rs`
    - Update the `WorkflowState` initialization in `apply_start` in `kernel.rs` to set these fields from `StartRequest`
    - Update all existing `StartRequest` and `WorkflowState` construction sites (tests, edge, runtime) to include the new fields (default `None`)
    - _Requirements: 6.1, 6.2, 6.3, 6.6_

  - [x] 2.2 Add deployment and build_id to ActivityState
    - Add `pub deployment: Option<DeploymentId>` and `pub build_id: Option<BuildId>` to `ActivityState` in `tokeira-kernel/src/state.rs`
    - Set from `ScheduleActivity` workflow command, falling back to workflow state values
    - Update all existing `ActivityState` construction sites to include the new fields
    - _Requirements: 6.2_

- [x] 3. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Propagate version fields in kernel dispatch ops
  - [x] 4.1 Update `schedule_workflow_task()` in TransitionBuilder
    - In `TransitionBuilder::schedule_workflow_task()`, change `deployment: None, build_id: None` to `deployment: self.state.deployment.clone(), build_id: self.state.build_id.clone()`
    - _Requirements: 6.1, 6.3_

  - [x] 4.2 Update EnqueueActivityTask sites in kernel.rs
    - In `apply_workflow_command` `ScheduleActivity` arm: read deployment/build_id from the new `ActivityState` fields, falling back to `builder.state.deployment`/`build_id`
    - In `apply_unpause_workflow`: change `deployment: None, build_id: None` to read from activity state with workflow state fallback
    - In `apply_unpause_activity`: same change
    - In `apply_reset_activity`: same change
    - _Requirements: 6.2, 6.3_

  - [x] 4.3 Update EnqueueWorkflowTask sites for task failed/timed out
    - In `apply_workflow_task_failed`: change `deployment: None, build_id: None` to `deployment: builder.state.deployment.clone(), build_id: builder.state.build_id.clone()`
    - In `apply_workflow_task_timed_out`: same change
    - _Requirements: 6.1_

  - [x] 4.4 Write property test for kernel dispatch version propagation (Property 6)
    - **Property 6: Kernel dispatch op version propagation**
    - Generate `WorkflowState` with random `(Option<DeploymentId>, Option<BuildId>)`
    - Run kernel through `schedule_workflow_task` path, verify emitted `DispatchOp::EnqueueWorkflowTask` QueueKey carries state's deployment/build_id
    - For activity dispatch, generate activity-level overrides and verify fallback logic
    - **Validates: Requirements 6.1, 6.2, 6.3**

- [x] 5. Update edge layer for version metadata propagation
  - [x] 5.1 Add deployment and build_id to PollWorkflowTaskQueueRequest
    - Add `pub deployment: Option<String>` and `pub build_id: Option<String>` to `PollWorkflowTaskQueueRequest` in `tokeira-edge/src/translate/mod.rs`
    - Update `to_internal::poll_request` to map these into `QueueKey`: `deployment: req.deployment.map(DeploymentId)`, `build_id: req.build_id.map(BuildId)`
    - Treat empty strings as `None`
    - _Requirements: 3.1, 3.2_

  - [x] 5.2 Activity poll edge translation is deferred
    - No activity poll DTO or gRPC adapter path exists in the edge layer today
    - The runtime's `poll_activity_task` already accepts a `QueueKey` — the caller constructs it correctly
    - Activity poll edge translation will be added when the activity poll gRPC endpoint is created
    - _Requirements: 3.3_

  - [x] 5.3 Write property test for edge translation preservation (Property 3)
    - **Property 3: Edge translation preserves deployment and build_id**
    - Generate random `PollWorkflowTaskQueueRequest` with arbitrary `Option<String>` deployment/build_id
    - Call `to_internal::poll_request`, verify QueueKey fields match `req.deployment.map(DeploymentId)` and `req.build_id.map(BuildId)`
    - Verify empty strings map to `None`
    - **Validates: Requirements 3.1, 3.2, 3.3**

- [x] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Fix activity retry version preservation
  - [x] 7.1 Update retry_activity_task in runtime.rs
    - In `retry_activity_task`, change the hardcoded `deployment: None, build_id: None` in the retry `QueueKey` to read from the activity's state (falling back to workflow state deployment/build_id)
    - Load the activity's deployment/build_id from `ActivityState` or from `WorkflowState` when constructing the retry QueueKey
    - _Requirements: 8.1, 8.2_

  - [x] 7.2 Write property test for activity retry version preservation (Property 7)
    - **Property 7: Activity retry version preservation**
    - Generate an activity with non-None deployment/build_id in its original dispatch QueueKey
    - Fail it, trigger retry, verify the retry dispatch QueueKey preserves the original deployment and build_id
    - **Validates: Requirements 8.1, 8.2**

- [x] 8. Write broker routing isolation property tests
  - [x] 8.1 Write property test for broker routing isolation (Property 4)
    - **Property 4: Broker routing isolation**
    - Generate two QueueKeys sharing namespace/task_queue/task_kind but with different deployment/build_id
    - Publish a task to one, poll on the other, verify no delivery
    - Poll on the matching key, verify delivery
    - Test both `InMemoryBroker` (workflow) and `InMemoryActivityBroker` (activity)
    - **Validates: Requirements 4.1, 4.2, 4.5, 7.1, 7.2, 7.3, 7.4**

  - [x] 8.2 Write property test for versioned task holding and delivery (Property 5)
    - **Property 5: Versioned task holding and delivery**
    - Generate a versioned QueueKey and task, publish without a waiting poller
    - Then poll with matching key, verify the held task is delivered
    - Test both workflow and activity brokers
    - **Validates: Requirements 5.1, 5.2, 5.4**

- [x] 9. Write unit tests for edge cases
  - [x] 9.1 Write unit tests for edge layer and broker edge cases
    - Edge layer: empty-string deployment/build_id maps to `None`
    - Broker: unversioned task not delivered to versioned poller (concrete example)
    - Broker: versioned task not delivered to unversioned poller (concrete example)
    - Kernel: workflow with `None` deployment produces `None` in dispatch op (regression guard)
    - Activity retry: verify the specific bug fix where `retry_activity_task` previously hardcoded `deployment: None`
    - _Requirements: 3.2, 4.2, 7.2, 7.3, 8.2_

- [x] 10. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- The InMemoryBroker and InMemoryActivityBroker require no code changes — they already route by exact QueueKey including deployment/build_id
- The RuntimeDispatchPublisher requires no code changes — it forwards DispatchOp queue fields verbatim
- All property tests use `proptest` (already a project dependency) with a minimum of 100 iterations
- Each property test is tagged with `// Feature: runtime-worker-versioning, Property N: <title>`
- Tasks reference specific requirements for traceability
