# Implementation Plan: Edge Eager Dispatch

## Overview

Three-phase implementation: Phase 1 adds eager WFT on `StartWorkflowExecution`, Phase 2 adds eager activity tasks on `RespondWorkflowTaskCompleted`, Phase 3 adds broker-level atomic claim methods that the edge layer calls. Implementation order is Phase 3 → Phase 1 → Phase 2 so the broker primitives exist before the edge layer uses them.

## Tasks

- [ ] 1. Broker atomic claim methods and worker registry check
  - [ ] 1.1 Add `try_claim_workflow_task` to `InMemoryBroker`
    - Add a public `async fn try_claim_workflow_task(&self, queue: &QueueKey, run_key: RunKey) -> Option<DispatchableWorkflowTask>` method
    - Acquire the inner `Mutex`, scan `general_ready` for the given queue key for a task matching `run_key`, remove it with `VecDeque::remove`, remove from `enqueued` dedup set, return the task
    - Return `None` if no matching task found (not just queue empty — must match the specific run_key)
    - Do NOT call `notify.notify_waiters()` — this is a claim, not a publish
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [ ] 1.2 Add `try_claim_activity_task` to `InMemoryActivityBroker`
    - Add a public `async fn try_claim_activity_task(&self, queue: &QueueKey, run_key: RunKey, activity_id: &str) -> Option<DispatchableActivityTask>` method
    - Acquire the inner `Mutex`, scan `ready` for the given queue key for a task matching `(run_key, activity_id)`, remove it, remove from `enqueued` dedup set, return the task
    - Return `None` if no matching task found
    - Do NOT call `notify.notify_waiters()`
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

  - [ ] 1.3 Add `has_active_poller` to `PollerRegistry`
    - Add `pub fn has_active_poller(&self, queue: &QueueKey, worker_identity: &WorkerIdentity) -> bool`
    - Iterate `self.pollers(queue)` and check if any entry's `identity` matches `worker_identity`
    - The `PollerRegistry` is the authoritative source for active poll liveness (NOT `WorkerRegistry`)
    - _Requirements: 2.1, 2.2, 2.3_

  - [ ]* 1.4 Write property test for `try_claim_workflow_task` (targeted broker claim correctness)
    - **Property 3: Workflow broker targeted claim correctness**
    - Generate random `QueueKey` values and task sequences with different `RunKey` values, publish to broker, call `try_claim_workflow_task` with a specific `run_key`, verify only the matching task is returned and others remain in the queue
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4**

  - [ ]* 1.5 Write property test for `try_claim_activity_task` (targeted activity broker claim correctness)
    - **Property 4: Activity broker targeted claim correctness**
    - Generate random `QueueKey` values and activity task sequences with different `(RunKey, activity_id)` pairs, publish to broker, call `try_claim_activity_task` with a specific identity, verify only the matching task is returned and others remain
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4**

  - [ ]* 1.6 Write property test for claimed task exclusion from normal polling
    - **Property 9: Claimed task is not delivered to normal pollers**
    - Publish a workflow task, claim it via `try_claim_workflow_task`, then poll — verify the poll does not return the claimed task
    - **Validates: Requirements 9.2, 9.4**

  - [ ]* 1.7 Write property test for claimed activity task exclusion from normal polling
    - **Property 10: Claimed activity task is not delivered to normal pollers**
    - Publish an activity task, claim it via `try_claim_activity_task`, then poll — verify the poll does not return the claimed task
    - **Validates: Requirements 10.2, 10.4**

  - [ ]* 1.8 Write property test for `has_active_poller` lookup correctness
    - **Property 2: Compatible poller lookup correctness**
    - Generate random queue keys and worker identities, register some via `PollerRegistry::register`, verify `has_active_poller` returns `true` iff the identity is actively registered on that queue. Drop `PollerGuard` and verify `has_active_poller` returns `false`.
    - **Validates: Requirements 2.2, 2.3**

- [ ] 2. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 3. Phase 1 — Eager WFT on StartWorkflowExecution
  - [ ] 3.1 Add `request_eager_execution` to DTOs and proto translation
    - Add `request_eager_execution: bool` field to `StartWorkflowExecutionRequest` in `translate/mod.rs`
    - Add `eager_workflow_task: Option<PollWorkflowTaskQueueResponse>` field to `StartWorkflowExecutionResponse` in `translate/mod.rs`
    - Parse `request_eager_execution` from the proto in `start_request_to_edge` in `grpc/translate.rs`
    - Serialize `eager_workflow_task` in `start_response_to_proto` using the existing `poll_response_to_proto` path
    - _Requirements: 1.1, 1.2, 4.1, 4.2_

  - [ ] 3.2 Implement eager WFT dispatch in `start_workflow_execution`
    - In the `Started` branch of `start_workflow_execution` in `workflow_service.rs`, after the existing response construction:
    - Check `request_eager_execution` on the request
    - If true, call `self.poller_registry.has_active_poller(&queue_key, &identity)`
    - If compatible, call `self.broker.try_claim_workflow_task(&queue_key, run_key)` — targeted by the just-started workflow's run_key
    - If claimed, build a `PollWorkflowTaskQueueResponse` using `from_internal::poll_response` and attach to the response's `eager_workflow_task` field
    - _Requirements: 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4, 3.5, 12.1_

  - [ ]* 3.3 Write property test for `request_eager_execution` flag preservation
    - **Property 1: request_eager_execution flag preservation**
    - Generate random `StartWorkflowExecutionRequest` protos with varying `request_eager_execution` values, translate to internal DTO and back, verify flag is preserved
    - **Validates: Requirements 1.1, 1.2**

  - [ ]* 3.4 Write property test for start response proto translation
    - **Property 7: Start response proto translation preserves eager_workflow_task**
    - Generate internal `StartWorkflowExecutionResponse` with and without `eager_workflow_task`, translate to proto, verify the field is set iff the internal response has `Some`
    - **Validates: Requirements 4.1, 4.2**

- [ ] 4. Phase 2 — Eager activity tasks on RespondWorkflowTaskCompleted
  - [ ] 4.1 Add eager activity DTO fields and config
    - Add `activity_tasks: Vec<PollActivityTaskQueueResponse>` field to `RespondWorkflowTaskCompletedResponse` in `translate/mod.rs`
    - Thread `request_eager_execution: bool` through the `ScheduleActivityTask` command representation in the kernel
    - Add `EagerDispatchConfig` with `max_eager_activity_tasks_per_response: usize` (default 3) to the edge layer config
    - Serialize `activity_tasks` in `completed_response_to_proto` using the existing `poll_activity_response_to_proto` path in `grpc/translate.rs`
    - _Requirements: 5.1, 5.2, 5.3, 7.3, 8.1, 8.2_

  - [ ] 4.2 Implement eager activity dispatch in `respond_workflow_task_completed`
    - After the runtime commit in `respond_workflow_task_completed` in `workflow_service.rs`:
    - Collect eager-eligible activity commands (those with `request_eager_execution=true`)
    - For each eligible command (up to `max_eager_activity_tasks_per_response`), call `self.activity_broker.try_claim_activity_task(&queue_key, run_key, &activity_id)` — targeted by the specific activity's identity
    - For each claimed task, build a `PollActivityTaskQueueResponse` using `from_internal::poll_activity_response`
    - Attach the list to the response's `activity_tasks` field
    - _Requirements: 5.1, 5.2, 6.1, 6.2, 6.3, 6.4, 6.5, 7.1, 7.2, 12.2, 12.3_

  - [ ]* 4.3 Write property test for eager activity flag threading
    - **Property 6: Eager activity flag threading through commands**
    - Generate `ScheduleActivityTask` commands with varying `request_eager_execution` values, translate through the internal command representation, verify flag preservation
    - **Validates: Requirements 5.1, 5.2, 5.3**

  - [ ]* 4.4 Write property test for eager activity task limit enforcement
    - **Property 5: Eager activity task limit enforcement**
    - Generate random counts of eager-eligible activity commands (0..20), configure various max limits, verify the response never exceeds the configured maximum
    - **Validates: Requirements 7.1, 7.2**

  - [ ]* 4.5 Write property test for complete response proto translation
    - **Property 8: Complete response proto translation preserves activity_tasks**
    - Generate internal `RespondWorkflowTaskCompletedResponse` with varying `activity_tasks` lists, translate to proto, verify the repeated field has the same count
    - **Validates: Requirements 8.1, 8.2**

- [ ] 5. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Implementation order is Phase 3 (broker) → Phase 1 (start) → Phase 2 (complete) so primitives exist before consumers
- All property tests use `proptest` with minimum 100 iterations, tagged `Feature: edge-eager-dispatch, Property {N}: {title}`
- Existing timeout scanners handle recovery for eagerly claimed tasks — no new recovery mechanisms needed (Requirement 11)
- The `try_claim_*` methods mirror the existing `try_take` pattern but skip sticky-tier and worker-deny logic
