# Implementation Plan: Edge Eager Dispatch

## Overview

Tasks 1-5 record the original eager workflow/activity implementation. Tier 3.18 proved that the workflow-start half used the wrong contract: it required a live `PollerRegistry` entry and reclaimed a published task, while v1.31.0 atomically starts the first WFT for the requesting caller. Tasks 6-10 are the reviewed correction and supersede the workflow-start portions of tasks 1 and 3. The activity path remains unchanged.

## Tasks

- [x] 1. Historical broker atomic-claim methods and poller lookup
  - [x] 1.1 Add `try_claim_workflow_task` to `InMemoryBroker`
    - Add a public `async fn try_claim_workflow_task(&self, queue: &QueueKey, run_key: RunKey) -> Option<DispatchableWorkflowTask>` method
    - Acquire the inner `Mutex`, scan `general_ready` for the given queue key for a task matching `run_key`, remove it with `VecDeque::remove`, remove from `enqueued` dedup set, return the task
    - Return `None` if no matching task found (not just queue empty — must match the specific run_key)
    - Do NOT call `notify.notify_waiters()` — this is a claim, not a publish
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 1.2 Add `try_claim_activity_task` to `InMemoryActivityBroker`
    - Add a public `async fn try_claim_activity_task(&self, queue: &QueueKey, run_key: RunKey, activity_id: &str) -> Option<DispatchableActivityTask>` method
    - Acquire the inner `Mutex`, scan `ready` for the given queue key for a task matching `(run_key, activity_id)`, remove it, remove from `enqueued` dedup set, return the task
    - Return `None` if no matching task found
    - Do NOT call `notify.notify_waiters()`
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

  - [x] 1.3 Add the legacy `has_active_poller` helper (removed by task 8.3 if unused)
    - Add `pub fn has_active_poller(&self, queue: &QueueKey, worker_identity: &WorkerIdentity) -> bool`
    - Iterate `self.pollers(queue)` and check if any entry's `identity` matches `worker_identity`
    - This helper was part of the superseded workflow-start gate; it is not a v1.31.0 eager-acceptance requirement

  - [x]* 1.4 Write property test for `try_claim_workflow_task` (targeted broker claim correctness)
    - **Property 13: Workflow broker targeted claim correctness**
    - Generate random `QueueKey` values and task sequences with different `RunKey` values, publish to broker, call `try_claim_workflow_task` with a specific `run_key`, verify only the matching task is returned and others remain in the queue
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4**

  - [x]* 1.5 Write property test for `try_claim_activity_task` (targeted activity broker claim correctness)
    - **Property 8: Activity broker targeted claim correctness**
    - Generate random `QueueKey` values and activity task sequences with different `(RunKey, activity_id)` pairs, publish to broker, call `try_claim_activity_task` with a specific identity, verify only the matching task is returned and others remain
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4**

  - [x]* 1.6 Write property test for claimed task exclusion from normal polling
    - **Property 12: Claimed task is not delivered to normal pollers**
    - Publish a workflow task, claim it via `try_claim_workflow_task`, then poll — verify the poll does not return the claimed task
    - **Validates: Requirements 9.2, 9.4**

  - [x]* 1.7 Write property test for claimed activity task exclusion from normal polling
    - **Property 12: Claimed activity task is not delivered to normal pollers**
    - Publish an activity task, claim it via `try_claim_activity_task`, then poll — verify the poll does not return the claimed task
    - **Validates: Requirements 10.2, 10.4**

  - [x]* 1.8 Write legacy coverage for `has_active_poller` lookup correctness
    - Generate random queue keys and worker identities, register some via `PollerRegistry::register`, verify `has_active_poller` returns `true` iff the identity is actively registered on that queue. Drop `PollerGuard` and verify `has_active_poller` returns `false`.
    - Historical coverage only; task 8.3 removes it with the helper if no caller remains

- [x] 2. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Phase 1 — Eager WFT on StartWorkflowExecution
  - [x] 3.1 Add `request_eager_execution` to DTOs and proto translation
    - Add `request_eager_execution: bool` field to `StartWorkflowExecutionRequest` in `translate/mod.rs`
    - Add `eager_workflow_task: Option<PollWorkflowTaskQueueResponse>` field to `StartWorkflowExecutionResponse` in `translate/mod.rs`
    - Parse `request_eager_execution` from the proto in `start_request_to_edge` in `grpc/translate.rs`
    - Serialize `eager_workflow_task` in `start_response_to_proto` using the existing `poll_response_to_proto` path
    - _Requirements: 1.1, 1.2, 4.1, 4.2_

  - [x] 3.2 Implement the original eager WFT broker-claim path (superseded by task 8)
    - In the `Started` branch of `start_workflow_execution` in `workflow_service.rs`, after the existing response construction:
    - Check `request_eager_execution` on the request
    - If true, call `self.poller_registry.has_active_poller(&queue_key, &identity)`
    - If compatible, call `self.broker.try_claim_workflow_task(&queue_key, run_key)` — targeted by the just-started workflow's run_key
    - If claimed, build a `PollWorkflowTaskQueueResponse` using `from_internal::poll_response` and attach to the response's `eager_workflow_task` field
    - Historical implementation only; tasks 7-8 replace this workflow-start path

  - [x]* 3.3 Write property test for `request_eager_execution` flag preservation
    - **Property 1: request_eager_execution flag preservation**
    - Generate random `StartWorkflowExecutionRequest` protos with varying `request_eager_execution` values, translate to internal DTO and back, verify flag is preserved
    - **Validates: Requirements 1.1, 1.2**

  - [x]* 3.4 Write property test for start response proto translation
    - **Property 11: Start response proto translation preserves eager_workflow_task**
    - Generate internal `StartWorkflowExecutionResponse` with and without `eager_workflow_task`, translate to proto, verify the field is set iff the internal response has `Some`
    - **Validates: Requirements 4.1, 4.2**

- [x] 4. Phase 2 — Eager activity tasks on RespondWorkflowTaskCompleted
  - [x] 4.1 Add eager activity DTO fields and config
    - Add `activity_tasks: Vec<PollActivityTaskQueueResponse>` field to `RespondWorkflowTaskCompletedResponse` in `translate/mod.rs`
    - Thread `request_eager_execution: bool` through the `ScheduleActivityTask` command representation in the kernel
    - Add `EagerDispatchConfig` with `max_eager_activity_tasks_per_response: usize` (default 3) to the edge layer config
    - Serialize `activity_tasks` in `completed_response_to_proto` using the existing `poll_activity_response_to_proto` path in `grpc/translate.rs`
    - _Requirements: 5.1, 5.2, 5.3, 7.3, 8.1, 8.2_

  - [x] 4.2 Implement eager activity dispatch in `respond_workflow_task_completed`
    - After the runtime commit in `respond_workflow_task_completed` in `workflow_service.rs`:
    - Collect eager-eligible activity commands (those with `request_eager_execution=true`)
    - For each eligible command (up to `max_eager_activity_tasks_per_response`), call `self.activity_broker.try_claim_activity_task(&queue_key, run_key, &activity_id)` — targeted by the specific activity's identity
    - For each claimed task, build a `PollActivityTaskQueueResponse` using `from_internal::poll_activity_response`
    - Attach the list to the response's `activity_tasks` field
    - _Requirements: 5.1, 5.2, 6.1, 6.2, 6.3, 6.4, 6.5, 7.1, 7.2, 12.2, 12.3_

  - [x]* 4.3 Write property test for eager activity flag threading
    - **Property 10: Eager activity flag threading through commands**
    - Generate `ScheduleActivityTask` commands with varying `request_eager_execution` values, translate through the internal command representation, verify flag preservation
    - **Validates: Requirements 5.1, 5.2, 5.3**

  - [x]* 4.4 Write property test for eager activity task limit enforcement
    - **Property 9: Eager activity task limit enforcement**
    - Generate random counts of eager-eligible activity commands (0..20), configure various max limits, verify the response never exceeds the configured maximum
    - **Validates: Requirements 7.1, 7.2**

  - [x]* 4.5 Write property test for complete response proto translation
    - **Property 11: Complete response proto translation preserves activity_tasks**
    - Generate internal `RespondWorkflowTaskCompletedResponse` with varying `activity_tasks` lists, translate to proto, verify the repeated field has the same count
    - **Validates: Requirements 8.1, 8.2**

- [x] 5. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Confirm the Tier 3.18 bug condition and serialization baseline
  - [x] 6.1 Add the eager-start exploration test before changing implementation
    - Start with `request_eager_execution=true` and no active `PollerRegistry` entry; assert an inline first WFT and `eager_execution_accepted=true`
    - Confirm the test fails on the old implementation at the absent eager task
    - **Properties 2, 4, 5: runtime admission, atomic eager history, response/history agreement**
    - _Requirements: 2.1, 2.5, 3.1, 3.2, 3.7, 4.1_
  - [x] 6.2 Capture a genuine pre-Tier-3.18 started-event byte fixture
    - Use the existing DSQL history codec before changing the event enum
    - Record only the generated byte literal; remove the temporary generator
    - _Requirements: 4.4_

- [x] 7. Persist eager acceptance in the pure kernel
  - [x] 7.1 Add kernel input and the append-only V2 history variant
    - Add `eager_execution_accepted` to `StartRequest`
    - Preserve `WorkflowExecutionStarted` byte-for-byte as the legacy decode shape
    - Append `WorkflowExecutionStartedV2` and make all new start emitters use it
    - Set every internal/derived start constructor to false
    - _Requirements: 2.7, 2.8, 3.6, 3.7, 3.8, 4.3, 4.4_
  - [x] 7.2 Implement the one-directional kernel clamp
    - Record true only when the runtime supplied true, the effective start delay is not positive, and `reserved_poller_identity` is present
    - Never promote false to true
    - **Property 3: Kernel acceptance clamp**
    - _Requirements: 2.6, 2.7, 3.1, 3.7, 3.8_
  - [x] 7.3 Prove atomic eager history and postcard compatibility
    - Property-test the `(candidate, delay, inline identity)` clamp truth table with at least 100 cases
    - Golden-test accepted history as WES-V2 / WFT Scheduled / WFT Started in one transition
    - Decode the task-6.2 V1 fixture as accepted=false and round-trip V2 true
    - **Properties 3, 4, 7: clamp, atomic history, legacy decoding**
    - _Requirements: 2.6, 2.7, 3.1, 4.3, 4.4_

- [x] 8. Correct runtime and edge eager workflow start
  - [x] 8.1 Pre-gate eager acceptance and use atomic inline start
    - Pin the v1.31.0 eager-enable default to true without adding operator config
    - Apply client cron/start-delay normalization before the final acceptance clamp
    - Use the request caller identity as `reserved_poller_identity` without consulting `PollerRegistry`
    - Do not reserve a parked broker poller or publish/reclaim the accepted first WFT
    - Keep low-level/internal start callers unable to request an inline response they cannot return
    - **Property 2: Runtime eager admission**
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 3.2, 3.3_
  - [x] 8.2 Return fresh and deduped eager tasks from authoritative state
    - Extend fresh and deduped `StartWorkflowResult` variants with an optional started task
    - Reconstruct on dedup only for the still-started first WFT (`started_event_id=3`, `attempt=1`) with a live deadline
    - Prove reconstruction omits an elapsed task before the coarse scanner fires and does not mutate history
    - **Properties 5, 6: response/history agreement and request-ID reconstruction**
    - _Requirements: 3.2, 3.7, 3.8, 13.1, 13.2, 13.3_
  - [x] 8.3 Remove the edge poller gate and broker reclaim
    - Map the runtime-returned task through the normal poll response builder
    - Serialize V1 started events with accepted=false and V2 with the persisted value
    - Update every V1/V2 metadata, link, event-type, attribute, and poll workflow-type extraction match
    - Remove `has_active_poller` if no other caller remains
    - **Properties 1, 5, 11: request preservation, agreement, translation fidelity**
    - _Requirements: 1.1, 1.2, 2.5, 4.1, 4.2, 4.3, 4.4_
  - [x] 8.4 Advertise the implemented eager capability
    - Set GetSystemInfo and namespace eager-workflow-start capabilities to true
    - Add focused protocol tests for both surfaces
    - **Property 14: Capability agreement**
    - _Requirements: 14.1, 14.2_
  - [x] 8.5 Preserve activity and non-start direct-claim behavior
    - Re-run Properties 8-13 and the existing eager-activity integration tests
    - _Requirements: 5.1-10.5, 12.2-12.4_

- [x] 9. Classify the dynamic-config-only leaf and update conformance status
  - [x] 9.1 Add a cited skip for `TestEagerWorkflowStart_TerminateDuplicate`
    - The leaf requires `OverrideDynamicConfig(WorkflowIdReuseMinimalInterval=0)`; the pinned default is 1 second and cannot be injected over the wire
    - Edit only the fork skip registry, never the corpus test body
  - [x] 9.2 Promote compatibility evidence only after the suite is clean
    - Mark eager workflow start Implemented, remove the experimental-only config key, and cite Tier 3.18 evidence
    - Add the clean Tier 3.18 result to `docs/readiness/conformance.md`
    - _Requirements: 14.3_

- [x] 10. Final Tier 3.18 verification
  - [x] 10.1 Run focused formatting, kernel/runtime/edge/storage tests, lint, and workspace tests
  - [x] 10.2 Run `TestEagerWorkflowTestSuite` to 5 pass / 0 fail / 1 classified skip three consecutive times
  - [x] 10.3 Re-run the earlier workflow-start, WFT-timeout, retry, conflict-policy, history, and activity cohorts

## Task Dependency Graph

```text
6.1 ─┬─> 7.1 ─> 7.2 ─> 7.3 ─┐
6.2 ─┘                        ├─> 8.2 ─> 8.3 ─> 8.4 ─> 9.2 ─> 10
             7.2 ─> 8.1 ─────┘              └─> 8.5 ────────┘
                                      9.1 ───────────────────┘
```

## Notes

- Stars on completed legacy tasks are historical only. Every correction property in tasks 6-10 is required.
- Pure/generated-input properties use `proptest` with at least 100 cases and the tag `Feature: edge-eager-dispatch, Property {N}: {title}`; stateful async response/retry properties use fixed unit and integration tests against committed state.
- Existing timeout scanners remain the recovery mechanism; accepted eager WFTs are already authoritative started tasks.
- The pre-edit spec snapshot is `/tmp/tokeira-spec-snapshots/20260709-tier318-codex/`.
