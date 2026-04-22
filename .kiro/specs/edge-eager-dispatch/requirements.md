# Requirements Document: Edge Eager Dispatch

## Introduction

This spec implements Eager Dispatch — an optimization where the server returns a workflow task or activity task inline with the gRPC response that triggered it, avoiding a separate poll round-trip. This is Feature 9 from the umbrella spec `edge-complete-implementation`.

Eager dispatch has two independent paths:

1. **Eager workflow task on `StartWorkflowExecution`**: When the caller sets `request_eager_execution=true` and is a compatible poller on the workflow's task queue, the first workflow task is claimed from the `InMemoryBroker` and returned inline in `StartWorkflowExecutionResponse.eager_workflow_task`.
2. **Eager activity tasks on `RespondWorkflowTaskCompleted`**: When the completing worker sets `return_new_workflow_task=true` and the workflow task completion schedules activities, eligible activity tasks are claimed from the `InMemoryActivityBroker` and returned inline in `RespondWorkflowTaskCompletedResponse.activity_tasks`. The activity commands must carry `request_eager_execution=true` to be eligible.

Both paths require the broker to support an atomic "claim" operation that removes a task from the ready queue before any normal poller can take it. If the eager response fails to reach the client (connection drop), the claimed task is recovered by the existing WFT/activity timeout mechanism — the task times out and gets rescheduled by the scanner.

The edge layer already has a partial eager pattern: `respond_workflow_task_completed` builds an inline query-only WFT via `build_eager_query_workflow_task` when `return_new_workflow_task=true` and buffered queries exist. This spec extends that pattern to cover the two proto-defined eager dispatch paths.

Dependencies: Features 1 (poll response fidelity) and 2 (failure object completeness) for complete poll responses returned in eager tasks.

The implementation is organized into three phases:
- Phase 1: Eager workflow task on `StartWorkflowExecution`
- Phase 2: Eager activity tasks on `RespondWorkflowTaskCompleted`
- Phase 3: Broker coordination (atomic claim, re-enqueue safety)

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **InMemoryBroker**: The in-memory workflow task broker in `tokeira-runtime/src/broker.rs` that queues `DispatchableWorkflowTask` entries by `QueueKey` (namespace, task_queue), with sticky/general tiers and `Notify`-based long-poll wake.
- **InMemoryActivityBroker**: The in-memory activity task broker in `tokeira-runtime/src/broker.rs` that queues `DispatchableActivityTask` entries by `QueueKey`.
- **WorkerRegistry**: The `WorkerRegistry` in `tokeira-runtime/src/worker_registry.rs` that tracks active worker registrations by (worker_identity, namespace_id, task_queue), including version metadata and last-seen timestamps.
- **Eager_Workflow_Task**: A `PollWorkflowTaskQueueResponse` returned inline in `StartWorkflowExecutionResponse.eager_workflow_task`, containing the first workflow task for a just-started workflow.
- **Eager_Activity_Task**: A `PollActivityTaskQueueResponse` returned inline in `RespondWorkflowTaskCompletedResponse.activity_tasks`, containing an activity task scheduled by the just-completed workflow task.
- **Atomic_Claim**: A broker operation that removes a task from the ready queue in a single lock acquisition, preventing the task from being delivered to a normal poller concurrently.
- **Compatible_Poller**: A worker that is registered in the WorkerRegistry as actively polling on the same (namespace, task_queue) as the workflow being started, making it eligible to receive an eager workflow task.
- **Poll_Response**: The proto `PollWorkflowTaskQueueResponse` or `PollActivityTaskQueueResponse` returned to SDK workers.
- **QueueKey**: The (namespace_id, task_queue, versioning) tuple used to key broker queues.

## Requirements

---

## Phase 1: Eager Workflow Task on StartWorkflowExecution

### Requirement 1: Parse and Thread request_eager_execution

**User Story:** As a Tokeira developer, I want the edge layer to parse the `request_eager_execution` field from `StartWorkflowExecutionRequest`, so that the start handler can decide whether to attempt eager workflow task delivery.

#### Acceptance Criteria

1. WHEN a `StartWorkflowExecutionRequest` proto is received with `request_eager_execution` set to `true`, THE Edge_Layer SHALL preserve the flag in the internal `StartWorkflowExecutionRequest` struct.
2. WHEN a `StartWorkflowExecutionRequest` proto is received with `request_eager_execution` set to `false` or unset, THE Edge_Layer SHALL set the internal flag to `false`.

### Requirement 2: Compatible Poller Check

**User Story:** As a Tokeira developer, I want the edge layer to determine whether the calling worker is a compatible poller on the workflow's task queue, so that eager workflow tasks are only returned to workers that can execute them.

#### Acceptance Criteria

1. WHEN `request_eager_execution` is `true` on a `StartWorkflowExecution` request, THE Edge_Layer SHALL check the WorkerRegistry for a registration matching the request's (identity, namespace, task_queue) combination.
2. WHEN the WorkerRegistry contains a matching registration for the caller, THE Edge_Layer SHALL consider the caller a Compatible_Poller and proceed with eager dispatch.
3. WHEN the WorkerRegistry does not contain a matching registration for the caller, THE Edge_Layer SHALL skip eager dispatch and return the response without an `eager_workflow_task` field.

### Requirement 3: Eager Workflow Task Claim and Return

**User Story:** As an SDK user, I want `StartWorkflowExecutionResponse` to include the first workflow task inline when I set `request_eager_execution=true`, so that my worker can begin executing the workflow immediately without a separate poll round-trip.

#### Acceptance Criteria

1. WHEN a workflow is successfully started with `request_eager_execution=true` and the caller is a Compatible_Poller, THE Edge_Layer SHALL attempt to claim the first workflow task from the InMemoryBroker using an Atomic_Claim operation on the workflow's task queue.
2. WHEN the Atomic_Claim succeeds, THE Edge_Layer SHALL build a complete `PollWorkflowTaskQueueResponse` from the claimed task and return it in the `eager_workflow_task` field of `StartWorkflowExecutionResponse`.
3. WHEN the Atomic_Claim fails (task not yet published, already taken by a poller, or queue empty), THE Edge_Layer SHALL return the `StartWorkflowExecutionResponse` without an `eager_workflow_task` field (the task will be delivered via normal polling).
4. THE Eager_Workflow_Task SHALL use the same task token encoding as a normally polled workflow task.
5. THE Eager_Workflow_Task SHALL include all fields required by a complete `PollWorkflowTaskQueueResponse`: `task_token`, `started_event_id`, `previous_started_event_id`, `attempt`, `scheduled_time`, `started_time`, history payload, and workflow metadata.

### Requirement 4: StartWorkflowExecutionResponse Proto Translation

**User Story:** As a Tokeira developer, I want the proto translation layer to serialize the optional eager workflow task into the `StartWorkflowExecutionResponse` proto, so that SDK clients receive the eager task when available.

#### Acceptance Criteria

1. WHEN the internal `StartWorkflowExecutionResponse` contains an `eager_workflow_task`, THE proto translation function SHALL populate the `eager_workflow_task` field on the proto response using the same serialization path as `poll_workflow_task_queue_response_to_proto`.
2. WHEN the internal `StartWorkflowExecutionResponse` does not contain an `eager_workflow_task`, THE proto translation function SHALL leave the `eager_workflow_task` field unset on the proto response.

---

## Phase 2: Eager Activity Tasks on RespondWorkflowTaskCompleted

### Requirement 5: Identify Eager-Eligible Activity Commands

**User Story:** As a Tokeira developer, I want the edge layer to identify which `ScheduleActivityTask` commands in a workflow task completion are eligible for eager dispatch, so that only explicitly opted-in activities are returned inline.

#### Acceptance Criteria

1. WHEN a `RespondWorkflowTaskCompletedRequest` contains `ScheduleActivityTask` commands with `request_eager_execution=true`, THE Edge_Layer SHALL mark those commands as eager-eligible.
2. WHEN a `ScheduleActivityTask` command has `request_eager_execution=false` or unset, THE Edge_Layer SHALL NOT mark it as eager-eligible.
3. THE Edge_Layer SHALL thread the `request_eager_execution` flag through the internal command representation so that the eager dispatch logic can identify eligible activities after the workflow task is committed.

### Requirement 6: Eager Activity Task Claim and Return

**User Story:** As an SDK user, I want `RespondWorkflowTaskCompletedResponse` to include activity tasks inline when my workflow task schedules eager-eligible activities, so that my worker can begin executing activities immediately without separate poll round-trips.

#### Acceptance Criteria

1. WHEN a workflow task completion commits successfully and the response contains eager-eligible activity commands, THE Edge_Layer SHALL attempt to claim the corresponding activity tasks from the InMemoryActivityBroker using Atomic_Claim operations.
2. WHEN an Atomic_Claim succeeds for an activity task, THE Edge_Layer SHALL build a complete `PollActivityTaskQueueResponse` from the claimed task and include it in the `activity_tasks` list of `RespondWorkflowTaskCompletedResponse`.
3. WHEN an Atomic_Claim fails for an activity task (task not yet published, already taken, or queue empty), THE Edge_Layer SHALL omit that activity from the `activity_tasks` list (the task will be delivered via normal polling).
4. EACH Eager_Activity_Task SHALL use the same task token encoding as a normally polled activity task.
5. EACH Eager_Activity_Task SHALL include all fields required by a complete `PollActivityTaskQueueResponse`: `task_token`, `activity_id`, `activity_type`, `input`, `workflow_execution`, `scheduled_time`, `started_time`, `current_attempt_scheduled_time`, `heartbeat_timeout`, `schedule_to_close_timeout`, `start_to_close_timeout`, and workflow metadata.

### Requirement 7: Eager Activity Task Limit

**User Story:** As a Tokeira operator, I want a configurable maximum number of eager activity tasks per response, so that a single workflow task completion does not monopolize the worker's activity slots.

#### Acceptance Criteria

1. THE Edge_Layer SHALL enforce a maximum number of eager activity tasks returned per `RespondWorkflowTaskCompletedResponse`.
2. WHEN the number of eager-eligible activity commands exceeds the maximum, THE Edge_Layer SHALL claim at most the configured maximum number of activity tasks and leave the remainder for normal polling.
3. THE maximum SHALL default to a reasonable value (e.g., 3) and be configurable via the edge layer's configuration.

### Requirement 8: RespondWorkflowTaskCompletedResponse Proto Translation

**User Story:** As a Tokeira developer, I want the proto translation layer to serialize eager activity tasks into the `RespondWorkflowTaskCompletedResponse` proto, so that SDK clients receive the eager activity tasks when available.

#### Acceptance Criteria

1. WHEN the internal `RespondWorkflowTaskCompletedResponse` contains one or more eager activity tasks, THE proto translation function SHALL populate the `activity_tasks` repeated field on the proto response using the same serialization path as `poll_activity_task_queue_response_to_proto`.
2. WHEN the internal `RespondWorkflowTaskCompletedResponse` contains no eager activity tasks, THE proto translation function SHALL leave the `activity_tasks` field as an empty list on the proto response.

---

## Phase 3: Broker Coordination

### Requirement 9: InMemoryBroker Atomic Claim for Workflow Tasks

**User Story:** As a Tokeira developer, I want the InMemoryBroker to support an atomic claim operation that removes a specific workflow task from the ready queue without blocking, so that the eager dispatch path can claim a just-published task before any poller takes it.

#### Acceptance Criteria

1. THE InMemoryBroker SHALL expose a `try_claim_workflow_task` method that accepts a `QueueKey` and attempts to remove the next available workflow task from the general ready queue.
2. WHEN a task is available in the general ready queue for the specified `QueueKey`, THE `try_claim_workflow_task` method SHALL remove and return the task atomically (within a single lock acquisition).
3. WHEN no task is available in the general ready queue, THE `try_claim_workflow_task` method SHALL return `None` without blocking.
4. THE `try_claim_workflow_task` method SHALL remove the task from the deduplication set (`enqueued`) so that the task is not delivered again via normal polling.
5. THE `try_claim_workflow_task` method SHALL NOT wake any waiting pollers (the task is being claimed, not published).

### Requirement 10: InMemoryActivityBroker Atomic Claim for Activity Tasks

**User Story:** As a Tokeira developer, I want the InMemoryActivityBroker to support an atomic claim operation that removes activity tasks from the ready queue without blocking, so that the eager dispatch path can claim just-published activity tasks before any poller takes them.

#### Acceptance Criteria

1. THE InMemoryActivityBroker SHALL expose a `try_claim_activity_task` method that accepts a `QueueKey` and attempts to remove the next available activity task from the ready queue.
2. WHEN a task is available in the ready queue for the specified `QueueKey`, THE `try_claim_activity_task` method SHALL remove and return the task atomically (within a single lock acquisition).
3. WHEN no task is available in the ready queue, THE `try_claim_activity_task` method SHALL return `None` without blocking.
4. THE `try_claim_activity_task` method SHALL remove the task from the deduplication set (`enqueued`) so that the task is not delivered again via normal polling.
5. THE `try_claim_activity_task` method SHALL NOT wake any waiting pollers.

### Requirement 11: Re-Enqueue Safety via Existing Timeout Mechanisms

**User Story:** As a Tokeira developer, I want eagerly dispatched tasks to be recovered if the client never completes them (e.g., connection drop after response), so that no work is permanently lost.

#### Acceptance Criteria

1. WHEN an Eager_Workflow_Task is claimed and returned to the client, THE Runtime SHALL rely on the existing WFT timeout mechanism to detect non-completion: if the worker does not call `RespondWorkflowTaskCompleted` within the `workflow_task_timeout`, the WFT timeout scanner SHALL reschedule the workflow task.
2. WHEN an Eager_Activity_Task is claimed and returned to the client, THE Runtime SHALL rely on the existing activity timeout mechanisms (schedule-to-start, start-to-close, schedule-to-close) to detect non-completion and reschedule the activity task.
3. THE eager dispatch path SHALL NOT introduce any new timeout or re-enqueue mechanism — the existing scanner-based recovery is sufficient because eagerly claimed tasks are indistinguishable from normally polled tasks once claimed.

### Requirement 12: Claim Timing and Publish Ordering

**User Story:** As a Tokeira developer, I want the eager claim to happen after the runtime has published the task to the broker, so that the claim always targets a task that exists in the ready queue.

#### Acceptance Criteria

1. WHEN the Edge_Layer attempts an eager workflow task claim after `start_workflow`, THE claim SHALL occur after the runtime's `start_workflow_with_policy` has committed the transition and the dispatch publisher has published the workflow task to the InMemoryBroker.
2. WHEN the Edge_Layer attempts eager activity task claims after `complete_workflow_task`, THE claims SHALL occur after the runtime's `complete_workflow_task` has committed the transition and the dispatch publisher has published the activity tasks to the InMemoryActivityBroker.
3. IF the dispatch publisher has not yet published the task at claim time (race condition), THE Atomic_Claim SHALL return `None` and the task will be delivered via normal polling.
