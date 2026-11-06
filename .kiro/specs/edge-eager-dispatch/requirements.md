# Requirements Document: Edge Eager Dispatch

## Introduction

This spec implements Eager Dispatch — an optimization where the server returns a workflow task or activity task inline with the gRPC response that triggered it, avoiding a separate poll round-trip. This is Feature 9 from the umbrella spec `edge-complete-implementation`.

Eager dispatch has two independent paths:

1. **Eager workflow task on `StartWorkflowExecution`**: When the caller sets `request_eager_execution=true`, eager workflow start is enabled, and the first workflow task has no backoff, the runtime commits the first workflow task as started in the run-creation transition and returns it inline in `StartWorkflowExecutionResponse.eager_workflow_task`. Temporal v1.31.0 does not require a server-observed active poller; the caller is the intended worker.
2. **Eager activity tasks on `RespondWorkflowTaskCompleted`**: When eager activity execution is enabled, eligible requested activities are started in the completion transition and returned in `RespondWorkflowTaskCompletedResponse.activity_tasks`. This does not depend on `return_new_workflow_task` or a parked activity poll.

The workflow-start path does not put correctness weight on the broker: the kernel's existing reserved-start path authors `WorkflowExecutionStarted`, `WorkflowTaskScheduled`, and `WorkflowTaskStarted` in one transition. The activity path likewise bypasses dispatch rows and offers when it starts work inline. If an eager response fails to reach the client, the existing workflow-task or activity timeout mechanism recovers the already-authoritative pending work.

The edge layer already has a partial eager pattern: `respond_workflow_task_completed` builds an inline query-only WFT via `build_eager_query_workflow_task` when `return_new_workflow_task=true` and buffered queries exist. This spec extends that pattern to cover the two proto-defined eager dispatch paths.

Dependencies: Features 1 (poll response fidelity) and 2 (failure object completeness) for complete poll responses returned in eager tasks.

The implementation is organized into three phases:
- Phase 1: Eager workflow task on `StartWorkflowExecution`
- Phase 2: Eager activity tasks on `RespondWorkflowTaskCompleted`
- Phase 3: Activity/direct broker coordination and timeout recovery

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **InMemoryBroker**: The in-memory workflow task broker in `tokeira-runtime/src/broker.rs` that queues `DispatchableWorkflowTask` entries by `QueueKey` (namespace, task_queue), with sticky/general tiers and `Notify`-based long-poll wake.
- **InMemoryActivityBroker**: The in-memory activity task broker in `tokeira-runtime/src/broker.rs` that queues `DispatchableActivityTask` entries by `QueueKey`.
- **Eager_Workflow_Task**: A `PollWorkflowTaskQueueResponse` returned inline in `StartWorkflowExecutionResponse.eager_workflow_task`, containing the first workflow task for a just-started workflow.
- **Eager_Activity_Task**: A `PollActivityTaskQueueResponse` returned inline in `RespondWorkflowTaskCompletedResponse.activity_tasks`, containing an activity task scheduled by the just-completed workflow task.
- **Eager_Acceptance**: The runtime-owned, pre-gated decision that eager workflow start is enabled and the first WFT can be started inline. The kernel records this decision and may only clamp it to `false` when the transition does not actually start the WFT inline.
- **Atomic_Claim**: A broker operation that removes a task from the ready queue in a single lock acquisition, preventing the task from being delivered to a normal poller concurrently.
- **Poll_Response**: The proto `PollWorkflowTaskQueueResponse` or `PollActivityTaskQueueResponse` returned to SDK workers.
- **QueueKey**: The (namespace_id, task_queue, versioning) tuple used to key broker queues.

## Target State and Ground Truth

- Wire request/response shape comes from `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`; durable acceptance is field 38, `eager_execution_accepted`, in `proto/upstream/temporal/api/history/v1/message.proto`.
- `service/history/api/startworkflow/api.go @ v1.31.0` pre-gates the request flag: disabled eager start and first-WFT backoff force it to `false`; an immediate request-id retry returns the still-started first WFT, while a retry after fallback does not.
- `service/history/historybuilder/event_factory.go @ v1.31.0` copies the already-gated flag into `WorkflowExecutionStarted.eager_execution_accepted`; the event builder does not own the config decision.
- `service/history/api/create_workflow_util.go @ v1.31.0` schedules and starts the accepted first WFT while creating the run. Tokeira matches that observable result through its existing `reserved_poller_identity` start branch, without requiring a live `PollerRegistry` entry.
- Current Tokeira code incorrectly gates eager start on `PollerRegistry::has_active_poller`, claims after publication, omits the durable history flag, and always omits the eager task on request-id dedup. Tier 3.18's six leaves therefore fail at the missing eager task.

The pinned v1.31.0 eager **workflow-start** default is a constant `true`, not a new operator knob. Feature-mode testing may inject the disabled value into the pure admission decision, but the default server path remains enabled.

## Requirements

---

## Phase 1: Eager Workflow Task on StartWorkflowExecution

### Requirement 1: Parse and Thread request_eager_execution

**User Story:** As a Tokeira developer, I want the edge layer to parse the `request_eager_execution` field from `StartWorkflowExecutionRequest`, so that the start handler can decide whether to attempt eager workflow task delivery.

#### Acceptance Criteria

1. WHEN a `StartWorkflowExecutionRequest` proto is received with `request_eager_execution` set to `true`, THE Edge_Layer SHALL preserve the flag in the internal `StartWorkflowExecutionRequest` struct.
2. WHEN a `StartWorkflowExecutionRequest` proto is received with `request_eager_execution` set to `false` or unset, THE Edge_Layer SHALL set the internal flag to `false`.

### Requirement 2: Eager Acceptance Decision

**User Story:** As a Tokeira developer, I want the runtime to decide eager acceptance before the start transition, so that history and the inline response always describe the same outcome.

#### Acceptance Criteria

1. WHEN `request_eager_execution` is `true`, eager workflow start is enabled, and the effective first-WFT backoff is zero, THE Runtime SHALL set the kernel-facing `eager_execution_accepted` value to `true`.
2. WHEN eager workflow start is accepted, THE Runtime SHALL identify the request's caller as the inline worker for the atomic start transition.
3. WHEN `request_eager_execution` is `false` or eager workflow start is disabled, THE Runtime SHALL set `eager_execution_accepted` to `false`.
4. WHEN the effective first-WFT backoff is positive, THE Runtime SHALL set `eager_execution_accepted` to `false`.
5. THE Runtime SHALL NOT require an active `PollerRegistry` entry to accept eager workflow start.
6. IF `eager_execution_accepted` reaches the Kernel without both a zero first-WFT backoff and an inline worker identity, THEN THE Kernel SHALL clamp the recorded value to `false`.
7. THE Kernel SHALL NOT promote a runtime-supplied `false` acceptance decision to `true`.
8. WHEN an internal start path does not originate from an accepted eager `StartWorkflowExecution` request, THE Runtime SHALL set `eager_execution_accepted` to `false`.

### Requirement 3: Atomic Eager Workflow Start and Return

**User Story:** As an SDK user, I want `StartWorkflowExecutionResponse` to include the first workflow task inline when I set `request_eager_execution=true`, so that my worker can begin executing the workflow immediately without a separate poll round-trip.

#### Acceptance Criteria

1. WHEN a new workflow is started with `eager_execution_accepted=true`, THE Kernel SHALL author `WorkflowExecutionStarted`, `WorkflowTaskScheduled`, and `WorkflowTaskStarted` in the same transition.
2. WHEN the accepted start transition commits, THE Runtime SHALL return the committed started WFT directly to the Edge.
3. WHEN eager workflow start is accepted, THE Runtime SHALL NOT publish and re-claim the first WFT through the InMemoryBroker.
4. WHEN eager execution is not accepted, THE Runtime SHALL preserve normal workflow-task dispatch behavior and return no eager WFT.
5. THE Eager_Workflow_Task SHALL use the same task token encoding as a normally polled workflow task.
6. THE Eager_Workflow_Task SHALL include all fields required by a complete `PollWorkflowTaskQueueResponse`: `task_token`, `started_event_id`, `previous_started_event_id`, `attempt`, `scheduled_time`, `started_time`, history payload, and workflow metadata.
7. WHEN a successful fresh-start response carries an eager WFT, THE authoritative `WorkflowExecutionStarted` event SHALL record `eager_execution_accepted=true`.
8. WHEN a successful fresh-start response carries no eager WFT, THE authoritative `WorkflowExecutionStarted` event SHALL record `eager_execution_accepted=false`.
9. WHEN a non-eager start synchronously matches a parked normal poller, THE authoritative `WorkflowExecutionStarted` event SHALL record `eager_execution_accepted=false`.

### Requirement 4: StartWorkflowExecutionResponse Proto Translation

**User Story:** As a Tokeira developer, I want the proto translation layer to serialize the optional eager workflow task into the `StartWorkflowExecutionResponse` proto, so that SDK clients receive the eager task when available.

#### Acceptance Criteria

1. WHEN the internal `StartWorkflowExecutionResponse` contains an `eager_workflow_task`, THE proto translation function SHALL populate the `eager_workflow_task` field on the proto response using the same serialization path as `poll_workflow_task_queue_response_to_proto`.
2. WHEN the internal `StartWorkflowExecutionResponse` does not contain an `eager_workflow_task`, THE proto translation function SHALL leave the `eager_workflow_task` field unset on the proto response.
3. WHEN history contains `WorkflowExecutionStarted.eager_execution_accepted=true`, THE history serializer SHALL populate proto field 38 with `true` in both the inline response and later history reads.
4. WHEN history contains the legacy `WorkflowExecutionStarted` event shape, THE history serializer SHALL emit `eager_execution_accepted=false`.

---

## Phase 2: Eager Activity Tasks on RespondWorkflowTaskCompleted

### Requirement 5: Identify Eager-Eligible Activity Commands

**User Story:** As a Tokeira developer, I want the edge layer to identify which `ScheduleActivityTask` commands in a workflow task completion are eligible for eager dispatch, so that only explicitly opted-in activities are returned inline.

#### Acceptance Criteria

1. WHEN a `RespondWorkflowTaskCompletedRequest` contains `ScheduleActivityTask` commands with `request_eager_execution=true`, THE Edge_Layer SHALL preserve them as eager candidates, subject to the SDK exclusion and runtime/kernel eligibility checks.
2. WHEN a `ScheduleActivityTask` command has `request_eager_execution=false` or unset, THE Edge_Layer SHALL NOT mark it as eager-eligible.
3. THE Edge_Layer SHALL thread `request_eager_execution` and `use_workflow_build_id` into completion inputs before the transition.
4. WHEN the SDK headers identify Python `0.1a1`, `0.1a2`, `0.1b1`, or `0.1b2`, or parseable TypeScript below `1.4.4`, THE Edge_Layer SHALL clear activity eager requests; invalid TypeScript semver SHALL remain unexcluded by this check (`service/frontend/overrides.go:23-56 @ v1.31.0`).

### Requirement 6: Eager Activity Start and Return in Completion

**User Story:** As an SDK worker, I want my completion to start its inline activities, so that another poll cannot take them first.

#### Acceptance Criteria

1. WHEN the setting is enabled and a command requests eagerness, THE completion SHALL apply the versioning, pause and surviving-command eligibility in [activity-dispatch Requirement 9](../activity-dispatch/requirements.md#requirement-9-start-eager-activities-inside-completion).
2. WHEN all commands succeed, THE kernel SHALL start eligible surviving activities in the completion's transition with fresh runtime-supplied request IDs, the completing identity and the workflow's assigned build ID.
3. WHEN the run closes or an activity is cancelled in the same completion, THE completion SHALL NOT start or return that activity.
4. EACH Eager_Activity_Task SHALL use the normal activity task-token encoding and exact scheduled input/header, schedule/start times, attempt, timeouts, workflow metadata and priority from committed state.
5. THE eager first-start path SHALL bypass workflow-rule evaluation; ordinary starts and retries SHALL retain it.
6. WHEN the completion commits, THE runtime SHALL return its started activity results directly to the edge without a second start commit.

Authority: `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:487-622 @ v1.31.0`.

### Requirement 7: Default-Off Policy Without an Activity Count Cap

**User Story:** As an operator, I want explicit eager activity enablement matching the stock default, so that production policy is intentional.

#### Acceptance Criteria

1. THE typed deployment setting SHALL default to false and apply to every namespace; the existing functional harness override SHALL take precedence per test.
2. WHEN the setting is false, THE response SHALL contain no activity tasks and otherwise dispatchable activities SHALL remain available to polls.
3. THE server SHALL impose no maximum number of eager activities per response; existing payload/command admission SHALL remain in effect.
4. THE activity path SHALL NOT require `return_new_workflow_task` or an active poller.

Authority: `common/dynamicconfig/constants.go:193-197` and the unbounded response mutations in `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:524-622 @ v1.31.0`.

### Requirement 8: RespondWorkflowTaskCompletedResponse Proto Translation

**User Story:** As a Tokeira developer, I want the proto translation layer to serialize eager activity tasks into the `RespondWorkflowTaskCompletedResponse` proto, so that SDK clients receive the eager activity tasks when available.

#### Acceptance Criteria

1. WHEN the internal `RespondWorkflowTaskCompletedResponse` contains one or more eager activity tasks, THE proto translation function SHALL populate the `activity_tasks` repeated field on the proto response using the same serialization path as `poll_activity_task_queue_response_to_proto`.
2. WHEN the internal `RespondWorkflowTaskCompletedResponse` contains no eager activity tasks, THE proto translation function SHALL leave the `activity_tasks` field as an empty list on the proto response.

---

## Phase 3: Broker Coordination

### Requirement 9: InMemoryBroker Targeted Claim for Direct Workflow Tasks

**User Story:** As a Tokeira developer, I want the InMemoryBroker to retain a targeted claim operation for non-start direct-delivery paths, so that those paths never claim an unrelated workflow's task.

#### Acceptance Criteria

1. THE InMemoryBroker SHALL expose a `try_claim_workflow_task` method that accepts a `QueueKey` and a `RunKey`, and attempts to remove the workflow task matching that `RunKey` from the general ready queue.
2. WHEN a task matching the specified `RunKey` is available in the general ready queue for the specified `QueueKey`, THE `try_claim_workflow_task` method SHALL remove and return the task atomically (within a single lock acquisition).
3. WHEN no task matching the specified `RunKey` is available in the general ready queue, THE `try_claim_workflow_task` method SHALL return `None` without blocking.
4. THE `try_claim_workflow_task` method SHALL remove the task from the deduplication set (`enqueued`) so that the task is not delivered again via normal polling.
5. THE `try_claim_workflow_task` method SHALL NOT wake any waiting pollers (the task is being claimed, not published).

### Requirement 10: No Dispatch for Inline Activity Starts

**User Story:** As a worker, I want eagerly started work absent from poll delivery, so that the response owns its start.

#### Acceptance Criteria

1. WHEN a completion starts an activity inline, THE kernel SHALL emit no activity enqueue operation for that incarnation.
2. WHEN that transition commits, THE repository SHALL write no activity dispatch row for the inline incarnation.
3. THE publisher, discovery and acquisition repair SHALL publish no offer for an inline-started activity.
4. THE completion path SHALL NOT call `try_claim_activity_task` or depend on activity-broker visibility.
5. WHEN an optional inline origin is outside the worker's authorized scope, THE completion SHALL leave that work unstarted for ordinary authorized dispatch.

### Requirement 11: Re-Enqueue Safety via Existing Timeout Mechanisms

**User Story:** As a Tokeira developer, I want eagerly dispatched tasks to be recovered if the client never completes them (e.g., connection drop after response), so that no work is permanently lost.

#### Acceptance Criteria

1. WHEN an Eager_Workflow_Task is committed as started and returned to the client, THE Runtime SHALL rely on the existing WFT start-to-close timeout mechanism to detect non-completion and reschedule the workflow task.
2. IF an Eager_Activity_Task response is lost, THEN THE activity SHALL remain started until its applicable start-to-close, heartbeat or schedule-to-close timeout resolves it; retry policy SHALL determine retry versus terminal timeout.
3. THE eager dispatch path SHALL NOT immediately reoffer work on response loss or apply schedule-to-start timeout to work already started.

### Requirement 12: Authoritative Commit and Publish Ordering

**User Story:** As a Tokeira developer, I want every eager response to be derived from committed authoritative state, so that a lost broker entry or response cannot lose work.

#### Acceptance Criteria

1. WHEN eager workflow start is accepted, THE Runtime SHALL commit the run and started first WFT before constructing `StartWorkflowExecutionResponse`.
2. WHEN a completion starts activities inline, THE runtime SHALL commit their started state in the completion before exposing any task token.
3. IF the completion commit fails, THEN THE edge SHALL expose no inline tasks.
4. WHEN optional task provenance cannot be registered after commit, THE edge SHALL withhold that task and let its started timeout recover it; THE runtime SHALL NOT undo or republish its start.

### Requirement 13: Request-ID Retry Fidelity

**User Story:** As an SDK user, I want a retried eager start to return the same still-live first WFT, so that transport retries do not discard the eager optimization or duplicate work.

#### Acceptance Criteria

1. WHEN an eager `StartWorkflowExecution` request is retried with the same request ID, the authoritative start event records `eager_execution_accepted=true`, and the first WFT remains started with `started_event_id=3`, `attempt=1`, and an unexpired start-to-close deadline, THE Runtime SHALL return that WFT in `eager_workflow_task`.
2. WHEN the same start request is retried after the first WFT timed out, its start-to-close deadline elapsed, or it fell back to a later attempt, THE Runtime SHALL omit `eager_workflow_task`.
3. WHEN an eager start is retried, THE Runtime SHALL NOT author a second workflow start or workflow-task-start transition.
4. WHEN a duplicate request changes `request_eager_execution` from false to true but the authoritative start event records `eager_execution_accepted=false`, THE Runtime SHALL omit `eager_workflow_task`.

### Requirement 14: Capability Advertisement

**User Story:** As an SDK user, I want the server to advertise eager workflow start when it implements the behaviour, so that SDK eager dispatchers actually request the feature.

#### Acceptance Criteria

1. WHERE the pinned eager-workflow-start default is enabled, THE `GetSystemInfo` response SHALL report `capabilities.eager_workflow_start=true`.
2. WHERE the pinned eager-workflow-start default is enabled, THE namespace capability response SHALL report `eager_workflow_start=true`.
3. WHEN Tier 3.18 passes clean, THE compatibility matrix SHALL classify eager workflow start as `Implemented` with corpus evidence.
