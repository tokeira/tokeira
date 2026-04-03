# Requirements Document: Runtime Complete Implementation

## Introduction

This document captures the full requirements for the Tokeira runtime (`tokeira-runtime`), the execution shell around the pure kernel. The runtime owns lane-based execution, task delivery, activity lifecycle, timer scanning, child workflow orchestration, external signal/cancel routing, continue-as-new chaining, Nexus dispatch, worker versioning, sweeper recovery, durable backlog, query dispatch, update lifecycle, and broker fairness.

The runtime calls the kernel for state transitions and storage for persistence. It never computes authoritative state itself — it orchestrates the flow from command arrival through kernel application, storage commit, and derived-effect publication.

The authoritative specifications are [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md), [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md), and [090-failover-and-recovery](../../../docs/architecture/090-failover-and-recovery.md).

The implementation is organized into 15 incremental features with explicit dependency ordering. Feature 1 hardens the existing lane execution path. Features 2–5 add activity, timer, and timeout infrastructure. Features 6–9 add cross-run orchestration. Features 10–15 add operational maturity (versioning, recovery, backlog, fairness, queries, updates).

**Dependency graph:**

- Feature 1 (Lane OCC Retry and Mailbox Coalescing) — no dependencies, partially implemented
- Feature 2 (Activity Pump) — depends on Feature 1
- Feature 3 (Activity Heartbeat and Timeouts) — depends on Feature 2
- Feature 4 (Timer Scanner) — depends on Feature 1
- Feature 5 (Workflow Timeouts) — depends on Feature 1
- Feature 6 (Child Workflow Orchestration) — depends on Features 1, 2
- Feature 7 (External Signal and Cancel Delivery) — depends on Feature 1
- Feature 8 (Continue-As-New) — depends on Features 1, 5, 6
- Feature 9 (Nexus Operation Dispatch) — depends on Features 1, 2
- Feature 10 (Worker Versioning and Deployment Routing) — depends on Features 1, 2
- Feature 11 (Sweeper and Recovery) — depends on Features 1, 2, 4
- Feature 12 (Durable Backlog Integration) — depends on Features 1, 2, 11
- Feature 13 (Query Dispatch) — depends on Feature 1
- Feature 14 (Update Two-Phase Lifecycle) — depends on Feature 1
- Feature 15 (Broker Fairness and Admission) — depends on Features 1, 2, 12

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication. Performs I/O but delegates state transition logic to the Kernel.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(shard_id, run_key) mod lane_count`.
- **Run_Actor**: A demand-loaded in-memory object representing one workflow run on a lane. Loads state, drains mailbox, invokes kernel, commits, publishes effects, then parks or evicts.
- **Mailbox**: The per-run message queue on a lane. Multiple commands for the same run can be drained in one activation cycle.
- **Mailbox_Coalescing**: Draining multiple mailbox items for the same run before parking, subject to fairness and transaction-size bounds.
- **OCC_Conflict**: An optimistic concurrency control conflict returned by storage when `expected_seq` does not match the durable transition sequence. Indicates another writer committed first.
- **Broker**: The in-memory delivery subsystem (`InMemoryBroker`) that matches pending tasks with waiting pollers. Not authoritative — the sweeper can reconstruct its state from durable storage.
- **Sync_Match**: Matching a newly published task with an already-waiting poller, avoiding durable backlog entirely.
- **Live_Ready**: Short-lived in-memory ready structure (Tier B) where tasks wait for near-future poller matches before falling back to durable backlog.
- **Durable_Backlog**: Persistent task storage (Tier C) used when tasks survive past the live-ready grace window.
- **Sweeper**: Background process that reconstructs volatile delivery state from authoritative durable state after restart or shard failover.
- **Timer_Scanner**: Background task that scans timer buckets for due timers and injects `TimerDue` commands into run actor mailboxes.
- **Activity_Pump**: The subsystem responsible for activity task dispatch, polling, completion, failure, and retry.
- **Heartbeat**: Periodic progress report from a running activity to the runtime, used for timeout detection and cancellation propagation.
- **Shard_Epoch**: Monotonically increasing fencing token for shard ownership. Stale owners cannot commit transitions.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from a committed transition.
- **CommitResult**: The outcome of a storage commit — Applied (with new state), Conflict (OCC failure), or Duplicate (request already processed).
- **Sticky_Routing**: Preferential routing of workflow tasks back to the worker that last executed the run, to reuse cached workflow state.
- **Parent_Close_Policy**: Policy applied to open child workflows when the parent closes: Terminate, RequestCancel, or Abandon.
- **Continue_As_New**: Workflow feature where the current run closes and a successor run starts with fresh history, preserving logical execution identity.
- **Nexus_Operation**: Cross-namespace service invocation through typed contracts, dispatched by the runtime on behalf of the kernel.
- **Worker_Versioning**: Deployment-aware task routing where workers register with version/deployment metadata and the broker matches tasks to compatible workers.

## Requirements

---

## Feature 1: Lane OCC Retry and Mailbox Coalescing (partially implemented)

### Requirement 1.1: OCC Retry Loop on Commit Conflict

**User Story:** As a Tokeira developer, I want the lane to automatically retry on OCC conflicts, so that concurrent writes to the same run are resolved without surfacing transient failures to callers.

#### Acceptance Criteria

1. WHEN `handle_message` receives a `CommitResult::Conflict` from storage, THE Lane SHALL reload the run state from storage and recompute the transition via the Kernel.
2. WHEN `handle_message` retries after an OCC conflict, THE Lane SHALL use the freshly loaded state for the retry attempt.
3. WHEN `handle_message` retries after an OCC conflict, THE Lane SHALL pass the same original Command to the Kernel on each retry.
4. THE Lane SHALL bound the number of OCC retry attempts to a configurable maximum (default 5).
5. IF the retry count exceeds the configured maximum, THEN THE Lane SHALL return an error indicating retry exhaustion.
6. WHEN an OCC retry succeeds, THE Lane SHALL return the successful CommitResult to the caller.

### Requirement 1.2: Mailbox Coalescing

**User Story:** As a Tokeira developer, I want the lane to drain multiple mailbox items for the same run before parking, so that bursty signal floods are processed efficiently without repeated load/park cycles.

#### Acceptance Criteria

1. WHEN a run actor is activated to process a command, THE Lane SHALL check for additional pending mailbox items for the same run before parking.
2. WHEN additional mailbox items exist for the same run, THE Lane SHALL drain and process them in the same activation cycle.
3. THE Lane SHALL bound the number of mailbox items drained per activation to a configurable maximum to preserve fairness across runs.
4. WHEN multiple mailbox items are drained in one activation, THE Lane SHALL process each item sequentially (load, kernel apply, commit) using the latest committed state.
5. WHEN a mailbox item fails during a coalesced drain, THE Lane SHALL return the error for that item and stop draining further items for that run in the current activation.

### Requirement 1.3: Lane Message Routing

**User Story:** As a Tokeira developer, I want commands to be routed to a deterministic lane based on run identity, so that per-run serialization is maintained by construction.

#### Acceptance Criteria

1. THE Runtime SHALL route commands to lanes using `hash(run_key) mod lane_count`.
2. THE Runtime SHALL maintain at least one lane at all times.
3. WHEN a command is submitted for a run, THE Runtime SHALL always route it to the same lane for a given lane_count.

### Requirement 1.4: Dispatch Op Publication After Commit

**User Story:** As a Tokeira developer, I want the runtime to publish all dispatch ops from a committed transition, so that derived effects (workflow tasks, activity tasks, child starts, signals, etc.) are acted upon.

#### Acceptance Criteria

1. WHEN a transition is committed successfully, THE Runtime SHALL inspect the transition's dispatch_ops and publish each one to the appropriate subsystem.
2. WHEN a DispatchOp::EnqueueWorkflowTask is present, THE Runtime SHALL publish the workflow task to the Broker.
3. WHEN a DispatchOp::EnqueueActivityTask is present, THE Runtime SHALL publish the activity task to the activity delivery subsystem.
4. WHEN a DispatchOp for child workflows, external signals, external cancels, or Nexus operations is present, THE Runtime SHALL route it to the corresponding orchestration handler.
5. THE Runtime SHALL publish dispatch ops only after the storage commit succeeds, not before.

---

## Feature 2: Activity Pump — Dispatch, Poll, Complete, Retry

**Depends on:** Feature 1

### Requirement 2.1: Activity Task Broker

**User Story:** As a Tokeira developer, I want an activity task delivery subsystem parallel to the workflow task broker, so that activity tasks can be matched with activity workers.

#### Acceptance Criteria

1. THE Runtime SHALL maintain an activity task broker that accepts published activity tasks and matches them with waiting activity pollers.
2. THE Activity_Broker SHALL key tasks and pollers by QueueKey (namespace, task_queue, activity task kind, deployment, build_id).
3. THE Activity_Broker SHALL support deduplication by (run_key, activity_id, attempt) to prevent duplicate dispatch.
4. THE Activity_Broker SHALL support a notify/wake mechanism so pollers are woken when new tasks arrive.

### Requirement 2.2: Poll Activity Task Endpoint

**User Story:** As a Tokeira developer, I want a `poll_activity_task` endpoint, so that activity workers can receive activity tasks.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `poll_activity_task` method that accepts a QueueKey, worker identity, and timeout duration.
2. WHEN a compatible activity task is available, THE Runtime SHALL return the task to the poller.
3. WHEN no compatible activity task is available within the timeout, THE Runtime SHALL return None.
4. WHEN an activity task is matched to a poller, THE Runtime SHALL perform an activity-task-start transaction that records the start in authoritative state.

### Requirement 2.3: Complete Activity Task Endpoint

**User Story:** As a Tokeira developer, I want a `complete_activity_task` endpoint, so that workers can report successful activity completion.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `complete_activity_task` method that accepts an activity task token and result payload.
2. WHEN a valid activity completion is received, THE Runtime SHALL submit an `ActivityResolved` command with a Completed resolution to the owning run via the lane.
3. WHEN the activity task token is stale (mismatched attempt, unknown activity, or closed run), THE Runtime SHALL reject the completion cleanly without mutating state.

### Requirement 2.4: Fail Activity Task Endpoint

**User Story:** As a Tokeira developer, I want a `fail_activity_task` endpoint, so that workers can report activity failures.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `fail_activity_task` method that accepts an activity task token and failure details.
2. WHEN a valid activity failure is received and the activity's retry policy permits retry, THE Runtime SHALL re-dispatch the activity with an incremented attempt count rather than submitting an ActivityResolved command.
3. WHEN a valid activity failure is received and the retry policy is exhausted (max attempts reached or non-retryable error), THE Runtime SHALL submit an `ActivityResolved` command with a Failed resolution to the owning run.
4. THE Runtime SHALL evaluate retry policy logic (max attempts, non-retryable error types, backoff interval) outside the kernel.

### Requirement 2.5: Activity Task Start Transaction

**User Story:** As a Tokeira developer, I want activity task starts to be recorded authoritatively, so that stale completions can be rejected and duplicate starts are prevented.

#### Acceptance Criteria

1. WHEN an activity task is matched to a poller, THE Runtime SHALL record the start attempt in authoritative activity state.
2. THE activity task token SHALL encode run_key, activity_id, schedule_event_id, attempt, and shard_epoch.
3. WHEN a completion or failure arrives with a token that does not match the current authoritative activity state, THE Runtime SHALL reject it.

### Requirement 2.6: Activity Dispatch Op Handling

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::EnqueueActivityTask from committed transitions, so that scheduled activities are delivered to workers.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::EnqueueActivityTask, THE Runtime SHALL publish the activity task to the Activity_Broker.
2. THE published activity task SHALL carry the queue key, run_key, activity_id, schedule_event_id, attempt, and timeout configuration from the dispatch op.

---

## Feature 3: Activity Heartbeat and Timeouts

**Depends on:** Feature 2

### Requirement 3.1: Record Activity Heartbeat Endpoint

**User Story:** As a Tokeira developer, I want a `record_activity_heartbeat` endpoint, so that long-running activities can report progress and detect cancellation.

#### Acceptance Criteria

1. THE Runtime SHALL expose a `record_activity_heartbeat` method that accepts an activity task token and heartbeat details.
2. WHEN a valid heartbeat is received, THE Runtime SHALL update the last-heartbeat timestamp for the activity.
3. WHEN a heartbeat is received for an activity that has a pending cancellation, THE Runtime SHALL return a cancellation indicator to the caller.
4. WHEN a heartbeat is received with a stale token, THE Runtime SHALL reject it cleanly.

### Requirement 3.2: Heartbeat Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect heartbeat timeouts, so that unresponsive activities are terminated.

#### Acceptance Criteria

1. WHEN an activity has a configured heartbeat_timeout and the elapsed time since the last heartbeat exceeds the timeout, THE Runtime SHALL submit an `ActivityResolved` command with a TimedOut resolution.
2. THE Runtime SHALL run a background scanner that periodically checks started activities for heartbeat timeout violations.
3. THE heartbeat timeout scanner SHALL use configurable scan intervals.

### Requirement 3.3: Schedule-to-Start Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect schedule-to-start timeouts, so that activities stuck in the dispatch queue are timed out.

#### Acceptance Criteria

1. WHEN an activity has a configured schedule_to_start_timeout and the elapsed time since scheduling exceeds the timeout without the activity being started, THE Runtime SHALL submit an `ActivityResolved` command with a TimedOut resolution.
2. THE Runtime SHALL check schedule-to-start timeout as part of the activity timeout scanning background task.

### Requirement 3.4: Start-to-Close Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect start-to-close timeouts, so that activities that run too long are terminated.

#### Acceptance Criteria

1. WHEN an activity has a configured start_to_close_timeout and the elapsed time since the activity was started exceeds the timeout, THE Runtime SHALL submit an `ActivityResolved` command with a TimedOut resolution.
2. THE Runtime SHALL check start-to-close timeout as part of the activity timeout scanning background task.

### Requirement 3.5: Schedule-to-Close Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect schedule-to-close timeouts, so that the overall activity lifecycle is bounded.

#### Acceptance Criteria

1. WHEN an activity has a configured schedule_to_close_timeout and the elapsed time since scheduling exceeds the timeout, THE Runtime SHALL submit an `ActivityResolved` command with a TimedOut resolution regardless of whether the activity has been started.
2. THE schedule-to-close timeout SHALL take precedence when it fires before other timeout types.

---

## Feature 4: Timer Scanner

**Depends on:** Feature 1

### Requirement 4.1: Background Timer Scanning

**User Story:** As a Tokeira developer, I want a background timer scanner, so that due timers are detected and delivered to their owning runs.

#### Acceptance Criteria

1. THE Runtime SHALL run a background task that periodically calls `storage.list_due_timers(now, limit)` to discover timers whose fire_at has passed.
2. WHEN due timers are discovered, THE Timer_Scanner SHALL inject a `Command::TimerDue` for each timer into the owning run's lane mailbox.
3. THE Timer_Scanner SHALL use a configurable scan interval (default suitable for sub-second timer resolution).
4. THE Timer_Scanner SHALL use a configurable batch limit to bound the number of timers processed per scan cycle.

### Requirement 4.2: Timer Scanning Is Not Authoritative

**User Story:** As a Tokeira developer, I want timer scanning to be non-authoritative, so that duplicate or stale timer firings are harmless.

#### Acceptance Criteria

1. THE Timer_Scanner SHALL NOT modify authoritative state directly; the authoritative transition happens when the Kernel processes the TimerDue command.
2. WHEN a TimerDue command is delivered for a timer that has already been canceled or fired, THE Kernel SHALL reject it with UnknownTimer, and THE Runtime SHALL treat that rejection as a harmless no-op.
3. WHEN a TimerDue command is delivered for a run that is already closed, THE Kernel SHALL reject it with RunClosed, and THE Runtime SHALL treat that rejection as a harmless no-op.

### Requirement 4.3: Timer Scanner Distributed Coordination

**User Story:** As a Tokeira developer, I want timer scanning to be scoped to owned shards, so that multiple runtime nodes do not duplicate timer work.

#### Acceptance Criteria

1. THE Timer_Scanner SHALL only scan timer buckets for shards owned by the current runtime node.
2. WHEN shard ownership changes, THE Timer_Scanner SHALL stop scanning timers for relinquished shards and begin scanning for newly acquired shards.

---

## Feature 5: Workflow Timeouts

**Depends on:** Feature 1

### Requirement 5.1: Workflow Execution Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect workflow execution timeouts, so that workflows that exceed their configured execution timeout are terminated.

#### Acceptance Criteria

1. WHEN a workflow has a configured workflow_execution_timeout and the elapsed time since workflow start exceeds the timeout, THE Runtime SHALL submit a `Command::WorkflowExecutionTimedOut` with timeout_type ExecutionTimeout to the owning run.
2. THE Runtime SHALL run a background scanner that periodically checks open runs approaching their execution timeout.
3. THE workflow execution timeout scanner SHALL use configurable scan intervals.

### Requirement 5.2: Workflow Run Timeout Detection

**User Story:** As a Tokeira developer, I want the runtime to detect workflow run timeouts, so that individual runs within a retry or continue-as-new chain are bounded.

#### Acceptance Criteria

1. WHEN a workflow has a configured workflow_run_timeout and the elapsed time since the current run started exceeds the timeout, THE Runtime SHALL submit a `Command::WorkflowExecutionTimedOut` with timeout_type RunTimeout to the owning run.
2. THE Runtime SHALL check workflow run timeouts as part of the same background scanner used for execution timeouts.

### Requirement 5.3: Workflow Timeout Is Non-Authoritative

**User Story:** As a Tokeira developer, I want workflow timeout detection to be non-authoritative, so that duplicate or stale timeout commands are harmless.

#### Acceptance Criteria

1. THE workflow timeout scanner SHALL NOT modify authoritative state directly; the authoritative transition happens when the Kernel processes the WorkflowExecutionTimedOut command.
2. WHEN a WorkflowExecutionTimedOut command is delivered for a run that is already closed, THE Kernel SHALL reject it with RunClosed, and THE Runtime SHALL treat that rejection as a harmless no-op.

---

## Feature 6: Child Workflow Orchestration

**Depends on:** Features 1, 2

### Requirement 6.1: Start Child Workflow Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::StartChildWorkflow, so that child workflow executions are created when the parent requests them.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::StartChildWorkflow, THE Runtime SHALL issue a `Command::Start` for the child workflow with the specified namespace, workflow_id, workflow_type, task_queue, and input.
2. WHEN the child start succeeds, THE Runtime SHALL submit a `Command::ChildStartConfirmed` with a success variant (carrying child_run_id and workflow_type) to the parent run.
3. WHEN the child start fails (e.g., workflow ID already exists), THE Runtime SHALL submit a `Command::ChildStartConfirmed` with a failure variant to the parent run.

### Requirement 6.2: Terminate Child Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::TerminateChild, so that parent close policy can terminate child workflows.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::TerminateChild, THE Runtime SHALL submit a `Command::Terminate` to the child run identified by child_workflow_id and child_run_id.
2. IF the child run is already closed or not found, THEN THE Runtime SHALL treat the termination as a harmless no-op.

### Requirement 6.3: Cancel Child Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::CancelChild, so that parent close policy can request cancellation of child workflows.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::CancelChild, THE Runtime SHALL submit a `Command::Cancel` to the child run identified by child_workflow_id and child_run_id.
2. IF the child run is already closed or not found, THEN THE Runtime SHALL treat the cancellation as a harmless no-op.

### Requirement 6.4: Child Resolution Delivery

**User Story:** As a Tokeira developer, I want the runtime to deliver child workflow resolutions back to the parent, so that the parent can observe child completion.

#### Acceptance Criteria

1. WHEN a child workflow run reaches a terminal state (Completed, Failed, Canceled, Terminated, TimedOut), THE Runtime SHALL submit a `Command::ChildResolved` to the parent run with the appropriate resolution variant.
2. THE Runtime SHALL identify the parent run from the child's execution metadata (parent workflow ID and run ID recorded at child start time).
3. IF the parent run is already closed when the child resolves, THEN THE Runtime SHALL treat the delivery as a harmless no-op.

### Requirement 6.5: Parent Close Policy Enforcement

**User Story:** As a Tokeira developer, I want the runtime to enforce parent close policy dispatch ops, so that child workflows are handled according to the configured policy when the parent closes.

#### Acceptance Criteria

1. WHEN a committed transition closes a parent run and the Kernel emits DispatchOp::TerminateChild ops, THE Runtime SHALL execute each termination dispatch.
2. WHEN a committed transition closes a parent run and the Kernel emits DispatchOp::CancelChild ops, THE Runtime SHALL execute each cancel dispatch.
3. THE Runtime SHALL process parent close policy dispatch ops asynchronously; failure to terminate or cancel a child SHALL NOT block the parent's close commit.

---

## Feature 7: External Signal and Cancel Delivery

**Depends on:** Feature 1

### Requirement 7.1: Signal External Workflow Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::SignalExternalWorkflow, so that workflows can signal other workflow executions.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::SignalExternalWorkflow, THE Runtime SHALL resolve the target workflow execution and submit a `Command::Signal` to the target run.
2. WHEN the signal delivery succeeds, THE Runtime SHALL submit a `Command::ExternalSignalResolved` with a Signaled result to the originating run.
3. WHEN the signal delivery fails (target not found, target closed), THE Runtime SHALL submit a `Command::ExternalSignalResolved` with a Failed result to the originating run.

### Requirement 7.2: Cancel External Workflow Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::RequestCancelExternalWorkflow, so that workflows can request cancellation of other workflow executions.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::RequestCancelExternalWorkflow, THE Runtime SHALL resolve the target workflow execution and submit a `Command::Cancel` to the target run.
2. WHEN the cancel delivery succeeds, THE Runtime SHALL submit a `Command::ExternalCancelResolved` with a CancelRequested result to the originating run.
3. WHEN the cancel delivery fails (target not found, target closed), THE Runtime SHALL submit a `Command::ExternalCancelResolved` with a Failed result to the originating run.

### Requirement 7.3: Cross-Namespace Signal and Cancel Routing

**User Story:** As a Tokeira developer, I want external signal and cancel dispatch to support cross-namespace routing, so that workflows in different namespaces can communicate.

#### Acceptance Criteria

1. WHEN a DispatchOp::SignalExternalWorkflow or DispatchOp::RequestCancelExternalWorkflow targets a workflow in a different namespace, THE Runtime SHALL resolve the target execution in the target namespace.
2. THE Runtime SHALL use the same execution resolution mechanism (storage `resolve_execution`) regardless of whether the target is in the same or a different namespace.

---

## Feature 8: Continue-As-New

**Depends on:** Features 1, 5, 6

### Requirement 8.1: Successor Run Creation

**User Story:** As a Tokeira developer, I want the runtime to create successor runs for continue-as-new, so that workflows can checkpoint into fresh history.

#### Acceptance Criteria

1. WHEN a committed transition closes a run with ExecutionStatus::ContinuedAsNew, THE Runtime SHALL read the WorkflowExecutionContinuedAsNew event from the committed history events.
2. THE Runtime SHALL issue a `Command::Start` for the successor run using the new_run_id, workflow_type, task_queue, input, memo, search_attributes, and timeout configuration from the continued-as-new event.
3. THE successor Start command SHALL carry `continued_execution_run_id` set to the current run's run_id.

### Requirement 8.2: Execution Chain Identity

**User Story:** As a Tokeira developer, I want continue-as-new to preserve execution chain identity, so that the logical workflow execution can be traced across runs.

#### Acceptance Criteria

1. WHEN the current run has a `first_execution_run_id`, THE Runtime SHALL propagate it to the successor Start command.
2. WHEN the current run does not have a `first_execution_run_id` (it is the first run in the chain), THE Runtime SHALL set `first_execution_run_id` to the current run's run_id on the successor Start command.

### Requirement 8.3: Successor Start Failure Handling

**User Story:** As a Tokeira developer, I want the runtime to handle successor start failures gracefully, so that continue-as-new failures do not leave the execution chain in an inconsistent state.

#### Acceptance Criteria

1. IF the successor Start command fails (e.g., workflow ID conflict from a concurrent start), THEN THE Runtime SHALL log the failure with sufficient context for operational diagnosis.
2. IF the successor Start command fails, THEN THE Runtime SHALL NOT attempt to reopen or modify the already-closed predecessor run.
3. THE Runtime SHALL retry the successor Start command with bounded retries before giving up.

---

## Feature 9: Nexus Operation Dispatch

**Depends on:** Features 1, 2

### Requirement 9.1: Schedule Nexus Operation Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::ScheduleNexusOperation, so that workflows can invoke cross-namespace Nexus services.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::ScheduleNexusOperation, THE Runtime SHALL resolve the Nexus endpoint and dispatch the operation via HTTP to the target service.
2. THE outbound Nexus request SHALL carry the operation_id, endpoint, service, operation name, input payload, and schedule_to_close_timeout.
3. WHEN the Nexus operation completes synchronously, THE Runtime SHALL submit a `Command::NexusOperationResolved` with the appropriate resolution (Completed, Failed) to the originating run.
4. WHEN the Nexus operation is accepted asynchronously, THE Runtime SHALL submit a `Command::NexusOperationResolved` with a Started resolution to the originating run.

### Requirement 9.2: Cancel Nexus Operation Dispatch

**User Story:** As a Tokeira developer, I want the runtime to handle DispatchOp::CancelNexusOperation, so that workflows can cancel pending Nexus operations.

#### Acceptance Criteria

1. WHEN a committed transition contains a DispatchOp::CancelNexusOperation, THE Runtime SHALL send a cancellation request to the Nexus endpoint for the identified operation.
2. IF the cancellation request fails or the operation has already completed, THEN THE Runtime SHALL treat the failure as a harmless no-op.

### Requirement 9.3: Nexus Operation Timeout Handling

**User Story:** As a Tokeira developer, I want the runtime to enforce Nexus operation timeouts, so that unresponsive Nexus services do not block workflow progress indefinitely.

#### Acceptance Criteria

1. WHEN a Nexus operation has a configured schedule_to_close_timeout and the elapsed time since scheduling exceeds the timeout, THE Runtime SHALL submit a `Command::NexusOperationResolved` with a TimedOut resolution to the originating run.
2. THE Runtime SHALL track pending Nexus operations and check for timeout violations as part of a background scanning task.

### Requirement 9.4: Nexus Endpoint Resolution

**User Story:** As a Tokeira developer, I want the runtime to resolve Nexus endpoints to network addresses, so that outbound Nexus operations can be dispatched.

#### Acceptance Criteria

1. THE Runtime SHALL maintain a Nexus endpoint registry that maps endpoint names to network addresses and service metadata.
2. WHEN a DispatchOp::ScheduleNexusOperation references an unknown endpoint, THE Runtime SHALL submit a `Command::NexusOperationResolved` with a Failed resolution indicating endpoint not found.

---

## Feature 10: Worker Versioning and Deployment Routing

**Depends on:** Features 1, 2

### Requirement 10.1: Deployment-Aware Broker Routing

**User Story:** As a Tokeira developer, I want the broker to route tasks based on deployment and build_id, so that versioned workers receive compatible tasks.

#### Acceptance Criteria

1. WHEN a task is published with a non-None deployment or build_id in its QueueKey, THE Broker SHALL only match it with pollers registered for a compatible deployment/build_id.
2. WHEN a task is published with None deployment and build_id, THE Broker SHALL match it with any compatible poller for the same namespace and task_queue.
3. THE Broker SHALL maintain separate ready queues per QueueKey, including deployment/build_id dimensions.

### Requirement 10.2: Worker Registration with Version Metadata

**User Story:** As a Tokeira developer, I want workers to register with version and deployment metadata, so that the broker can perform version-aware matching.

#### Acceptance Criteria

1. THE Runtime SHALL accept worker registration that includes optional deployment identifier and build_id.
2. WHEN a worker polls for tasks, THE Runtime SHALL use the worker's registered deployment/build_id to determine QueueKey compatibility.
3. THE Runtime SHALL allow workers without deployment metadata to receive tasks with None deployment in the QueueKey.

### Requirement 10.3: Version-Aware Task Matching

**User Story:** As a Tokeira developer, I want the broker to perform version-aware task matching, so that tasks are delivered to workers that can execute them correctly.

#### Acceptance Criteria

1. THE Broker SHALL match tasks to pollers only when the poller's deployment/build_id is compatible with the task's QueueKey deployment/build_id.
2. WHEN no compatible poller is available, THE Broker SHALL hold the task in the ready queue until a compatible poller arrives or the task falls through to durable backlog.
3. THE Broker SHALL NOT deliver a task to an incompatible worker under any circumstances.

---

## Feature 11: Sweeper and Recovery

**Depends on:** Features 1, 2, 4

### Requirement 11.1: Post-Failover Dispatchable Work Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to reconstruct dispatchable work from authoritative state after failover, so that no work is lost when broker memory is discarded.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan authoritative state for that shard to discover all pending dispatchable work.
2. THE Sweeper SHALL scan `workflow_hot` for runs with pending workflow tasks (scheduled but not started) and republish them to the Broker.
3. THE Sweeper SHALL scan `activity_state` for dispatchable activity attempts and republish them to the Activity_Broker.
4. THE Sweeper SHALL scan `timer_bucket` for due timers and inject `TimerDue` commands into the appropriate run actor mailboxes.

### Requirement 11.2: Expired Sticky Claim Cleanup

**User Story:** As a Tokeira developer, I want the sweeper to clear expired sticky claims, so that workflow tasks are not stuck waiting for a dead worker.

#### Acceptance Criteria

1. THE Sweeper SHALL identify runs with sticky affinity where the sticky expiry has passed.
2. WHEN an expired sticky claim is found on a pending workflow task, THE Sweeper SHALL republish the task to the Broker without sticky preference so it can be matched to any compatible worker.

### Requirement 11.3: Shard Lease Acquisition and Epoch Fencing

**User Story:** As a Tokeira developer, I want shard acquisition to establish a fencing epoch, so that stale owners cannot commit transitions.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Runtime SHALL obtain a new shard epoch via `LeaseRepository::try_acquire_bundle`.
2. THE Runtime SHALL include the current shard epoch in all transition commits for runs in that shard.
3. WHEN a commit is attempted with a stale shard epoch, THE storage layer SHALL reject it.
4. THE Runtime SHALL periodically renew the shard lease via `LeaseRepository::renew_bundle`.
5. IF lease renewal fails, THEN THE Runtime SHALL stop accepting new commands for runs in that shard and drain in-flight work.

### Requirement 11.4: Operational Recovery Sequence

**User Story:** As a Tokeira developer, I want shard acquisition to follow a defined sequence, so that recovery is orderly and does not overwhelm storage.

#### Acceptance Criteria

1. WHEN a shard is acquired, THE Runtime SHALL follow this sequence: (1) acquire lease and epoch, (2) start control tasks (lease renewer, timer scanner, sweeper), (3) rebuild dispatchable work into broker, (4) admit new commands, (5) let demand load actors lazily.
2. THE Runtime SHALL NOT eagerly rehydrate all run actors after shard acquisition; actors SHALL be loaded on demand.
3. THE Runtime SHALL NOT admit new commands for a shard until the sweeper has completed its initial scan for that shard.

---

## Feature 12: Durable Backlog Integration

**Depends on:** Features 1, 2, 11

### Requirement 12.1: Broker Tier C — Persist to Backlog

**User Story:** As a Tokeira developer, I want the broker to persist tasks to durable backlog when the live-ready window expires, so that tasks are not lost if no poller arrives promptly.

#### Acceptance Criteria

1. WHEN a task has been in the live-ready tier for longer than the configured grace window without being matched, THE Broker SHALL persist it to durable backlog via `storage.persist_to_backlog`.
2. THE live-ready grace window SHALL be configurable (default suitable for typical poller arrival latency).
3. WHEN a task is persisted to backlog, THE Broker SHALL remove it from the live-ready tier to avoid double dispatch.

### Requirement 12.2: Drain Backlog for Broker Sweep

**User Story:** As a Tokeira developer, I want the broker to drain tasks from durable backlog, so that persisted tasks are eventually delivered to workers.

#### Acceptance Criteria

1. THE Broker SHALL periodically call `storage.drain_backlog` to retrieve persisted tasks for queues with waiting pollers.
2. WHEN backlog tasks are drained, THE Broker SHALL attempt to match them with waiting pollers using the same matching logic as live-ready tasks.
3. THE Broker SHALL use configurable drain intervals and batch limits.

### Requirement 12.3: Backlog Fairness Policy

**User Story:** As a Tokeira developer, I want the backlog to enforce fairness, so that no single workflow or namespace monopolizes task delivery.

#### Acceptance Criteria

1. THE Broker SHALL deliver backlog tasks in FIFO order within the same priority band.
2. THE Broker SHALL support priority bands for backlog tasks, with higher-priority tasks delivered before lower-priority tasks.
3. THE Broker SHALL NOT allow backlog delivery to starve fresh sync-matchable work from the live-ready tier.

---

## Feature 13: Query Dispatch

**Depends on:** Feature 1

### Requirement 13.1: Read-Only Query Routing

**User Story:** As a Tokeira developer, I want the runtime to route queries to workers without kernel involvement, so that read-only queries do not create transitions or modify state.

#### Acceptance Criteria

1. THE Runtime SHALL expose a query dispatch method that routes a query to a worker currently executing or recently cached for the target run.
2. THE Runtime SHALL NOT submit queries as commands to the Kernel; queries do not produce transitions, history events, or dispatch ops.
3. THE Runtime SHALL NOT modify authoritative run state as a result of query dispatch.

### Requirement 13.2: Query Timeout Handling

**User Story:** As a Tokeira developer, I want query dispatch to have timeout handling, so that unresponsive workers do not block query callers indefinitely.

#### Acceptance Criteria

1. THE Runtime SHALL enforce a configurable timeout on query dispatch.
2. WHEN a query times out, THE Runtime SHALL return a timeout error to the caller.
3. THE Runtime SHALL NOT modify run state or create transitions as a result of a query timeout.

---

## Feature 14: Update Two-Phase Lifecycle

**Depends on:** Feature 1

### Requirement 14.1: Update Command Routing

**User Story:** As a Tokeira developer, I want the runtime to route Update commands through the kernel, so that updates are recorded in history and delivered to workers.

#### Acceptance Criteria

1. WHEN an Update request is received, THE Runtime SHALL submit a `Command::Update` to the owning run via the lane.
2. THE Runtime SHALL publish any resulting dispatch ops (workflow task scheduling) after the commit succeeds.

### Requirement 14.2: Update Acceptance/Rejection/Completion Lifecycle

**User Story:** As a Tokeira developer, I want the runtime to manage the update lifecycle, so that callers can wait for update acceptance and completion.

#### Acceptance Criteria

1. THE Runtime SHALL support callers waiting for update acceptance (the point at which the worker accepts or rejects the update).
2. THE Runtime SHALL support callers waiting for update completion (the point at which the worker completes the update with a result).
3. WHEN the worker rejects an update via an UpdateRejected workflow command, THE Runtime SHALL notify the waiting caller with the rejection reason.
4. WHEN the worker completes an update via an UpdateCompleted workflow command, THE Runtime SHALL notify the waiting caller with the result.

### Requirement 14.3: Update Timeout Handling

**User Story:** As a Tokeira developer, I want update dispatch to have timeout handling, so that unresponsive workers do not block update callers indefinitely.

#### Acceptance Criteria

1. THE Runtime SHALL enforce a configurable timeout on update lifecycle waiting.
2. WHEN an update times out waiting for acceptance or completion, THE Runtime SHALL return a timeout error to the caller.
3. THE Runtime SHALL NOT modify run state as a result of an update timeout at the runtime level; the update remains pending in the kernel's state.

---

## Feature 15: Broker Fairness and Admission

**Depends on:** Features 1, 2, 12

### Requirement 15.1: Per-Namespace Admission and Caps

**User Story:** As a Tokeira developer, I want the broker to enforce per-namespace admission limits, so that no single namespace monopolizes task delivery capacity.

#### Acceptance Criteria

1. THE Broker SHALL support configurable per-namespace caps on the number of concurrent outstanding tasks.
2. WHEN a namespace exceeds its configured cap, THE Broker SHALL defer new task publications for that namespace until capacity is available.
3. THE Broker SHALL NOT drop tasks when a namespace cap is reached; deferred tasks SHALL remain in the live-ready or backlog tier.

### Requirement 15.2: Fairness Budgets Between Delivery Sources

**User Story:** As a Tokeira developer, I want the broker to balance delivery between sticky, live-ready, and backlog sources, so that no single source starves the others.

#### Acceptance Criteria

1. THE Broker SHALL maintain weighted service budgets across sticky offers, live-ready offers, and backlog offers.
2. WHEN backlog age is low, THE Broker SHALL heavily prefer sticky and live-ready sources.
3. WHEN backlog age is high, THE Broker SHALL increase the backlog share while preserving a minimum budget for fresh sync-matchable work.
4. THE Broker SHALL NOT allow backlog delivery to drive sync-match rate to zero.

### Requirement 15.3: Broker Control Loop

**User Story:** As a Tokeira developer, I want the broker to run a control loop that adjusts delivery weights, so that the system adapts to changing load patterns.

#### Acceptance Criteria

1. THE Broker SHALL periodically evaluate delivery metrics (schedule-to-start latency, sync match rate, poll success rate, backlog age) and adjust source weights.
2. THE Broker SHALL expose the current delivery weights and metrics for observability.
3. THE control loop interval SHALL be configurable.

### Requirement 15.4: Schedule-to-Start Latency Monitoring

**User Story:** As a Tokeira developer, I want the runtime to track schedule-to-start latency, so that operators can monitor task delivery health.

#### Acceptance Criteria

1. THE Runtime SHALL record the elapsed time between task scheduling and task start for both workflow tasks and activity tasks.
2. THE Runtime SHALL expose schedule-to-start latency as a metric, broken down by namespace and task queue.

### Requirement 15.5: Sync Match Rate and Poll Success Rate Tracking

**User Story:** As a Tokeira developer, I want the runtime to track sync match rate and poll success rate, so that operators can diagnose delivery efficiency.

#### Acceptance Criteria

1. THE Runtime SHALL track the ratio of tasks matched synchronously (Tier A) versus tasks that enter the live-ready or backlog tiers.
2. THE Runtime SHALL track the ratio of poll requests that return a task versus poll requests that time out empty.
3. THE Runtime SHALL expose sync match rate and poll success rate as metrics, broken down by namespace and task queue.

---

## Cross-Cutting Requirements

### Requirement CC.1: History as Authority — Runtime Compliance

**User Story:** As a Tokeira developer, I want the runtime to never hold authoritative state only in memory, so that crash recovery reduces to "load the latest durable prefix and resume."

#### Acceptance Criteria

1. THE Runtime SHALL NOT treat any in-memory structure (broker queues, waiter registrations, actor cache, sticky hints) as authoritative.
2. WHEN the runtime process crashes, THE system SHALL be able to reconstruct all dispatchable work from durable storage without data loss.
3. THE Runtime SHALL NOT require in-memory state to be correct for workflow correctness; all correctness-critical state SHALL be in committed transitions.

### Requirement CC.2: No Transport or Storage Leakage into Kernel

**User Story:** As a Tokeira developer, I want the runtime to be the boundary between I/O and the pure kernel, so that the kernel remains testable and formally modelable.

#### Acceptance Criteria

1. THE Runtime SHALL be the only crate that calls storage APIs and publishes to the broker.
2. THE Runtime SHALL NOT pass storage handles, broker references, or network connections to the Kernel.
3. THE Kernel SHALL receive only `LoadedRun` and `Command` values and return only `Transition` or `Reject` values.

### Requirement CC.3: Pollers and Waiters Are Not Durable Correctness Objects

**User Story:** As a Tokeira developer, I want pollers and waiters to be purely ephemeral, so that their loss does not affect workflow correctness.

#### Acceptance Criteria

1. THE Runtime SHALL NOT persist poller registrations or waiter state to durable storage.
2. WHEN a poller disconnects or times out, THE Runtime SHALL clean up the in-memory waiter without modifying authoritative state.
3. THE Broker SHALL NOT allocate storage connections for long-poll operations.

### Requirement CC.4: Lane Does Not Own a Run Forever

**User Story:** As a Tokeira developer, I want run actors to be disposable and reloadable, so that lane assignment is an implementation concern, not a correctness dependency.

#### Acceptance Criteria

1. THE Runtime SHALL support evicting a run actor from a lane at any time without affecting correctness.
2. WHEN a run actor is evicted, THE Runtime SHALL reload it from storage on the next command for that run.
3. THE Runtime SHALL NOT assume that a run actor's in-memory state is fresher than storage; the OCC retry loop handles stale reads.

### Requirement CC.5: Inactivity Is Not Expensive

**User Story:** As a Tokeira developer, I want dormant runs to impose near-zero runtime cost, so that high-cardinality workloads with many idle executions are efficient.

#### Acceptance Criteria

1. THE Runtime SHALL evict idle run actors aggressively from lane caches.
2. A parked (evicted) run SHALL have no in-memory actor, no dedicated DB connection, and no background scanning cost.
3. THE Runtime SHALL reload parked runs on demand when a real command or due timer arrives.

### Requirement CC.6: Task Token Safety

**User Story:** As a Tokeira developer, I want task tokens to encode enough information to reject stale completions, so that worker failure and failover are idempotent.

#### Acceptance Criteria

1. Workflow task tokens SHALL encode run_key, logical_task_seq, started_event_id, attempt, and shard_epoch.
2. Activity task tokens SHALL encode run_key, activity_id, schedule_event_id, attempt, and shard_epoch.
3. WHEN a completion arrives with a token that does not match current authoritative state, THE Runtime SHALL reject it cleanly without mutating state.

### Requirement CC.7: Idempotent Derived Effect Publication

**User Story:** As a Tokeira developer, I want derived effect publication to be idempotent, so that retries after partial failures do not cause duplicate side effects.

#### Acceptance Criteria

1. THE Broker SHALL deduplicate task publications by (run_key, logical_seq) for workflow tasks and (run_key, activity_id, attempt) for activity tasks.
2. WHEN a dispatch op is published multiple times (e.g., after OCC retry), THE Broker SHALL accept only the first publication and ignore duplicates.
3. THE Runtime SHALL NOT assume that dispatch op publication happens exactly once; the design SHALL tolerate at-least-once publication.

### Requirement CC.8: Graceful Degradation Under Storage Pressure

**User Story:** As a Tokeira developer, I want the runtime to degrade gracefully when storage is slow or unavailable, so that transient storage issues do not cascade into system-wide failures.

#### Acceptance Criteria

1. WHEN storage operations fail transiently, THE Runtime SHALL retry with bounded backoff before surfacing errors to callers.
2. THE Runtime SHALL use classified DB permits (Control, Commit, Read, Projection, Maintenance) to prioritize critical operations under connection pressure.
3. THE Runtime SHALL protect control traffic (lease renewal, fencing) above all other storage operations.
4. THE Runtime SHALL allow projections and maintenance to fall behind under pressure without affecting workflow correctness.
