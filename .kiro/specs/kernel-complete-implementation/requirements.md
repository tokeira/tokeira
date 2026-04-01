# Requirements Document: Kernel Complete Implementation

## Introduction

This document captures the full requirements for the Tokeira kernel (`tokeira-kernel`), the pure deterministic state machine at the center of the durable execution engine. The kernel answers one question: given a loaded run state and one validated command, what exact transition should happen next? It performs no I/O, uses no ambient time, and produces no side effects.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md).

The implementation is organized into 10 incremental features with explicit dependency ordering. Features 1–4 form the core lifecycle. Features 5–9 add entity types and advanced capabilities. Feature 10 (Reset) is deferred pending the history loading path.

**Dependency graph:**

- Feature 1 (Foundation + WFT lifecycle) — no dependencies, partially implemented
- Feature 2 (WFT failure/timeout recovery) — depends on Feature 1
- Feature 3 (Cancel and Terminate) — depends on Feature 1
- Feature 4 (ContinueAsNew + workflow timeout) — depends on Features 1, 3
- Feature 5 (Child workflows) — depends on Features 1, 3
- Feature 6 (External signals and cancel requests) — depends on Features 1, 3
- Feature 7 (Updates) — depends on Feature 1
- Feature 8 (Markers and execution options) — depends on Feature 1
- Feature 9 (Nexus operations) — depends on Features 1, 3
- Feature 10 (Reset) — depends on Features 1, 2, 3, 4; deferred

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call. Contains next_state, history events, dispatch ops, projection ops, activity/timer ops, and request dedupe ops.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`. Only `Start` accepts `Absent`.
- **TransitionBuilder**: Internal helper that assembles a Transition by taking ownership of WorkflowState, emitting events with contiguous IDs, and incrementing transition_seq exactly once on `finish()`.
- **PendingWorkflowTask**: The authoritative record that a WFT exists for the run, tracking logical_seq, scheduled/started event IDs, and attempt count.
- **StickyAffinity**: Worker preference recorded on run state when a worker provides a sticky_ttl during WorkflowTaskStarted.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **ActivityState**: Tracking record for an open activity in WorkflowState.
- **TimerState**: Tracking record for an open timer in WorkflowState.
- **ChildWorkflowState**: Tracking record for an open child workflow in WorkflowState.
- **PendingUpdate**: Tracking record for an accepted but not yet completed update in WorkflowState.
- **PendingExternalSignal**: Tracking record for an initiated but not yet resolved external signal in WorkflowState.
- **PendingExternalCancel**: Tracking record for an initiated but not yet resolved external cancel request in WorkflowState.
- **PendingNexusOperation**: Tracking record for a scheduled but not yet resolved Nexus operation in WorkflowState.
- **ParentClosePolicy**: Policy applied to open child workflows when the parent closes: Terminate, RequestCancel, or Abandon.
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions, incremented exactly once per `apply` call.

## Requirements

---

## Feature 1: Kernel Foundation + WFT Lifecycle (partially implemented)

### Requirement 1.1: Pure Deterministic State Machine Interface

**User Story:** As a Tokeira developer, I want the Kernel to expose a pure `apply(LoadedRun, Command) -> Result<Transition, Reject>` interface, so that correctness is easy to reason about, test, and formally model.

#### Acceptance Criteria

1. THE Kernel SHALL expose an `apply` method that accepts a LoadedRun and a Command and returns either a Transition or a Reject.
2. THE Kernel SHALL perform no I/O, use no ambient clock, and create no random values internally.
3. THE Kernel SHALL compute the full next WorkflowState (not a delta) in every Transition.
4. WHEN the Kernel produces a Transition, THE Transition SHALL carry an `expected_seq` equal to the WorkflowState's transition_seq at the start of processing.
5. WHEN the Kernel produces a Transition, THE Transition's `next_state.transition_seq` SHALL equal `expected_seq + 1`.
6. WHEN the Kernel emits history events within a single Transition, THE Kernel SHALL assign contiguous Event_IDs starting from `last_event_id + 1`.
7. WHEN the Kernel emits history events, THE Transition's `next_state.last_event_id` SHALL equal the last emitted event's Event_ID.

### Requirement 1.2: Start Command

**User Story:** As a Tokeira developer, I want the Kernel to initialize a new workflow run from a Start command, so that workflow executions can be created.

#### Acceptance Criteria

1. WHEN a Start command is received with LoadedRun::Absent, THE Kernel SHALL initialize a WorkflowState with ExecutionStatus::Running, TransitionSeq::ZERO, last_event_id 0, empty entity maps, and identity fields from the request.
2. WHEN a Start command is received, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
3. WHEN a Start command is received, THE Kernel SHALL emit a WorkflowExecutionStarted event carrying workflow type, task queue, input, memo, and search attributes.
4. WHEN a Start command is received, THE Kernel SHALL emit a ProjectionOp::UpsertExecution with Running status.
5. WHEN a Start command is received, THE Kernel SHALL schedule a workflow task by emitting WorkflowTaskScheduled, setting the pending WFT, and pushing a DispatchOp::EnqueueWorkflowTask.
6. WHEN a Start command is received with LoadedRun::Existing, THE Kernel SHALL reject with RunAlreadyExists.

### Requirement 1.3: Signal Command

**User Story:** As a Tokeira developer, I want the Kernel to record signals and trigger workflow tasks, so that external events can be delivered to running workflows.

#### Acceptance Criteria

1. WHEN a Signal command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Signal command is received for an open run, THE Kernel SHALL emit a WorkflowExecutionSignaled event carrying signal name, input, request ID, and caller identity.
3. WHEN a Signal command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a Signal command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
5. WHEN a Signal command is received for a missing run, THE Kernel SHALL reject with MissingRun.
6. WHEN a Signal command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 1.4: WorkflowTaskStarted Command

**User Story:** As a Tokeira developer, I want the Kernel to record when a worker begins executing a workflow task, so that task lifecycle is tracked accurately.

#### Acceptance Criteria

1. WHEN a WorkflowTaskStarted command is received with a matching logical_seq and no started_event_id on the pending WFT, THE Kernel SHALL emit a WorkflowTaskStarted event with the logical sequence, scheduled event ID, incremented attempt count, and worker identity.
2. WHEN a WorkflowTaskStarted command is received, THE Kernel SHALL set started_event_id on the pending WFT to the emitted event's ID.
3. WHEN a WorkflowTaskStarted command is received and the worker provides a sticky_ttl, THE Kernel SHALL record StickyAffinity on the run state with the worker identity and computed expiry.
4. WHEN a WorkflowTaskStarted command is received with no pending WFT, THE Kernel SHALL reject with NoPendingWorkflowTask.
5. WHEN a WorkflowTaskStarted command is received with a mismatched logical_seq, THE Kernel SHALL reject with WorkflowTaskSeqMismatch.
6. WHEN a WorkflowTaskStarted command is received and the pending WFT already has a started_event_id, THE Kernel SHALL reject with WorkflowTaskAlreadyStarted.

### Requirement 1.5: WorkflowTaskCompleted Command

**User Story:** As a Tokeira developer, I want the Kernel to process workflow task completions and apply workflow commands, so that workflow code can express intent through the state machine.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command is received with a valid token matching the pending WFT, THE Kernel SHALL emit a WorkflowTaskCompleted event and clear the pending WFT from state.
2. WHEN a WorkflowTaskCompleted command is received, THE Kernel SHALL apply each workflow command in order.
3. WHEN a workflow command closes the run during WorkflowTaskCompleted processing, THE Kernel SHALL reject subsequent workflow commands with CommandsAfterClose.
4. WHEN force_new_workflow_task is set and the run is still open with no pending WFT after processing all commands, THE Kernel SHALL schedule a new workflow task.
5. WHEN a WorkflowTaskCompleted command is received with no pending WFT, THE Kernel SHALL reject with NoPendingWorkflowTask.
6. WHEN a WorkflowTaskCompleted command is received and the pending WFT has no started_event_id, THE Kernel SHALL reject with WorkflowTaskNotStarted.
7. WHEN a WorkflowTaskCompleted command is received with a mismatched logical_seq in the token, THE Kernel SHALL reject with WorkflowTaskSeqMismatch.
8. WHEN a WorkflowTaskCompleted command is received with a mismatched attempt or started_event_id in the token, THE Kernel SHALL reject with WorkflowTaskTokenMismatch.

### Requirement 1.6: Workflow Commands — ScheduleActivity and StartTimer

**User Story:** As a Tokeira developer, I want the Kernel to schedule activities and start timers from workflow commands, so that workflows can perform external work and delay execution.

#### Acceptance Criteria

1. WHEN a ScheduleActivity workflow command is received with a unique activity_id, THE Kernel SHALL emit an ActivityTaskScheduled event, create an ActivityState entry, push an ActivityOp::Upsert, and push a DispatchOp::EnqueueActivityTask.
2. WHEN a ScheduleActivity workflow command is received with an activity_id that is already open, THE Kernel SHALL reject with DuplicateActivityId.
3. WHEN a StartTimer workflow command is received with a unique timer_id, THE Kernel SHALL emit a TimerStarted event, create a TimerState entry, and push a TimerOp::Upsert.
4. WHEN a StartTimer workflow command is received with a timer_id that is already open, THE Kernel SHALL reject with DuplicateTimerId.

### Requirement 1.7: Workflow Commands — CompleteWorkflow, FailWorkflow, UpsertMemo, UpsertSearchAttributes, RequestNewWorkflowTask

**User Story:** As a Tokeira developer, I want the Kernel to handle terminal workflow commands and metadata mutations, so that workflows can complete, fail, and update their metadata.

#### Acceptance Criteria

1. WHEN a CompleteWorkflow workflow command is received, THE Kernel SHALL emit a WorkflowExecutionCompleted event and close the run with ExecutionStatus::Completed.
2. WHEN a FailWorkflow workflow command is received, THE Kernel SHALL emit a WorkflowExecutionFailed event and close the run with ExecutionStatus::Failed.
3. WHEN an UpsertMemo workflow command is received, THE Kernel SHALL update the memo on WorkflowState and emit a ProjectionOp::UpsertExecution.
4. WHEN an UpsertSearchAttributes workflow command is received, THE Kernel SHALL update the search attributes on WorkflowState and emit a ProjectionOp::UpsertExecution.
5. WHEN a RequestNewWorkflowTask workflow command is received and the run is open with no pending WFT, THE Kernel SHALL schedule a workflow task.
6. WHEN a RequestNewWorkflowTask workflow command is received and a WFT is already pending, THE Kernel SHALL treat the command as a no-op.

### Requirement 1.8: ActivityResolved Command

**User Story:** As a Tokeira developer, I want the Kernel to process activity resolutions, so that workflow code can observe activity results.

#### Acceptance Criteria

1. WHEN an ActivityResolved command is received for an open activity with a Completed resolution, THE Kernel SHALL emit an ActivityTaskCompleted event.
2. WHEN an ActivityResolved command is received for an open activity with a Failed resolution, THE Kernel SHALL emit an ActivityTaskFailed event.
3. WHEN an ActivityResolved command is received, THE Kernel SHALL remove the activity from the activities map and push an ActivityOp::Delete.
4. WHEN an ActivityResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
5. WHEN an ActivityResolved command is received for an unknown activity_id, THE Kernel SHALL reject with UnknownActivity.

### Requirement 1.9: TimerDue Command

**User Story:** As a Tokeira developer, I want the Kernel to process timer firings, so that workflow code can observe timer expirations.

#### Acceptance Criteria

1. WHEN a TimerDue command is received for an open timer, THE Kernel SHALL emit a TimerFired event.
2. WHEN a TimerDue command is received, THE Kernel SHALL remove the timer from the timers map and push a TimerOp::Delete.
3. WHEN a TimerDue command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a TimerDue command is received for an unknown timer_id, THE Kernel SHALL reject with UnknownTimer.

### Requirement 1.10: TransitionBuilder Mechanics

**User Story:** As a Tokeira developer, I want the TransitionBuilder to enforce structural invariants during transition assembly, so that every transition is well-formed.

#### Acceptance Criteria

1. THE TransitionBuilder SHALL take ownership of the current WorkflowState and a `now` timestamp on construction.
2. WHEN the TransitionBuilder emits an event, THE TransitionBuilder SHALL assign the next contiguous Event_ID starting from `last_event_id + 1`.
3. WHEN the TransitionBuilder's `schedule_workflow_task` is called, THE TransitionBuilder SHALL emit a WorkflowTaskScheduled event, set the pending WFT on state, and push a DispatchOp::EnqueueWorkflowTask with sticky_preferred from current StickyAffinity.
4. WHEN the TransitionBuilder's `close` is called, THE TransitionBuilder SHALL set terminal status, clear pending WFT, clear StickyAffinity, and emit a ProjectionOp::CloseExecution.
5. WHEN the TransitionBuilder's `finish` is called, THE TransitionBuilder SHALL increment transition_seq exactly once and return the assembled Transition.

### Requirement 1.11: At-Most-One-WFT Invariant

**User Story:** As a Tokeira developer, I want the Kernel to maintain at most one pending WFT at any time, so that wakeup amplification is prevented during signal floods.

#### Acceptance Criteria

1. THE Kernel SHALL maintain the invariant that at most one workflow task is pending at any time for a given run.
2. WHEN a command would normally trigger a WFT and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
3. WHEN a Transition is produced, THE Transition's next_state SHALL contain at most one PendingWorkflowTask.

### Requirement 1.12: Request Deduplication

**User Story:** As a Tokeira developer, I want the Kernel to emit request dedupe ops for external commands, so that idempotent handling is possible across retries and partial failures.

#### Acceptance Criteria

1. WHEN a command carries a RequestContext (Start, Signal), THE Kernel SHALL emit a RequestDedupeOp containing the request_id.
2. WHEN a command is internal runtime machinery (WorkflowTaskStarted, WorkflowTaskCompleted, ActivityResolved, TimerDue), THE Kernel SHALL NOT emit a RequestDedupeOp.

### Requirement 1.13: Reject Taxonomy — Foundation

**User Story:** As a Tokeira developer, I want the Kernel to produce precise, enumerated rejection reasons, so that the runtime can act on rejections programmatically.

#### Acceptance Criteria

1. THE Kernel SHALL reject with RunAlreadyExists when Start is called for an existing run.
2. THE Kernel SHALL reject with MissingRun when a command targets a non-existent run.
3. THE Kernel SHALL reject with RunClosed when a command targets a closed run.
4. THE Kernel SHALL reject with NoPendingWorkflowTask when WorkflowTaskStarted or WorkflowTaskCompleted is called with no pending WFT.
5. THE Kernel SHALL reject with WorkflowTaskSeqMismatch when the logical_seq does not match the pending WFT.
6. THE Kernel SHALL reject with WorkflowTaskAlreadyStarted when WorkflowTaskStarted is called and the pending WFT already has a started_event_id.
7. THE Kernel SHALL reject with WorkflowTaskTokenMismatch when the completion token does not match.
8. THE Kernel SHALL reject with DuplicateActivityId when ScheduleActivity references an already-open activity.
9. THE Kernel SHALL reject with DuplicateTimerId when StartTimer references an already-open timer.
10. THE Kernel SHALL reject with UnknownActivity when ActivityResolved references an unknown activity.
11. THE Kernel SHALL reject with UnknownTimer when TimerDue references an unknown timer.
12. THE Kernel SHALL reject with CommandsAfterClose when a workflow command follows a close command in the same WFT completion.

### Requirement 1.14: Timeout Configuration on Start

**User Story:** As a Tokeira developer, I want the Kernel to record timeout configuration on WorkflowState at start time, so that the runtime can enforce workflow-level timeouts.

#### Acceptance Criteria

1. WHEN a Start command includes workflow_execution_timeout, workflow_run_timeout, or workflow_task_timeout, THE Kernel SHALL record those values on WorkflowState.
2. THE Kernel SHALL NOT enforce timeout expiry itself; timeout enforcement is a runtime concern.

### Requirement 1.15: Retry Policy Recording on Start

**User Story:** As a Tokeira developer, I want the Kernel to record retry policy and attempt count on WorkflowState at start time, so that the runtime can make retry decisions on failure or timeout.

#### Acceptance Criteria

1. WHEN a Start command includes a retry_policy, THE Kernel SHALL record the retry_policy on WorkflowState.
2. WHEN a Start command includes an attempt count, THE Kernel SHALL record the attempt on WorkflowState.
3. THE Kernel SHALL NOT evaluate retry policy logic; retry decisions are a runtime concern.

---

## Feature 2: WFT Failure and Timeout Recovery

**Depends on:** Feature 1

### Requirement 2.1: WorkflowTaskFailed Command

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow task failures, so that non-determinism errors and invalid commands can be recovered from via retry.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is received for an open run with a started pending WFT, THE Kernel SHALL emit a WorkflowTaskFailed event carrying scheduled/started event IDs, failure cause, failure details, and worker identity.
2. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL clear started_event_id on the pending WFT (revert to scheduled-but-not-started).
3. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL push a DispatchOp::EnqueueWorkflowTask to re-dispatch the WFT for retry.
4. WHEN a WorkflowTaskFailed command is received with no pending WFT, THE Kernel SHALL reject with NoPendingWorkflowTask.
5. WHEN a WorkflowTaskFailed command is received and the pending WFT has no started_event_id, THE Kernel SHALL reject with WorkflowTaskNotStarted.

### Requirement 2.2: WorkflowTaskTimedOut Command

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow task timeouts, so that unresponsive workers do not block workflow progress.

#### Acceptance Criteria

1. WHEN a WorkflowTaskTimedOut command is received for an open run with a started pending WFT, THE Kernel SHALL emit a WorkflowTaskTimedOut event carrying scheduled/started event IDs and timeout type.
2. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL clear started_event_id on the pending WFT.
3. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL clear StickyAffinity on the run state.
4. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL push a DispatchOp::EnqueueWorkflowTask to re-dispatch without sticky preference.
5. WHEN a WorkflowTaskTimedOut command is received with no pending WFT, THE Kernel SHALL reject with NoPendingWorkflowTask.
6. WHEN a WorkflowTaskTimedOut command is received and the pending WFT has no started_event_id, THE Kernel SHALL reject with WorkflowTaskNotStarted.

---

## Feature 3: Cancel and Terminate

**Depends on:** Feature 1

### Requirement 3.1: Cancel Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to record cancellation requests, so that workflows can be gracefully stopped with an opportunity for cleanup.

#### Acceptance Criteria

1. WHEN a Cancel command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Cancel command is received, THE Kernel SHALL emit a WorkflowExecutionCancelRequested event carrying the reason and, if applicable, the external workflow execution that initiated the cancellation.
3. WHEN a Cancel command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a Cancel command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
5. THE Kernel SHALL NOT close the run on Cancel; cancellation is a cooperative two-phase operation.
6. WHEN a Cancel command is received for a missing run, THE Kernel SHALL reject with MissingRun.
7. WHEN a Cancel command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 3.2: Terminate Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to immediately terminate a workflow run, so that hard-stop semantics are available without consulting the worker.

#### Acceptance Criteria

1. WHEN a Terminate command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Terminate command is received, THE Kernel SHALL emit a WorkflowExecutionTerminated event carrying the reason, optional details, and caller identity.
3. WHEN a Terminate command is received, THE Kernel SHALL close the run with ExecutionStatus::Terminated (set terminal status, clear pending WFT, clear StickyAffinity).
4. WHEN a Terminate command is received, THE Kernel SHALL emit a ProjectionOp::CloseExecution with Terminated status.
5. WHEN a Terminate command is received, THE Kernel SHALL clear the activities map and emit an ActivityOp::Delete for each open activity.
6. WHEN a Terminate command is received, THE Kernel SHALL clear the timers map and emit a TimerOp::Delete for each open timer.
7. WHEN a Terminate command is received and open child workflows exist, THE Kernel SHALL apply Parent Close Policy for each open child.
8. WHEN a Terminate command is received for a missing run, THE Kernel SHALL reject with MissingRun.
9. WHEN a Terminate command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 3.3: CancelWorkflow Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to handle the CancelWorkflow workflow command, so that workflow code can confirm cancellation after cleanup.

#### Acceptance Criteria

1. WHEN a CancelWorkflow workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a WorkflowExecutionCanceled event.
2. WHEN a CancelWorkflow workflow command is received, THE Kernel SHALL close the run with ExecutionStatus::Canceled.

### Requirement 3.4: RequestCancelActivity Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to handle activity cancellation requests from workflow code, so that workflows can request cancellation of in-progress activities.

#### Acceptance Criteria

1. WHEN a RequestCancelActivity workflow command is received for an open activity, THE Kernel SHALL emit an ActivityTaskCancelRequested event.
2. WHEN a RequestCancelActivity workflow command is received, THE Kernel SHALL keep the activity in the pending activities map until it is resolved.
3. WHEN an ActivityResolved command is received with a Canceled resolution, THE Kernel SHALL emit an ActivityTaskCanceled event, remove the activity, and push ActivityOp::Delete.

### Requirement 3.5: CancelTimer Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to handle timer cancellation from workflow code, so that workflows can cancel pending timers.

#### Acceptance Criteria

1. WHEN a CancelTimer workflow command is received for an open timer, THE Kernel SHALL emit a TimerCanceled event.
2. WHEN a CancelTimer workflow command is received, THE Kernel SHALL remove the timer from the timers map and push a TimerOp::Delete.

### Requirement 3.6: Open Entity Cleanup on Terminal Close

**User Story:** As a Tokeira developer, I want the Kernel to clean up all open entities when a run reaches a terminal state via Terminate, so that no orphaned entities remain.

#### Acceptance Criteria

1. WHEN the Kernel closes a run via Terminate, THE Kernel SHALL emit ActivityOp::Delete for every open activity and TimerOp::Delete for every open timer.
2. WHEN the Kernel closes a run via Terminate, THE Kernel SHALL clear the activities and timers maps in next_state.

### Requirement 3.7: ActivityResolved with Canceled and TimedOut Resolutions

**User Story:** As a Tokeira developer, I want the Kernel to handle all activity resolution types including Canceled and TimedOut, so that the full activity lifecycle is supported.

#### Acceptance Criteria

1. WHEN an ActivityResolved command is received with a TimedOut resolution, THE Kernel SHALL emit an ActivityTaskTimedOut event.
2. WHEN an ActivityResolved command is received with a Canceled resolution, THE Kernel SHALL emit an ActivityTaskCanceled event.

---

## Feature 4: ContinueAsNew and Workflow-Level Timeout

**Depends on:** Features 1, 3

### Requirement 4.1: ContinueAsNew Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to handle ContinueAsNew, so that workflows can checkpoint state into a successor run with fresh history.

#### Acceptance Criteria

1. WHEN a ContinueAsNew workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a WorkflowExecutionContinuedAsNew event carrying the new run ID, workflow type, task queue, input, memo, and search attributes.
2. WHEN a ContinueAsNew workflow command is received, THE Kernel SHALL close the current run with ExecutionStatus::ContinuedAsNew.
3. WHEN a ContinueAsNew workflow command is received, THE Kernel SHALL emit a ProjectionOp::CloseExecution with ContinuedAsNew status.
4. THE Kernel SHALL NOT create the successor run; the runtime reads the event and issues a Start command for the successor.

### Requirement 4.2: WorkflowExecutionTimedOut Command

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow execution timeouts, so that workflows that exceed their configured timeout are terminated by the server.

#### Acceptance Criteria

1. WHEN a WorkflowExecutionTimedOut command is received for an open run, THE Kernel SHALL emit a WorkflowExecutionTimedOut event carrying the timeout type and retry state.
2. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL close the run with ExecutionStatus::TimedOut.
3. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL emit a ProjectionOp::CloseExecution with TimedOut status.
4. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL clean up open entities (activities, timers, children) using the same cleanup logic as Terminate.
5. WHEN a WorkflowExecutionTimedOut command is received and the workflow has a retry_policy, THE Kernel SHALL emit retry metadata (attempt count, retry state) for the runtime to decide whether to create a retry run.

### Requirement 4.3: Workflow-Level Retry Metadata Emission

**User Story:** As a Tokeira developer, I want the Kernel to emit retry metadata on failure and timeout, so that the runtime can make informed retry decisions.

#### Acceptance Criteria

1. WHEN a FailWorkflow command closes a run and the workflow has a retry_policy, THE Kernel SHALL emit the current attempt count and retry state in the WorkflowExecutionFailed event.
2. WHEN a WorkflowExecutionTimedOut command closes a run and the workflow has a retry_policy, THE Kernel SHALL emit the current attempt count and retry state in the WorkflowExecutionTimedOut event.
3. THE Kernel SHALL NOT evaluate retry policy logic (max attempts, non-retryable error types, backoff); retry decisions are a runtime concern.

---

## Feature 5: Child Workflows

**Depends on:** Features 1, 3

### Requirement 5.1: StartChildWorkflow Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to initiate child workflow executions from workflow commands, so that workflows can compose other workflows.

#### Acceptance Criteria

1. WHEN a StartChildWorkflow workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a StartChildWorkflowExecutionInitiated event.
2. WHEN a StartChildWorkflow workflow command is received, THE Kernel SHALL add a ChildWorkflowState entry to the open children map with child_run_id None, started_event_id None, and the specified ParentClosePolicy.
3. WHEN a StartChildWorkflow workflow command is received, THE Kernel SHALL push a DispatchOp::StartChildWorkflow so the runtime can create the child run.
4. WHEN a StartChildWorkflow workflow command is received with a child_workflow_id that is already in the open children map, THE Kernel SHALL reject with DuplicateChildWorkflowId.

### Requirement 5.2: ChildStartConfirmed Command

**User Story:** As a Tokeira developer, I want the Kernel to record child workflow start confirmations, so that the parent can track child lifecycle.

#### Acceptance Criteria

1. WHEN a ChildStartConfirmed command is received with a success variant for a known child, THE Kernel SHALL emit a ChildWorkflowExecutionStarted event and update the child entry with the started_event_id and child_run_id.
2. WHEN a ChildStartConfirmed command is received with a failure variant, THE Kernel SHALL emit a StartChildWorkflowExecutionFailed event and remove the child from the open children map.
3. WHEN a ChildStartConfirmed command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a ChildStartConfirmed command is received for an unknown child, THE Kernel SHALL reject with UnknownChild.

### Requirement 5.3: ChildResolved Command

**User Story:** As a Tokeira developer, I want the Kernel to process child workflow resolutions, so that the parent workflow can observe child completion.

#### Acceptance Criteria

1. WHEN a ChildResolved command is received for a known open child, THE Kernel SHALL emit the appropriate event (ChildWorkflowExecutionCompleted, Failed, Canceled, Terminated, or TimedOut) based on the child's terminal status.
2. WHEN a ChildResolved command is received, THE Kernel SHALL remove the child from the open children map.
3. WHEN a ChildResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN a ChildResolved command is received for an unknown child, THE Kernel SHALL reject with UnknownChild.

### Requirement 5.4: Parent Close Policy

**User Story:** As a Tokeira developer, I want the Kernel to apply Parent Close Policy when a parent workflow closes, so that child workflows are handled according to the configured policy.

#### Acceptance Criteria

1. WHEN a parent workflow closes (via Terminate, TimedOut, Completed, Failed, Canceled, or ContinuedAsNew) and open children exist with ParentClosePolicy::Terminate, THE Kernel SHALL emit dispatch ops to terminate those children.
2. WHEN a parent workflow closes and open children exist with ParentClosePolicy::RequestCancel, THE Kernel SHALL emit dispatch ops to send cancel requests to those children.
3. WHEN a parent workflow closes and open children exist with ParentClosePolicy::Abandon, THE Kernel SHALL take no action for those children.
4. WHEN Parent Close Policy is applied, THE Kernel SHALL remove the affected children from the open children map in next_state.

---

## Feature 6: External Signals and Cancel Requests

**Depends on:** Features 1, 3

### Requirement 6.1: SignalExternalWorkflowExecution Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to initiate external workflow signals from workflow commands, so that workflows can signal other workflow executions.

#### Acceptance Criteria

1. WHEN a SignalExternalWorkflowExecution workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a SignalExternalWorkflowExecutionInitiated event.
2. WHEN a SignalExternalWorkflowExecution workflow command is received, THE Kernel SHALL add a PendingExternalSignal entry to the pending external signals map tracking the initiated_event_id, target workflow ID, optional target run ID, and signal name.
3. WHEN a SignalExternalWorkflowExecution workflow command is received, THE Kernel SHALL push a DispatchOp::SignalExternalWorkflow.

### Requirement 6.2: ExternalSignalResolved Command

**User Story:** As a Tokeira developer, I want the Kernel to process external signal resolutions, so that workflow code can observe whether the signal was delivered.

#### Acceptance Criteria

1. WHEN an ExternalSignalResolved command is received with a success variant for a known pending external signal, THE Kernel SHALL emit an ExternalWorkflowExecutionSignaled event and remove the entry from the pending set.
2. WHEN an ExternalSignalResolved command is received with a failure variant, THE Kernel SHALL emit a SignalExternalWorkflowExecutionFailed event and remove the entry from the pending set.
3. WHEN an ExternalSignalResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN an ExternalSignalResolved command is received for an unknown pending signal, THE Kernel SHALL reject with UnknownExternalSignal.

### Requirement 6.3: RequestCancelExternalWorkflowExecution Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to initiate external workflow cancel requests from workflow commands, so that workflows can request cancellation of other workflow executions.

#### Acceptance Criteria

1. WHEN a RequestCancelExternalWorkflowExecution workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a RequestCancelExternalWorkflowExecutionInitiated event.
2. WHEN a RequestCancelExternalWorkflowExecution workflow command is received, THE Kernel SHALL add a PendingExternalCancel entry to the pending external cancels map.
3. WHEN a RequestCancelExternalWorkflowExecution workflow command is received, THE Kernel SHALL push a DispatchOp::RequestCancelExternalWorkflow.

### Requirement 6.4: ExternalCancelResolved Command

**User Story:** As a Tokeira developer, I want the Kernel to process external cancel resolutions, so that workflow code can observe whether the cancel request was delivered.

#### Acceptance Criteria

1. WHEN an ExternalCancelResolved command is received with a success variant for a known pending external cancel, THE Kernel SHALL emit an ExternalWorkflowExecutionCancelRequested event and remove the entry from the pending set.
2. WHEN an ExternalCancelResolved command is received with a failure variant, THE Kernel SHALL emit a RequestCancelExternalWorkflowExecutionFailed event and remove the entry from the pending set.
3. WHEN an ExternalCancelResolved command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
4. WHEN an ExternalCancelResolved command is received for an unknown pending cancel, THE Kernel SHALL reject with UnknownExternalCancel.

---

## Feature 7: Updates

**Depends on:** Feature 1

### Requirement 7.1: Update Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to accept workflow updates, so that callers can synchronously write to and read from a running workflow.

#### Acceptance Criteria

1. WHEN an Update command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN an Update command is received, THE Kernel SHALL emit a WorkflowExecutionUpdateAccepted event carrying the update ID, update name, and input.
3. WHEN an Update command is received, THE Kernel SHALL add a PendingUpdate entry to the pending updates map tracking the update_id, accepted_event_id, and name.
4. WHEN an Update command is received and no WFT is pending, THE Kernel SHALL schedule a workflow task.
5. WHEN an Update command is received and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
6. WHEN an Update command is received for a missing run, THE Kernel SHALL reject with MissingRun.
7. WHEN an Update command is received for a closed run, THE Kernel SHALL reject with RunClosed.

### Requirement 7.2: UpdateCompleted Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to record update completions from workflow code, so that update callers can receive results.

#### Acceptance Criteria

1. WHEN an UpdateCompleted workflow command is received for a known pending update, THE Kernel SHALL emit a WorkflowExecutionUpdateCompleted event.
2. WHEN an UpdateCompleted workflow command is received, THE Kernel SHALL remove the update from the pending updates map.
3. WHEN an UpdateCompleted workflow command is received for an unknown update_id, THE Kernel SHALL reject with UnknownUpdate.

### Requirement 7.3: UpdateRejected Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to record update rejections from workflow code, so that update callers can receive rejection reasons.

#### Acceptance Criteria

1. WHEN an UpdateRejected workflow command is received for a known pending update, THE Kernel SHALL emit a WorkflowExecutionUpdateRejected event.
2. WHEN an UpdateRejected workflow command is received, THE Kernel SHALL remove the update from the pending updates map.
3. WHEN an UpdateRejected workflow command is received for an unknown update_id, THE Kernel SHALL reject with UnknownUpdate.

### Requirement 7.4: ProtocolMessage Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to handle ProtocolMessage workflow commands, so that update acceptance/rejection events appear at the correct position in history relative to other events in the same WFT completion.

#### Acceptance Criteria

1. WHEN a ProtocolMessage workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL use the message ID to determine the correct position in the event sequence for the corresponding update event.
2. THE ProtocolMessage SHALL NOT emit a standalone history event; it is a sequencing directive.

---

## Feature 8: Markers and Execution Options

**Depends on:** Feature 1

### Requirement 8.1: RecordMarker Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to record opaque markers in history, so that SDKs can persist side effects, local activity results, and version markers for stable replay.

#### Acceptance Criteria

1. WHEN a RecordMarker workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a MarkerRecorded event carrying the marker name, details map, failure details, and header.
2. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT modify WorkflowState beyond appending the history event.
3. WHEN a RecordMarker workflow command is received, THE Kernel SHALL NOT emit any dispatch ops or projection ops.

### Requirement 8.2: UpdateExecutionOptions Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to handle execution option updates, so that operators can modify versioning overrides and completion callbacks on running workflows.

#### Acceptance Criteria

1. WHEN an UpdateExecutionOptions command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN an UpdateExecutionOptions command is received, THE Kernel SHALL emit a WorkflowExecutionOptionsUpdated event carrying the versioning override and/or completion callbacks.
3. WHEN an UpdateExecutionOptions command is received, THE Kernel SHALL update the relevant fields (versioning_override, completion_callbacks) on WorkflowState.
4. WHEN an UpdateExecutionOptions command is received for a missing run, THE Kernel SHALL reject with MissingRun.
5. WHEN an UpdateExecutionOptions command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## Feature 9: Nexus Operations

**Depends on:** Features 1, 3

### Requirement 9.1: ScheduleNexusOperation Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to schedule Nexus operations from workflow commands, so that workflows can invoke cross-namespace services through typed contracts.

#### Acceptance Criteria

1. WHEN a ScheduleNexusOperation workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a NexusOperationScheduled event.
2. WHEN a ScheduleNexusOperation workflow command is received, THE Kernel SHALL add a PendingNexusOperation entry to the pending Nexus operations map tracking the operation_id, scheduled_event_id, endpoint, service, and operation.
3. WHEN a ScheduleNexusOperation workflow command is received, THE Kernel SHALL push a DispatchOp::ScheduleNexusOperation.
4. WHEN a ScheduleNexusOperation workflow command is received with an operation_id that is already pending, THE Kernel SHALL reject with DuplicateNexusOperationId.

### Requirement 9.2: NexusOperationResolved Command

**User Story:** As a Tokeira developer, I want the Kernel to process Nexus operation resolutions, so that workflow code can observe Nexus operation results.

#### Acceptance Criteria

1. WHEN a NexusOperationResolved command is received with a Started variant (async operation accepted), THE Kernel SHALL emit a NexusOperationStarted event and keep the operation in the pending set.
2. WHEN a NexusOperationResolved command is received with a Completed variant, THE Kernel SHALL emit a NexusOperationCompleted event and remove the operation from the pending set.
3. WHEN a NexusOperationResolved command is received with a Failed variant, THE Kernel SHALL emit a NexusOperationFailed event and remove the operation from the pending set.
4. WHEN a NexusOperationResolved command is received with a Canceled variant, THE Kernel SHALL emit a NexusOperationCanceled event and remove the operation from the pending set.
5. WHEN a NexusOperationResolved command is received with a TimedOut variant, THE Kernel SHALL emit a NexusOperationTimedOut event and remove the operation from the pending set.
6. WHEN a NexusOperationResolved command results in a terminal resolution and no WFT is pending, THE Kernel SHALL schedule a workflow task.
7. WHEN a NexusOperationResolved command is received for an unknown operation, THE Kernel SHALL reject with UnknownNexusOperation.

### Requirement 9.3: CancelNexusOperation Workflow Command

**User Story:** As a Tokeira developer, I want the Kernel to handle Nexus operation cancellation requests from workflow commands, so that workflows can cancel pending Nexus operations.

#### Acceptance Criteria

1. WHEN a CancelNexusOperation workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a NexusOperationCancelRequested event.
2. WHEN a CancelNexusOperation workflow command is received, THE Kernel SHALL push a DispatchOp::CancelNexusOperation.
3. WHEN a CancelNexusOperation workflow command is received, THE Kernel SHALL keep the Nexus operation in the pending set until it is resolved.

---

## Feature 10: Reset (Deferred)

**Depends on:** Features 1, 2, 3, 4 — deferred pending history loading path

### Requirement 10.1: Reset Command (Top-Level)

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow reset, so that operators can discard history after a chosen event and restart from that point.

#### Acceptance Criteria

1. WHEN a Reset command is received for an open run, THE Kernel SHALL emit a RequestDedupeOp for the request ID.
2. WHEN a Reset command is received, THE Kernel SHALL emit a WorkflowTaskFailed event with a RESET_WORKFLOW cause, referencing the fork event ID.
3. WHEN a Reset command is received, THE Kernel SHALL close the current run with ExecutionStatus::Terminated.
4. WHEN a Reset command is received, THE Kernel SHALL emit metadata for the runtime to create the reset run.
5. THE Kernel SHALL NOT copy history or construct the reset run's initial state; the runtime handles that.
6. WHEN a Reset command is received with an invalid fork event ID, THE Kernel SHALL reject with ResetConstraintViolation.
7. WHEN a Reset command is received for a missing run, THE Kernel SHALL reject with MissingRun.
8. WHEN a Reset command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## Cross-Cutting Requirements

### Requirement CC.1: No Per-Execution Concurrency Limits

**User Story:** As a Tokeira developer, I want the Kernel to impose no per-execution pending-entity ceilings, so that workflows are not artificially constrained by limits inherited from a different system architecture.

#### Acceptance Criteria

1. THE Kernel SHALL NOT reject commands based on pending-entity counts (activities, children, timers, updates, Nexus operations, external signals, or external cancels).
2. THE Kernel SHALL NOT accept a limits configuration for per-execution entity counts.
3. THE Kernel SHALL schedule as many entities as the workflow code requests.

### Requirement CC.2: Queries Are Outside the Kernel Boundary

**User Story:** As a Tokeira developer, I want queries to be handled entirely by the runtime, so that the Kernel remains focused on state transitions.

#### Acceptance Criteria

1. THE Kernel SHALL NOT process query commands; queries do not mutate state, produce history events, or create transitions.
2. THE Kernel SHALL NOT expose a query-handling interface.

### Requirement CC.3: Activity Retry and Heartbeat Are Runtime Concerns

**User Story:** As a Tokeira developer, I want activity retry and heartbeat to be handled by the runtime, so that the Kernel remains focused on single-transition correctness.

#### Acceptance Criteria

1. THE Kernel SHALL NOT retry activities; retry policy enforcement is a runtime concern.
2. THE Kernel SHALL NOT process heartbeat messages; heartbeat timeout detection is a runtime concern that results in an ActivityResolved command with TimedOut resolution.

### Requirement CC.4: Terminal State Absorption

**User Story:** As a Tokeira developer, I want closed runs to reject all further commands, so that terminal states are absorbing.

#### Acceptance Criteria

1. WHEN a run has reached a terminal ExecutionStatus (Completed, Failed, Canceled, Terminated, TimedOut, ContinuedAsNew), THE Kernel SHALL reject all subsequent commands with RunClosed.
2. WHEN a run is closed within a transition, THE Kernel SHALL NOT schedule new workflow tasks, activities, or timers in the same transition after the close.

### Requirement CC.5: Reject Taxonomy — Full Set

**User Story:** As a Tokeira developer, I want the Kernel's Reject enum to cover all rejection reasons across all features, so that the runtime can handle every rejection programmatically.

#### Acceptance Criteria

1. THE Kernel SHALL define the following additional Reject variants as the corresponding features are implemented:
   - UnknownUpdate for update completion/rejection of unknown updates (Feature 7)
   - UnknownChild for child resolution of unknown children (Feature 5)
   - UnknownExternalSignal for external signal resolution of unknown signals (Feature 6)
   - UnknownExternalCancel for external cancel resolution of unknown cancels (Feature 6)
   - UnknownNexusOperation for Nexus operation resolution of unknown operations (Feature 9)
   - DuplicateChildWorkflowId for duplicate child workflow IDs (Feature 5)
   - DuplicateNexusOperationId for duplicate Nexus operation IDs (Feature 9)
   - ResetConstraintViolation for invalid reset parameters (Feature 10)
   - WorkflowTaskFailureCause for structured WFT failure causes (Feature 2)

### Requirement CC.6: WorkflowState Target Shape

**User Story:** As a Tokeira developer, I want WorkflowState to evolve incrementally toward the target shape defined in the architecture doc, so that each feature adds only the fields it needs.

#### Acceptance Criteria

1. WHEN Feature 1 is implemented, THE WorkflowState SHALL include identity fields, lifecycle fields, WFT fields, sticky affinity, memo, search attributes, activities, timers, and timeout configuration.
2. WHEN Feature 3 is implemented, THE WorkflowState SHALL NOT require new fields beyond what Feature 1 provides (CancelWorkflow uses existing close mechanics).
3. WHEN Feature 4 is implemented, THE WorkflowState SHALL include retry_policy, attempt, and cron_schedule fields.
4. WHEN Feature 5 is implemented, THE WorkflowState SHALL include a children map of ChildWorkflowState entries.
5. WHEN Feature 6 is implemented, THE WorkflowState SHALL include pending_external_signals and pending_external_cancels maps.
6. WHEN Feature 7 is implemented, THE WorkflowState SHALL include a pending_updates map.
7. WHEN Feature 8 is implemented, THE WorkflowState SHALL include versioning_override and completion_callbacks fields.
8. WHEN Feature 9 is implemented, THE WorkflowState SHALL include a pending_nexus_operations map.

### Requirement CC.7: Dispatch Ops — Full Set

**User Story:** As a Tokeira developer, I want the Kernel to emit the full set of dispatch ops as features are implemented, so that the runtime knows what task delivery actions to perform.

#### Acceptance Criteria

1. THE Kernel SHALL emit EnqueueWorkflowTask dispatch ops for WFT scheduling (Feature 1).
2. THE Kernel SHALL emit EnqueueActivityTask dispatch ops for activity scheduling (Feature 1).
3. WHEN Feature 5 is implemented, THE Kernel SHALL emit StartChildWorkflow dispatch ops.
4. WHEN Feature 5 is implemented, THE Kernel SHALL emit dispatch ops for Parent Close Policy enforcement (TerminateChild, CancelChild).
5. WHEN Feature 6 is implemented, THE Kernel SHALL emit SignalExternalWorkflow and RequestCancelExternalWorkflow dispatch ops.
6. WHEN Feature 9 is implemented, THE Kernel SHALL emit ScheduleNexusOperation and CancelNexusOperation dispatch ops.

### Requirement CC.8: Structural Invariants Across All Transitions

**User Story:** As a Tokeira developer, I want every Transition produced by the Kernel to satisfy structural invariants, so that correctness is verifiable by property tests.

#### Acceptance Criteria

1. FOR ALL Transitions produced by the Kernel, event IDs SHALL be contiguous within the transition.
2. FOR ALL Transitions produced by the Kernel, transition_seq SHALL increment exactly once.
3. FOR ALL Transitions produced by the Kernel, next_state SHALL contain at most one PendingWorkflowTask.
4. FOR ALL Transitions where the run is closed, next_state SHALL NOT contain a PendingWorkflowTask.
5. FOR ALL Transitions where the run is closed, next_state SHALL have closed_at set.
6. FOR ALL Transitions, next_state.last_event_id SHALL equal the last emitted event's Event_ID.
7. FOR ALL Transitions, every ActivityOp::Upsert SHALL have a corresponding entry in next_state.activities.
8. FOR ALL Transitions, every ActivityOp::Delete SHALL have no corresponding entry in next_state.activities.
9. FOR ALL Transitions, every TimerOp::Upsert SHALL have a corresponding entry in next_state.timers.
10. FOR ALL Transitions, every TimerOp::Delete SHALL have no corresponding entry in next_state.timers.
