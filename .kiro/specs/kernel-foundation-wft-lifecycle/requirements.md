# Requirements Document: Kernel Foundation + WFT Lifecycle (Feature 1)

## Introduction

This document captures the requirements for Feature 1 of the Tokeira kernel implementation: the foundation layer and workflow task (WFT) lifecycle. Feature 1 has no dependencies and is the base upon which all subsequent kernel features (2–10) are built.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements document is [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md).

Feature 1 is **partially implemented**. The existing code in `tokeira-kernel` already provides the `Kernel` trait, `BasicKernel`, `TransitionBuilder`, all six top-level commands (Start, Signal, WorkflowTaskStarted, WorkflowTaskCompleted, ActivityResolved, TimerDue), and seven workflow commands (ScheduleActivity, StartTimer, CompleteWorkflow, FailWorkflow, UpsertMemo, UpsertSearchAttributes, RequestNewWorkflowTask). Request dedup, sticky affinity, and the 13-variant Reject enum are also in place.

This requirements document focuses on three areas:

1. **Gaps** in the existing implementation that must be filled to match the architecture spec.
2. **Property tests** that verify structural invariants across all transitions.
3. **Golden transition tests** that pin exact behavior for each command path.

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel.
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`.
- **TransitionBuilder**: Internal helper that assembles a Transition by emitting events with contiguous IDs and incrementing transition_seq exactly once on `finish()`.
- **PendingWorkflowTask**: The authoritative record that a WFT exists for the run.
- **StickyAffinity**: Worker preference recorded on run state when a worker provides a sticky_ttl.
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.
- **ActivityResolution**: The terminal outcome of an activity: Completed, Failed, TimedOut, or Canceled.
- **Golden_Transition_Test**: A test that constructs explicit run state + command and asserts the exact Transition output.
- **Property_Test**: A test that generates arbitrary valid inputs and checks that structural invariants hold across all transitions.

## Requirements

---

## Gap 1: Timeout Configuration on WorkflowState and Start

### Requirement 1.1: WorkflowState Timeout Fields

**User Story:** As a Tokeira developer, I want WorkflowState to carry timeout configuration, so that the runtime can enforce workflow-level timeouts using values recorded at start time.

#### Acceptance Criteria

1. THE WorkflowState SHALL include a `workflow_execution_timeout` field of type `Option<Duration>`.
2. THE WorkflowState SHALL include a `workflow_run_timeout` field of type `Option<Duration>`.
3. THE WorkflowState SHALL include a `workflow_task_timeout` field of type `Duration`.
4. WHEN a WorkflowState is initialized by the Start command, THE Kernel SHALL populate timeout fields from the StartRequest.

### Requirement 1.2: StartRequest Timeout Fields

**User Story:** As a Tokeira developer, I want the StartRequest to carry timeout configuration, so that the Kernel can record it on WorkflowState.

#### Acceptance Criteria

1. THE StartRequest SHALL include a `workflow_execution_timeout` field of type `Option<Duration>`.
2. THE StartRequest SHALL include a `workflow_run_timeout` field of type `Option<Duration>`.
3. THE StartRequest SHALL include a `workflow_task_timeout` field of type `Duration`.
4. WHEN a Start command is processed, THE Kernel SHALL copy timeout values from StartRequest to the initialized WorkflowState.
5. THE Kernel SHALL NOT enforce timeout expiry; timeout enforcement is a runtime concern.

---

## Gap 2: Retry Policy and Attempt on WorkflowState and Start

### Requirement 2.1: WorkflowState Retry Fields

**User Story:** As a Tokeira developer, I want WorkflowState to carry retry policy and attempt count, so that the runtime can make retry decisions on failure or timeout.

#### Acceptance Criteria

1. THE WorkflowState SHALL include a `retry_policy` field of type `Option<RetryPolicy>`.
2. THE WorkflowState SHALL include an `attempt` field of type `u32`.
3. WHEN a WorkflowState is initialized by the Start command, THE Kernel SHALL populate retry_policy and attempt from the StartRequest.

### Requirement 2.2: StartRequest Retry Fields

**User Story:** As a Tokeira developer, I want the StartRequest to carry retry policy and attempt count, so that the Kernel can record them on WorkflowState.

#### Acceptance Criteria

1. THE StartRequest SHALL include a `retry_policy` field of type `Option<RetryPolicy>`.
2. THE StartRequest SHALL include an `attempt` field of type `u32`.
3. WHEN a Start command is processed, THE Kernel SHALL copy retry_policy and attempt from StartRequest to the initialized WorkflowState.
4. THE Kernel SHALL NOT evaluate retry policy logic; retry decisions are a runtime concern.

---

## Gap 3: WorkflowExecutionStarted Event Completeness

### Requirement 3.1: WorkflowExecutionStarted Event Fields

**User Story:** As a Tokeira developer, I want the WorkflowExecutionStarted event to carry all Temporal-compatible fields, so that history replay and chain metadata are complete from the first event.

#### Acceptance Criteria

1. THE WorkflowExecutionStarted event variant SHALL include a `continued_execution_run_id` field of type `Option<RunId>`.
2. THE WorkflowExecutionStarted event variant SHALL include a `first_execution_run_id` field of type `Option<RunId>`.
3. THE WorkflowExecutionStarted event variant SHALL include a `retry_policy` field of type `Option<RetryPolicy>`.
4. THE WorkflowExecutionStarted event variant SHALL include an `attempt` field of type `u32`.
5. THE WorkflowExecutionStarted event variant SHALL include a `workflow_execution_timeout` field of type `Option<Duration>`.
6. THE WorkflowExecutionStarted event variant SHALL include a `workflow_run_timeout` field of type `Option<Duration>`.
7. THE WorkflowExecutionStarted event variant SHALL include a `workflow_task_timeout` field of type `Duration`.
8. WHEN a Start command is processed, THE Kernel SHALL populate all WorkflowExecutionStarted event fields from the StartRequest.

### Requirement 3.2: StartRequest Chain Metadata Fields

**User Story:** As a Tokeira developer, I want the StartRequest to carry chain metadata, so that ContinueAsNew and retry linkage is recorded in the first event.

#### Acceptance Criteria

1. THE StartRequest SHALL include a `continued_execution_run_id` field of type `Option<RunId>`.
2. THE StartRequest SHALL include a `first_execution_run_id` field of type `Option<RunId>`.
3. WHEN a Start command is processed, THE Kernel SHALL pass continued_execution_run_id and first_execution_run_id through to the WorkflowExecutionStarted event.

---

## Gap 4: Activity Resolution — TimedOut and Canceled

### Requirement 4.1: ActivityResolution Variants

**User Story:** As a Tokeira developer, I want ActivityResolution to support TimedOut and Canceled variants, so that the full activity lifecycle is representable.

#### Acceptance Criteria

1. THE ActivityResolution enum SHALL include a `TimedOut` variant carrying a timeout_type string.
2. THE ActivityResolution enum SHALL include a `Canceled` variant carrying optional details.
3. WHEN an ActivityResolved command is received with a TimedOut resolution, THE Kernel SHALL emit an ActivityTaskTimedOut event carrying the activity_id and timeout_type.
4. WHEN an ActivityResolved command is received with a Canceled resolution, THE Kernel SHALL emit an ActivityTaskCanceled event carrying the activity_id and optional details.
5. WHEN an ActivityResolved command is received with a TimedOut or Canceled resolution, THE Kernel SHALL remove the activity from the activities map and push an ActivityOp::Delete.
6. WHEN an ActivityResolved command is received with a TimedOut or Canceled resolution and no WFT is pending, THE Kernel SHALL schedule a workflow task.

### Requirement 4.2: ActivityTaskTimedOut and ActivityTaskCanceled Event Variants

**User Story:** As a Tokeira developer, I want HistoryEventKind to include ActivityTaskTimedOut and ActivityTaskCanceled variants, so that all activity terminal states are recorded in history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include an `ActivityTaskTimedOut` variant carrying activity_id and timeout_type.
2. THE HistoryEventKind enum SHALL include an `ActivityTaskCanceled` variant carrying activity_id and optional details.

---

## Gap 5: ScheduleActivity Timeout Fields

### Requirement 5.1: ScheduleActivity Timeout Pass-Through

**User Story:** As a Tokeira developer, I want ScheduleActivity to carry activity-level timeout fields, so that the runtime and dispatch layer can enforce activity timeouts.

#### Acceptance Criteria

1. THE ScheduleActivity workflow command variant SHALL include a `schedule_to_close_timeout` field of type `Option<Duration>`.
2. THE ScheduleActivity workflow command variant SHALL include a `schedule_to_start_timeout` field of type `Option<Duration>`.
3. THE ScheduleActivity workflow command variant SHALL include a `start_to_close_timeout` field of type `Option<Duration>`.
4. THE ScheduleActivity workflow command variant SHALL include a `heartbeat_timeout` field of type `Option<Duration>`.
5. WHEN a ScheduleActivity workflow command is processed, THE Kernel SHALL pass timeout fields through to the ActivityTaskScheduled event.
6. WHEN a ScheduleActivity workflow command is processed, THE Kernel SHALL pass timeout fields through to the DispatchOp::EnqueueActivityTask.

### Requirement 5.2: ActivityTaskScheduled Event Timeout Fields

**User Story:** As a Tokeira developer, I want the ActivityTaskScheduled event to carry timeout fields, so that history contains the authoritative timeout configuration for each activity.

#### Acceptance Criteria

1. THE ActivityTaskScheduled event variant SHALL include `schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`, and `heartbeat_timeout` fields of type `Option<Duration>`.
2. WHEN a ScheduleActivity workflow command is processed, THE Kernel SHALL populate the ActivityTaskScheduled event timeout fields from the workflow command.

### Requirement 5.3: ActivityState Timeout Tracking

**User Story:** As a Tokeira developer, I want ActivityState to track timeout configuration, so that the runtime can reference activity timeouts from the kernel's state.

#### Acceptance Criteria

1. THE ActivityState SHALL include `schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`, and `heartbeat_timeout` fields of type `Option<Duration>`.
2. WHEN a ScheduleActivity workflow command creates an ActivityState entry, THE Kernel SHALL populate timeout fields from the workflow command.

### Requirement 5.4: EnqueueActivityTask Timeout Fields

**User Story:** As a Tokeira developer, I want the EnqueueActivityTask dispatch op to carry timeout fields, so that the delivery layer can enforce schedule-to-start timeout.

#### Acceptance Criteria

1. THE DispatchOp::EnqueueActivityTask variant SHALL include `schedule_to_close_timeout`, `schedule_to_start_timeout`, `start_to_close_timeout`, and `heartbeat_timeout` fields of type `Option<Duration>`.
2. WHEN a ScheduleActivity workflow command is processed, THE Kernel SHALL populate the dispatch op timeout fields from the workflow command.

---

## Property Tests: Structural Invariants

### Requirement 6.1: Event ID Contiguity Property

**User Story:** As a Tokeira developer, I want a property test that verifies event IDs are contiguous within every transition, so that history integrity is guaranteed regardless of command sequence.

#### Acceptance Criteria

1. FOR ALL valid (LoadedRun, Command) pairs that produce a Transition, THE history_events in the Transition SHALL have event IDs forming a contiguous sequence starting from `expected_seq`'s last_event_id + 1.
2. THE property test SHALL generate arbitrary valid LoadedRun states and Commands using a property-based testing framework.

### Requirement 6.2: Transition Sequence Increment Property

**User Story:** As a Tokeira developer, I want a property test that verifies transition_seq increments exactly once per transition, so that the optimistic concurrency fence is always correct.

#### Acceptance Criteria

1. FOR ALL valid (LoadedRun, Command) pairs that produce a Transition, THE Transition's `next_state.transition_seq` SHALL equal `expected_seq + 1`.
2. FOR ALL valid (LoadedRun, Command) pairs that produce a Transition, THE Transition's `expected_seq` SHALL equal the input WorkflowState's `transition_seq`.

### Requirement 6.3: At-Most-One-WFT Property

**User Story:** As a Tokeira developer, I want a property test that verifies at most one WFT is pending after any transition, so that the wakeup amplification invariant is never violated.

#### Acceptance Criteria

1. FOR ALL valid (LoadedRun, Command) pairs that produce a Transition, THE Transition's `next_state` SHALL contain at most one PendingWorkflowTask.

### Requirement 6.4: Closed Workflow No-Schedule Property

**User Story:** As a Tokeira developer, I want a property test that verifies closed workflows never schedule new activities or WFTs, so that terminal state absorption is enforced.

#### Acceptance Criteria

1. FOR ALL Transitions where `next_state.status` is not Running, THE Transition's dispatch_ops SHALL NOT contain EnqueueWorkflowTask or EnqueueActivityTask ops.
2. FOR ALL Transitions where `next_state.status` is not Running, THE Transition's `next_state.pending_workflow_task` SHALL be None.
3. FOR ALL Transitions where `next_state.status` is not Running, THE Transition's `next_state.closed_at` SHALL be Some.

### Requirement 6.5: Last Event ID Consistency Property

**User Story:** As a Tokeira developer, I want a property test that verifies next_state.last_event_id equals the last emitted event's ID, so that event ID tracking is always consistent.

#### Acceptance Criteria

1. FOR ALL valid (LoadedRun, Command) pairs that produce a Transition with at least one history event, THE Transition's `next_state.last_event_id` SHALL equal the last element of `history_events`' event_id.
2. FOR ALL valid (LoadedRun, Command) pairs that produce a Transition with no history events, THE Transition's `next_state.last_event_id` SHALL equal the input state's last_event_id.

### Requirement 6.6: ActivityOp Consistency Property

**User Story:** As a Tokeira developer, I want a property test that verifies ActivityOp entries are consistent with next_state.activities, so that the activity tracking contract is never violated.

#### Acceptance Criteria

1. FOR ALL Transitions, every ActivityOp::Upsert entry SHALL have a corresponding entry in `next_state.activities` with matching activity_id.
2. FOR ALL Transitions, every ActivityOp::Delete entry SHALL have no corresponding entry in `next_state.activities`.

### Requirement 6.7: TimerOp Consistency Property

**User Story:** As a Tokeira developer, I want a property test that verifies TimerOp entries are consistent with next_state.timers, so that the timer tracking contract is never violated.

#### Acceptance Criteria

1. FOR ALL Transitions, every TimerOp::Upsert entry SHALL have a corresponding entry in `next_state.timers` with matching timer_id.
2. FOR ALL Transitions, every TimerOp::Delete entry SHALL have no corresponding entry in `next_state.timers`.

---

## Golden Transition Tests

### Requirement 7.1: Start from Absent Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Start from Absent, so that the exact transition output is pinned and regressions are caught.

#### Acceptance Criteria

1. WHEN a Start command is applied to LoadedRun::Absent, THE test SHALL assert the exact Transition including: next_state with Running status, transition_seq 1, last_event_id 2, populated identity and timeout fields, one PendingWorkflowTask; history_events containing WorkflowExecutionStarted and WorkflowTaskScheduled; one RequestDedupeOp; one ProjectionOp::UpsertExecution; one DispatchOp::EnqueueWorkflowTask.
2. THE test SHALL verify that next_state.workflow_execution_timeout, workflow_run_timeout, workflow_task_timeout, retry_policy, and attempt match the StartRequest values.

### Requirement 7.2: Signal with No Pending WFT Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Signal when no WFT is pending, so that signal-triggered WFT scheduling is pinned.

#### Acceptance Criteria

1. WHEN a Signal command is applied to an open run with no pending WFT, THE test SHALL assert the Transition contains: a WorkflowExecutionSignaled event, a WorkflowTaskScheduled event, a RequestDedupeOp, a DispatchOp::EnqueueWorkflowTask, and next_state with a PendingWorkflowTask.

### Requirement 7.3: Signal with Pending WFT Golden Test

**User Story:** As a Tokeira developer, I want a golden test for Signal when a WFT is already pending, so that the at-most-one-WFT invariant is pinned.

#### Acceptance Criteria

1. WHEN a Signal command is applied to an open run with a pending WFT, THE test SHALL assert the Transition contains: a WorkflowExecutionSignaled event, a RequestDedupeOp, NO WorkflowTaskScheduled event, NO DispatchOp::EnqueueWorkflowTask, and next_state with the same PendingWorkflowTask logical_seq as before.

### Requirement 7.4: WorkflowTaskStarted Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskStarted, so that WFT start recording and sticky affinity are pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskStarted command is applied with a matching logical_seq and a sticky_ttl, THE test SHALL assert the Transition contains: a WorkflowTaskStarted event with incremented attempt, next_state with started_event_id set on the pending WFT, and StickyAffinity recorded with the correct worker identity and expiry.

### Requirement 7.5: WorkflowTaskCompleted with Activities and Timers Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskCompleted that schedules activities and timers, so that the full workflow command processing path is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command is applied with ScheduleActivity and StartTimer workflow commands, THE test SHALL assert the Transition contains: a WorkflowTaskCompleted event, ActivityTaskScheduled and TimerStarted events, ActivityOp::Upsert and TimerOp::Upsert entries, DispatchOp::EnqueueActivityTask, and next_state with the activity and timer in their respective maps and no pending WFT.

### Requirement 7.6: WorkflowTaskCompleted with CompleteWorkflow Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskCompleted with CompleteWorkflow, so that the terminal close path is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command is applied with a CompleteWorkflow workflow command, THE test SHALL assert the Transition contains: a WorkflowTaskCompleted event, a WorkflowExecutionCompleted event, a ProjectionOp::CloseExecution with Completed status, and next_state with Completed status, closed_at set, no pending WFT, and no sticky affinity.

### Requirement 7.7: WorkflowTaskCompleted with FailWorkflow Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskCompleted with FailWorkflow, so that the failure close path is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskCompleted command is applied with a FailWorkflow workflow command, THE test SHALL assert the Transition contains: a WorkflowTaskCompleted event, a WorkflowExecutionFailed event, a ProjectionOp::CloseExecution with Failed status, and next_state with Failed status, closed_at set, no pending WFT, and no sticky affinity.

### Requirement 7.8: ActivityResolved Schedules WFT Golden Test

**User Story:** As a Tokeira developer, I want a golden test for ActivityResolved, so that activity resolution and WFT scheduling are pinned.

#### Acceptance Criteria

1. WHEN an ActivityResolved command with Completed resolution is applied to an open run with no pending WFT, THE test SHALL assert the Transition contains: an ActivityTaskCompleted event, an ActivityOp::Delete, a WorkflowTaskScheduled event, a DispatchOp::EnqueueWorkflowTask, and next_state with the activity removed and a new PendingWorkflowTask.

### Requirement 7.9: TimerDue Schedules WFT Golden Test

**User Story:** As a Tokeira developer, I want a golden test for TimerDue, so that timer firing and WFT scheduling are pinned.

#### Acceptance Criteria

1. WHEN a TimerDue command is applied to an open run with no pending WFT, THE test SHALL assert the Transition contains: a TimerFired event, a TimerOp::Delete, a WorkflowTaskScheduled event, a DispatchOp::EnqueueWorkflowTask, and next_state with the timer removed and a new PendingWorkflowTask.

### Requirement 7.10: Rejection Path Golden Tests

**User Story:** As a Tokeira developer, I want golden tests for every rejection path in Feature 1, so that all error conditions are pinned.

#### Acceptance Criteria

1. WHEN a Start command is applied to LoadedRun::Existing, THE test SHALL assert Reject::RunAlreadyExists.
2. WHEN a Signal command is applied to LoadedRun::Absent, THE test SHALL assert Reject::MissingRun.
3. WHEN a Signal command is applied to a closed run, THE test SHALL assert Reject::RunClosed.
4. WHEN a WorkflowTaskStarted command is applied with no pending WFT, THE test SHALL assert Reject::NoPendingWorkflowTask.
5. WHEN a WorkflowTaskStarted command is applied with a mismatched logical_seq, THE test SHALL assert Reject::WorkflowTaskSeqMismatch.
6. WHEN a WorkflowTaskStarted command is applied and the pending WFT already has a started_event_id, THE test SHALL assert Reject::WorkflowTaskAlreadyStarted.
7. WHEN a WorkflowTaskCompleted command is applied with no pending WFT, THE test SHALL assert Reject::NoPendingWorkflowTask.
8. WHEN a WorkflowTaskCompleted command is applied and the pending WFT has no started_event_id, THE test SHALL assert Reject::WorkflowTaskNotStarted.
9. WHEN a WorkflowTaskCompleted command is applied with a mismatched logical_seq, THE test SHALL assert Reject::WorkflowTaskSeqMismatch.
10. WHEN a WorkflowTaskCompleted command is applied with a mismatched attempt or started_event_id in the token, THE test SHALL assert Reject::WorkflowTaskTokenMismatch.
11. WHEN a ScheduleActivity workflow command references an already-open activity_id, THE test SHALL assert Reject::DuplicateActivityId.
12. WHEN a StartTimer workflow command references an already-open timer_id, THE test SHALL assert Reject::DuplicateTimerId.
13. WHEN an ActivityResolved command references an unknown activity_id, THE test SHALL assert Reject::UnknownActivity.
14. WHEN a TimerDue command references an unknown timer_id, THE test SHALL assert Reject::UnknownTimer.
15. WHEN a workflow command follows a CompleteWorkflow command in the same WFT completion, THE test SHALL assert Reject::CommandsAfterClose.

---

## Existing Implementation Verification

### Requirement 8.1: Existing Kernel Interface

**User Story:** As a Tokeira developer, I want to confirm the existing Kernel trait and BasicKernel implementation satisfy the foundation requirements, so that no regressions are introduced by gap-filling work.

#### Acceptance Criteria

1. THE Kernel trait SHALL expose an `apply` method that accepts a LoadedRun and a Command and returns `Result<Transition, Reject>`.
2. THE Kernel SHALL perform no I/O, use no ambient clock, and create no random values internally.
3. THE Kernel SHALL compute the full next WorkflowState (not a delta) in every Transition.
4. WHEN the Kernel produces a Transition, THE Transition SHALL carry an `expected_seq` equal to the WorkflowState's transition_seq at the start of processing.
5. WHEN the Kernel produces a Transition, THE Transition's `next_state.transition_seq` SHALL equal `expected_seq + 1`.

### Requirement 8.2: Existing Request Deduplication

**User Story:** As a Tokeira developer, I want to confirm request dedup is correctly emitted for external commands only, so that the idempotency contract is maintained.

#### Acceptance Criteria

1. WHEN a command carries a RequestContext (Start, Signal), THE Kernel SHALL emit a RequestDedupeOp containing the request_id.
2. WHEN a command is internal runtime machinery (WorkflowTaskStarted, WorkflowTaskCompleted, ActivityResolved, TimerDue), THE Kernel SHALL NOT emit a RequestDedupeOp.

### Requirement 8.3: Existing At-Most-One-WFT Invariant

**User Story:** As a Tokeira developer, I want to confirm the at-most-one-WFT invariant is maintained across all command paths, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. THE Kernel SHALL maintain the invariant that at most one workflow task is pending at any time for a given run.
2. WHEN a command would normally trigger a WFT and a WFT is already pending, THE Kernel SHALL NOT schedule a second workflow task.
