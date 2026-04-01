# Requirements Document: WFT Failure and Timeout Recovery (Feature 2)

## Introduction

This document captures the requirements for Feature 2 of the Tokeira kernel implementation: WorkflowTaskFailed and WorkflowTaskTimedOut command handling. Feature 2 depends on Feature 1 (kernel-foundation-wft-lifecycle), which is complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 2.1 and 2.2).

Feature 2 adds two new top-level kernel commands that handle WFT recovery. Both commands share a common pattern — they require an open run with a started pending WFT, emit a history event, clear `started_event_id` on the pending WFT (reverting it to scheduled-but-not-started), and re-dispatch the WFT for retry. The key difference is that WorkflowTaskTimedOut additionally clears StickyAffinity because the worker is presumed dead.

WFT failure and timeout are NOT terminal for the workflow. The server retries the WFT, giving the worker another chance to replay and produce valid commands.

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel.
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`.
- **TransitionBuilder**: Internal helper that assembles a Transition by emitting events with contiguous IDs and incrementing transition_seq exactly once on `finish()`.
- **PendingWorkflowTask**: The authoritative record that a WFT exists for the run, tracking logical_seq, scheduled/started event IDs, and attempt count.
- **StickyAffinity**: Worker preference recorded on run state when a worker provides a sticky_ttl during WorkflowTaskStarted.
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.
- **WorkflowTaskFailedRequest**: The request struct for the WorkflowTaskFailed command, carrying fencing fields (logical_seq, started_event_id), failure cause, failure details, and worker identity.
- **WorkflowTaskTimedOutRequest**: The request struct for the WorkflowTaskTimedOut command, carrying fencing fields (logical_seq, started_event_id) and timeout type.
- **WorkflowTaskFailedCause**: A domain enum representing the structured reason a WFT failed (e.g., NonDeterminismError, BadScheduleActivityAttributes, UnhandledCommand).
- **WorkflowTaskTimeoutType**: A domain enum representing the timeout classification (e.g., StartToClose).
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow from the committed transition.

## Requirements

---

## New Types and Command Variants

### Requirement 1.1: WorkflowTaskFailed Command Variant

**User Story:** As a Tokeira developer, I want a WorkflowTaskFailed variant in the Command enum, so that the runtime can report WFT failures to the kernel.

#### Acceptance Criteria

1. THE Command enum SHALL include a `WorkflowTaskFailed(WorkflowTaskFailedRequest)` variant.
2. THE WorkflowTaskFailedRequest struct SHALL include a `logical_seq` field of type `LogicalTaskSeq` for fencing against stale failure reports.
3. THE WorkflowTaskFailedRequest struct SHALL include a `started_event_id` field of type `i64` for fencing against stale failure reports.
4. THE WorkflowTaskFailedRequest struct SHALL include a `failure_cause` field of type `WorkflowTaskFailedCause` describing the structured failure reason.
5. THE WorkflowTaskFailedRequest struct SHALL include a `failure_details` field of type `Option<Payload>` carrying optional structured failure information.
6. THE WorkflowTaskFailedRequest struct SHALL include a `worker_identity` field of type `WorkerIdentity` identifying the worker that failed the task.
7. THE WorkflowTaskFailedRequest struct SHALL include a `now` field of type `OffsetDateTime` for the event timestamp.

### Requirement 1.1a: WorkflowTaskFailedCause Enum

**User Story:** As a Tokeira developer, I want a structured enum for WFT failure causes, so that failure reasons are type-safe and not stringly-typed.

#### Acceptance Criteria

1. THE `WorkflowTaskFailedCause` enum SHALL include at least the following variants: `NonDeterminismError`, `BadScheduleActivityAttributes`, `BadStartTimerAttributes`, `UnhandledCommand`, `BadRequestCancelActivityAttributes`, `WorkflowWorkerUnhandledFailure`, `BadSignalWorkflowExecutionAttributes`.
2. THE `WorkflowTaskFailedCause` enum SHALL derive `Clone, Debug, PartialEq`.
3. THE `WorkflowTaskFailedCause` enum SHALL be defined in the `tokeira-kernel` crate (it is kernel-specific, not a shared type).

### Requirement 1.2: WorkflowTaskTimedOut Command Variant

**User Story:** As a Tokeira developer, I want a WorkflowTaskTimedOut variant in the Command enum, so that the runtime can report WFT timeouts to the kernel.

#### Acceptance Criteria

1. THE Command enum SHALL include a `WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest)` variant.
2. THE WorkflowTaskTimedOutRequest struct SHALL include a `logical_seq` field of type `LogicalTaskSeq` for fencing against stale timeout reports.
3. THE WorkflowTaskTimedOutRequest struct SHALL include a `started_event_id` field of type `i64` for fencing against stale timeout reports.
4. THE WorkflowTaskTimedOutRequest struct SHALL include a `timeout_type` field of type `WorkflowTaskTimeoutType` carrying the structured timeout classification.
5. THE WorkflowTaskTimedOutRequest struct SHALL include a `now` field of type `OffsetDateTime` for the event timestamp.

### Requirement 1.2a: WorkflowTaskTimeoutType Enum

**User Story:** As a Tokeira developer, I want a structured enum for WFT timeout types, so that timeout classifications are type-safe.

#### Acceptance Criteria

1. THE `WorkflowTaskTimeoutType` enum SHALL include at least the variant `StartToClose`.
2. THE `WorkflowTaskTimeoutType` enum SHALL derive `Clone, Debug, PartialEq`.
3. THE `WorkflowTaskTimeoutType` enum SHALL be defined in the `tokeira-kernel` crate.

### Requirement 1.3: WorkflowTaskFailed History Event Variant

**User Story:** As a Tokeira developer, I want a WorkflowTaskFailed variant in HistoryEventKind, so that WFT failures are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `WorkflowTaskFailed` variant.
2. THE WorkflowTaskFailed event variant SHALL include a `logical_seq` field of type `LogicalTaskSeq`.
3. THE WorkflowTaskFailed event variant SHALL include a `scheduled_event_id` field of type `i64`.
4. THE WorkflowTaskFailed event variant SHALL include a `started_event_id` field of type `i64`.
5. THE WorkflowTaskFailed event variant SHALL include a `failure_cause` field of type `WorkflowTaskFailedCause`.
6. THE WorkflowTaskFailed event variant SHALL include a `failure_details` field of type `Option<Payload>`.
7. THE WorkflowTaskFailed event variant SHALL include an `identity` field of type `WorkerIdentity`.

### Requirement 1.4: WorkflowTaskTimedOut History Event Variant

**User Story:** As a Tokeira developer, I want a WorkflowTaskTimedOut variant in HistoryEventKind, so that WFT timeouts are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `WorkflowTaskTimedOut` variant.
2. THE WorkflowTaskTimedOut event variant SHALL include a `logical_seq` field of type `LogicalTaskSeq`.
3. THE WorkflowTaskTimedOut event variant SHALL include a `scheduled_event_id` field of type `i64`.
4. THE WorkflowTaskTimedOut event variant SHALL include a `started_event_id` field of type `i64`.
5. THE WorkflowTaskTimedOut event variant SHALL include a `timeout_type` field of type `WorkflowTaskTimeoutType`.

---

## WorkflowTaskFailed Command Behavior

### Requirement 2.1: WorkflowTaskFailed Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow task failures, so that non-determinism errors and invalid commands can be recovered from via retry.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is received for an open run with a started pending WFT whose logical_seq and started_event_id match the request, THE Kernel SHALL emit a WorkflowTaskFailed event carrying the pending WFT's logical_seq, scheduled_event_id, started_event_id, and the request's failure_cause, failure_details, and worker_identity.
2. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL clear started_event_id on the pending WFT by setting it to None.
3. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL preserve the pending WFT's logical_seq and scheduled_event_id unchanged.
4. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL push a DispatchOp::EnqueueWorkflowTask to re-dispatch the WFT for retry.
5. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL NOT clear StickyAffinity on the run state.
6. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL NOT close the run; WFT failure is not terminal.
7. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL NOT emit a RequestDedupeOp; WorkflowTaskFailed is internal runtime machinery.
8. WHEN a WorkflowTaskFailed command is received, THE Kernel SHALL emit exactly one history event (WorkflowTaskFailed).

### Requirement 2.2: WorkflowTaskFailed Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject invalid WorkflowTaskFailed commands, so that stale or impossible failure reports are caught.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is received with no pending WFT, THE Kernel SHALL reject with NoPendingWorkflowTask.
2. WHEN a WorkflowTaskFailed command is received and the pending WFT has no started_event_id, THE Kernel SHALL reject with WorkflowTaskNotStarted.
3. WHEN a WorkflowTaskFailed command is received for a missing run (LoadedRun::Absent), THE Kernel SHALL reject with MissingRun.
4. WHEN a WorkflowTaskFailed command is received for a closed run, THE Kernel SHALL reject with RunClosed.
5. WHEN a WorkflowTaskFailed command is received with a logical_seq that does not match the pending WFT's logical_seq, THE Kernel SHALL reject with WorkflowTaskSeqMismatch.
6. WHEN a WorkflowTaskFailed command is received with a started_event_id that does not match the pending WFT's started_event_id, THE Kernel SHALL reject with WorkflowTaskTokenMismatch.

---

## WorkflowTaskTimedOut Command Behavior

### Requirement 3.1: WorkflowTaskTimedOut Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow task timeouts, so that unresponsive workers do not block workflow progress.

#### Acceptance Criteria

1. WHEN a WorkflowTaskTimedOut command is received for an open run with a started pending WFT whose logical_seq and started_event_id match the request, THE Kernel SHALL emit a WorkflowTaskTimedOut event carrying the pending WFT's logical_seq, scheduled_event_id, started_event_id, and the request's timeout_type.
2. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL clear started_event_id on the pending WFT by setting it to None.
3. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL preserve the pending WFT's logical_seq and scheduled_event_id unchanged.
4. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL clear StickyAffinity on the run state by setting sticky to None.
5. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL push a DispatchOp::EnqueueWorkflowTask to re-dispatch the WFT for retry with sticky_preferred set to None (because StickyAffinity was cleared).
6. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL NOT close the run; WFT timeout is not terminal.
7. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL NOT emit a RequestDedupeOp; WorkflowTaskTimedOut is internal runtime machinery.
8. WHEN a WorkflowTaskTimedOut command is received, THE Kernel SHALL emit exactly one history event (WorkflowTaskTimedOut).

### Requirement 3.2: WorkflowTaskTimedOut Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject invalid WorkflowTaskTimedOut commands, so that stale or impossible timeout reports are caught.

#### Acceptance Criteria

1. WHEN a WorkflowTaskTimedOut command is received with no pending WFT, THE Kernel SHALL reject with NoPendingWorkflowTask.
2. WHEN a WorkflowTaskTimedOut command is received and the pending WFT has no started_event_id, THE Kernel SHALL reject with WorkflowTaskNotStarted.
3. WHEN a WorkflowTaskTimedOut command is received for a missing run (LoadedRun::Absent), THE Kernel SHALL reject with MissingRun.
4. WHEN a WorkflowTaskTimedOut command is received for a closed run, THE Kernel SHALL reject with RunClosed.
5. WHEN a WorkflowTaskTimedOut command is received with a logical_seq that does not match the pending WFT's logical_seq, THE Kernel SHALL reject with WorkflowTaskSeqMismatch.
6. WHEN a WorkflowTaskTimedOut command is received with a started_event_id that does not match the pending WFT's started_event_id, THE Kernel SHALL reject with WorkflowTaskTokenMismatch.

---

## Shared Behavioral Invariants

### Requirement 4.1: Pending WFT Preservation on Failure/Timeout

**User Story:** As a Tokeira developer, I want both WorkflowTaskFailed and WorkflowTaskTimedOut to preserve the pending WFT identity, so that the retry uses the same logical task sequence.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed or WorkflowTaskTimedOut command is processed, THE Kernel SHALL preserve the pending WFT's logical_seq in next_state.
2. WHEN a WorkflowTaskFailed or WorkflowTaskTimedOut command is processed, THE Kernel SHALL preserve the pending WFT's scheduled_event_id in next_state.
3. WHEN a WorkflowTaskFailed or WorkflowTaskTimedOut command is processed, THE Kernel SHALL set the pending WFT's started_event_id to None in next_state.
4. WHEN a WorkflowTaskFailed or WorkflowTaskTimedOut command is processed, THE next_state SHALL contain exactly one PendingWorkflowTask (the at-most-one-WFT invariant holds).

### Requirement 4.2: Sticky Affinity Difference Between Failure and Timeout

**User Story:** As a Tokeira developer, I want WorkflowTaskFailed to preserve sticky affinity while WorkflowTaskTimedOut clears it, so that the retry routing reflects worker availability.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is processed and the run state has StickyAffinity, THE Kernel SHALL preserve StickyAffinity in next_state unchanged.
2. WHEN a WorkflowTaskFailed command is processed, THE DispatchOp::EnqueueWorkflowTask SHALL carry sticky_preferred from the current StickyAffinity (if present).
3. WHEN a WorkflowTaskTimedOut command is processed, THE Kernel SHALL set sticky to None in next_state regardless of prior StickyAffinity.
4. WHEN a WorkflowTaskTimedOut command is processed, THE DispatchOp::EnqueueWorkflowTask SHALL carry sticky_preferred as None.

### Requirement 4.3: Re-dispatch on Failure/Timeout

**User Story:** As a Tokeira developer, I want both commands to re-dispatch the WFT, so that the workflow is never stuck after a WFT failure or timeout.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is processed, THE Transition SHALL contain exactly one DispatchOp::EnqueueWorkflowTask.
2. WHEN a WorkflowTaskTimedOut command is processed, THE Transition SHALL contain exactly one DispatchOp::EnqueueWorkflowTask.
3. WHEN a WorkflowTaskFailed or WorkflowTaskTimedOut command is processed, THE DispatchOp::EnqueueWorkflowTask SHALL carry the pending WFT's logical_seq.
4. WHEN a WorkflowTaskFailed or WorkflowTaskTimedOut command is processed, THE DispatchOp::EnqueueWorkflowTask SHALL carry a QueueKey with the run's task_queue and namespace_id.

---

## Structural Invariants (Extending Feature 1 Properties)

### Requirement 5.1: Event ID Contiguity for WFT Failure/Timeout

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for WorkflowTaskFailed and WorkflowTaskTimedOut transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskFailed transitions, THE single emitted event SHALL have event_id equal to the input state's last_event_id + 1.
2. FOR ALL WorkflowTaskTimedOut transitions, THE single emitted event SHALL have event_id equal to the input state's last_event_id + 1.
3. FOR ALL WorkflowTaskFailed and WorkflowTaskTimedOut transitions, THE next_state.last_event_id SHALL equal the emitted event's event_id.

### Requirement 5.2: Transition Sequence Increment for WFT Failure/Timeout

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for WorkflowTaskFailed and WorkflowTaskTimedOut, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskFailed transitions, THE expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.
2. FOR ALL WorkflowTaskTimedOut transitions, THE expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 5.3: At-Most-One-WFT Invariant Preservation

**User Story:** As a Tokeira developer, I want the at-most-one-WFT invariant to hold after WorkflowTaskFailed and WorkflowTaskTimedOut, so that wakeup amplification is prevented.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskFailed transitions, THE next_state SHALL contain exactly one PendingWorkflowTask.
2. FOR ALL WorkflowTaskTimedOut transitions, THE next_state SHALL contain exactly one PendingWorkflowTask.
3. FOR ALL WorkflowTaskFailed and WorkflowTaskTimedOut transitions, THE dispatch_ops SHALL contain exactly one EnqueueWorkflowTask (not zero, not two).

### Requirement 5.4: No Side Effects Beyond Event and Dispatch

**User Story:** As a Tokeira developer, I want WorkflowTaskFailed and WorkflowTaskTimedOut to produce minimal side effects, so that the transition is clean and predictable.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskFailed transitions, THE request_dedupe_ops SHALL be empty.
2. FOR ALL WorkflowTaskTimedOut transitions, THE request_dedupe_ops SHALL be empty.
3. FOR ALL WorkflowTaskFailed transitions, THE activity_ops SHALL be empty.
4. FOR ALL WorkflowTaskTimedOut transitions, THE activity_ops SHALL be empty.
5. FOR ALL WorkflowTaskFailed transitions, THE timer_ops SHALL be empty.
6. FOR ALL WorkflowTaskTimedOut transitions, THE timer_ops SHALL be empty.
7. FOR ALL WorkflowTaskFailed transitions, THE projection_ops SHALL be empty.
8. FOR ALL WorkflowTaskTimedOut transitions, THE projection_ops SHALL be empty.

---

## BasicKernel Integration

### Requirement 6.1: BasicKernel Apply Routing

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route WorkflowTaskFailed and WorkflowTaskTimedOut commands to dedicated handler methods, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_workflow_task_failed` method.
2. WHEN a WorkflowTaskTimedOut command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_workflow_task_timed_out` method.
3. THE `apply_workflow_task_failed` method SHALL follow the same pattern as existing apply methods: call `expect_open`, validate preconditions, construct a TransitionBuilder, emit events, mutate state, and call `finish`.
4. THE `apply_workflow_task_timed_out` method SHALL follow the same pattern as existing apply methods.

---

## Property Tests

### Requirement 7.1: WFT Failure Preserves Pending WFT Identity Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowTaskFailed preserves the pending WFT's logical_seq and scheduled_event_id while clearing started_event_id, so that retry identity is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a started pending WFT and FOR ALL valid WorkflowTaskFailedRequest values, WHEN WorkflowTaskFailed is applied, THE next_state.pending_workflow_task SHALL have the same logical_seq and scheduled_event_id as the input, and started_event_id SHALL be None.

### Requirement 7.2: WFT Timeout Clears Sticky Affinity Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowTaskTimedOut clears sticky affinity, so that the timeout recovery routing invariant is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a started pending WFT and optional StickyAffinity, and FOR ALL valid WorkflowTaskTimedOutRequest values, WHEN WorkflowTaskTimedOut is applied, THE next_state.sticky SHALL be None.

### Requirement 7.3: WFT Failure Preserves Sticky Affinity Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowTaskFailed preserves sticky affinity, so that the failure recovery routing invariant is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with a started pending WFT and StickyAffinity set, and FOR ALL valid WorkflowTaskFailedRequest values, WHEN WorkflowTaskFailed is applied, THE next_state.sticky SHALL equal the input state's sticky.

### Requirement 7.4: Both Commands Re-dispatch via EnqueueWorkflowTask Property

**User Story:** As a Tokeira developer, I want a property test verifying that both WorkflowTaskFailed and WorkflowTaskTimedOut always produce exactly one EnqueueWorkflowTask dispatch op, so that the workflow is never stuck.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskFailed transitions, THE dispatch_ops SHALL contain exactly one DispatchOp::EnqueueWorkflowTask.
2. FOR ALL valid WorkflowTaskTimedOut transitions, THE dispatch_ops SHALL contain exactly one DispatchOp::EnqueueWorkflowTask.

### Requirement 7.5: Both Commands Emit Exactly One History Event Property

**User Story:** As a Tokeira developer, I want a property test verifying that both commands emit exactly one history event, so that history growth is predictable.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskFailed transitions, THE history_events SHALL contain exactly one event of kind WorkflowTaskFailed.
2. FOR ALL valid WorkflowTaskTimedOut transitions, THE history_events SHALL contain exactly one event of kind WorkflowTaskTimedOut.

### Requirement 7.6: Structural Invariants Hold for New Commands Property

**User Story:** As a Tokeira developer, I want the existing structural invariant properties (event ID contiguity, transition_seq increment, at-most-one-WFT, last_event_id consistency) to cover WorkflowTaskFailed and WorkflowTaskTimedOut, so that the universal invariants are not violated by the new commands.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskFailed and WorkflowTaskTimedOut transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL valid WorkflowTaskFailed and WorkflowTaskTimedOut transitions, next_state.transition_seq SHALL equal expected_seq + 1.
3. FOR ALL valid WorkflowTaskFailed and WorkflowTaskTimedOut transitions, next_state SHALL contain at most one PendingWorkflowTask.
4. FOR ALL valid WorkflowTaskFailed and WorkflowTaskTimedOut transitions, next_state.last_event_id SHALL equal the last emitted event's event_id.

---

## Golden Transition Tests

### Requirement 8.1: WorkflowTaskFailed Success Path Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskFailed on a started WFT, so that the exact transition output is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is applied to an open run with a started pending WFT, THE test SHALL assert the exact Transition including: one WorkflowTaskFailed history event with correct logical_seq, scheduled_event_id, started_event_id, failure_cause, failure_details, identity, and reset metadata fields; next_state with pending_workflow_task having the same logical_seq and scheduled_event_id but started_event_id None; next_state.sticky unchanged; one DispatchOp::EnqueueWorkflowTask with the correct queue and logical_seq and sticky_preferred from current StickyAffinity; empty request_dedupe_ops, activity_ops, timer_ops, and projection_ops.

### Requirement 8.2: WorkflowTaskTimedOut Success Path Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskTimedOut on a started WFT, so that the exact transition output is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskTimedOut command is applied to an open run with a started pending WFT and StickyAffinity set, THE test SHALL assert the exact Transition including: one WorkflowTaskTimedOut history event with correct logical_seq, scheduled_event_id, started_event_id, and timeout_type; next_state with pending_workflow_task having the same logical_seq and scheduled_event_id but started_event_id None; next_state.sticky as None; one DispatchOp::EnqueueWorkflowTask with sticky_preferred None; empty request_dedupe_ops, activity_ops, timer_ops, and projection_ops.

### Requirement 8.3: WorkflowTaskFailed with No Sticky Affinity Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskFailed when no sticky affinity is set, so that the non-sticky path is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is applied to an open run with a started pending WFT and no StickyAffinity, THE test SHALL assert that next_state.sticky remains None and the DispatchOp::EnqueueWorkflowTask has sticky_preferred None.

### Requirement 8.4: WorkflowTaskTimedOut with No Sticky Affinity Golden Test

**User Story:** As a Tokeira developer, I want a golden test for WorkflowTaskTimedOut when no sticky affinity is set, so that the no-sticky timeout path is pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskTimedOut command is applied to an open run with a started pending WFT and no StickyAffinity, THE test SHALL assert that next_state.sticky is None and the DispatchOp::EnqueueWorkflowTask has sticky_preferred None.

### Requirement 8.5: WorkflowTaskFailed Rejection Path Golden Tests

**User Story:** As a Tokeira developer, I want golden tests for all WorkflowTaskFailed rejection paths, so that error conditions are pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskFailed command is applied to LoadedRun::Absent, THE test SHALL assert Reject::MissingRun.
2. WHEN a WorkflowTaskFailed command is applied to a closed run, THE test SHALL assert Reject::RunClosed.
3. WHEN a WorkflowTaskFailed command is applied to an open run with no pending WFT, THE test SHALL assert Reject::NoPendingWorkflowTask.
4. WHEN a WorkflowTaskFailed command is applied to an open run with a pending WFT that has no started_event_id, THE test SHALL assert Reject::WorkflowTaskNotStarted.
5. WHEN a WorkflowTaskFailed command is applied with a mismatched logical_seq, THE test SHALL assert Reject::WorkflowTaskSeqMismatch.
6. WHEN a WorkflowTaskFailed command is applied with a mismatched started_event_id, THE test SHALL assert Reject::WorkflowTaskTokenMismatch.

### Requirement 8.6: WorkflowTaskTimedOut Rejection Path Golden Tests

**User Story:** As a Tokeira developer, I want golden tests for all WorkflowTaskTimedOut rejection paths, so that error conditions are pinned.

#### Acceptance Criteria

1. WHEN a WorkflowTaskTimedOut command is applied to LoadedRun::Absent, THE test SHALL assert Reject::MissingRun.
2. WHEN a WorkflowTaskTimedOut command is applied to a closed run, THE test SHALL assert Reject::RunClosed.
3. WHEN a WorkflowTaskTimedOut command is applied to an open run with no pending WFT, THE test SHALL assert Reject::NoPendingWorkflowTask.
4. WHEN a WorkflowTaskTimedOut command is applied to an open run with a pending WFT that has no started_event_id, THE test SHALL assert Reject::WorkflowTaskNotStarted.
5. WHEN a WorkflowTaskTimedOut command is applied with a mismatched logical_seq, THE test SHALL assert Reject::WorkflowTaskSeqMismatch.
6. WHEN a WorkflowTaskTimedOut command is applied with a mismatched started_event_id, THE test SHALL assert Reject::WorkflowTaskTokenMismatch.
