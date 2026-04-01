# Requirements Document: ContinueAsNew and Workflow-Level Timeout (Feature 4)

## Introduction

This document captures the requirements for Feature 4 of the Tokeira kernel implementation: ContinueAsNew workflow command, WorkflowExecutionTimedOut top-level command, and workflow-level retry metadata emission. Feature 4 depends on Feature 1 (kernel-foundation-wft-lifecycle) and Feature 3 (kernel-cancel-terminate), both of which are complete.

The authoritative specification is [docs/architecture/020-kernel.md](../../../docs/architecture/020-kernel.md). The parent requirements are in [kernel-complete-implementation/requirements.md](../kernel-complete-implementation/requirements.md) (Requirements 4.1–4.3).

Feature 4 adds:

- **ContinueAsNew** — a workflow command within `WorkflowTaskCompleted` that closes the current run with `ExecutionStatus::ContinuedAsNew` and emits a `WorkflowExecutionContinuedAsNew` event carrying successor metadata. The kernel does NOT create the successor run; the runtime reads the event and issues a `Start` command for the successor. ContinueAsNew is a terminal workflow command (closes the run, rejects subsequent commands with `CommandsAfterClose`).

- **WorkflowExecutionTimedOut** — a top-level kernel command issued by the runtime when the workflow's execution timeout or run timeout expires. Closes the run with `ExecutionStatus::TimedOut` and cleans up open entities using the same pattern as Terminate. Carries timeout type and retry state.

- **Retry metadata emission** — when `FailWorkflow` closes a run and the workflow has a `retry_policy`, the kernel emits the current attempt count and retry state in the `WorkflowExecutionFailed` event. Same for `WorkflowExecutionTimedOut`. The kernel does NOT evaluate retry policy logic; retry decisions are a runtime concern.

Key design points from the architecture doc:
- ContinueAsNew is NOT a top-level kernel command — it is exclusively a workflow command within `WorkflowTaskCompleted`.
- Chain metadata (`continued_execution_run_id`, `first_execution_run_id`, `initiator`) is set by the runtime on the successor's `Start`, not by the kernel.
- `WorkflowExecutionTimedOut` follows the same close + cleanup pattern as Terminate (clear activities, timers, pending WFT, sticky).
- `WorkflowExecutionTimedOut` does NOT carry `RequestContext` and does NOT emit `RequestDedupeOp` — it is internal runtime machinery.

Downstream breakage: `WorkflowCommand` gains a `ContinueAsNew` variant (breaks exhaustive matches in translate.rs). `Command` gains `WorkflowExecutionTimedOut`. `ExecutionStatus` gains `ContinuedAsNew` and `TimedOut` variants (breaks exhaustive matches). The task plan must include a workspace compile checkpoint.

## Glossary

- **Kernel**: The pure deterministic state machine (`tokeira-kernel`) that processes commands against loaded run state and produces transitions. Performs no I/O.
- **Command**: A semantic mutation request delivered to the Kernel. Commands are either top-level (external or runtime-originated) or workflow commands (issued by worker code within a WorkflowTaskCompleted).
- **Transition**: The bounded, explicit description of what must be committed as a result of one `apply` call.
- **Reject**: An enumerated error indicating the command is stale, invalid, duplicated, or impossible in the current state.
- **WorkflowState**: The compact, mutation-friendly summary of a single workflow run's durable state.
- **LoadedRun**: Either `Absent` (run does not exist) or `Existing(WorkflowState)`.
- **TransitionBuilder**: Internal helper that assembles a Transition by emitting events with contiguous IDs and incrementing transition_seq exactly once on `finish()`.
- **ExecutionStatus**: Lifecycle state visible to operators and projections. Currently has variants: Running, Completed, Failed, Cancelled, Terminated. Feature 4 adds ContinuedAsNew and TimedOut.
- **ContinueAsNew**: A terminal workflow command within `WorkflowTaskCompleted` that closes the current run and emits successor metadata for the runtime.
- **WorkflowExecutionTimedOut**: A top-level kernel command issued by the runtime when a workflow's execution timeout or run timeout expires.
- **WorkflowTimeoutType**: A domain enum distinguishing between execution-level and run-level timeouts. Variants: ExecutionTimeout, RunTimeout.
- **RetryState**: A domain enum describing the retry disposition of a closed run. Variants: InProgress, NonRetryableFailure, Timeout, MaximumAttemptsReached, RetryPolicyNotSet, InternalServerError, CancelRequested.
- **WFT**: Workflow Task — the unit of work dispatched to a worker for executing workflow code.
- **Event_ID**: User-visible monotonic position in workflow history, assigned by the Kernel at emit time.
- **Transition_Seq**: Internal fence/checkpoint number for committed state transitions.
- **DispatchOp**: A value emitted by the Kernel telling the runtime what task delivery action must follow.
- **ProjectionOp**: A semantic mutation emitted by the Kernel for the read-model plane (visibility).
- **RequestDedupeOp**: A request ID persisted in the same fenced commit as history to enable idempotent external command handling.
- **ActivityOp**: An operation emitted by the Kernel for activity lifecycle management (Upsert or Delete).
- **TimerOp**: An operation emitted by the Kernel for timer lifecycle management (Upsert or Delete).

## Requirements

---

## New Types and Enum Extensions

### Requirement 1.1: ExecutionStatus New Variants

**User Story:** As a Tokeira developer, I want ExecutionStatus to include ContinuedAsNew and TimedOut variants, so that the kernel can represent these terminal states.

#### Acceptance Criteria

1. THE ExecutionStatus enum SHALL include a `ContinuedAsNew` variant.
2. THE ExecutionStatus enum SHALL include a `TimedOut` variant.
3. THE `is_open` method on ExecutionStatus SHALL return `false` for both `ContinuedAsNew` and `TimedOut`.
4. THE `ContinuedAsNew` and `TimedOut` variants SHALL derive the same traits as existing variants (Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize).

### Requirement 1.2: WorkflowTimeoutType Domain Enum

**User Story:** As a Tokeira developer, I want a WorkflowTimeoutType enum to distinguish between execution-level and run-level timeouts, so that the timeout event carries precise semantics.

#### Acceptance Criteria

1. THE WorkflowTimeoutType enum SHALL include an `ExecutionTimeout` variant.
2. THE WorkflowTimeoutType enum SHALL include a `RunTimeout` variant.
3. THE WorkflowTimeoutType enum SHALL derive Clone, Debug, PartialEq.
4. THE WorkflowTimeoutType enum SHALL be defined in the kernel command module.

### Requirement 1.3: RetryState Domain Enum

**User Story:** As a Tokeira developer, I want a RetryState enum to describe the retry disposition of a closed run, so that the runtime can make informed retry decisions.

#### Acceptance Criteria

1. THE RetryState enum SHALL include variants: InProgress, NonRetryableFailure, Timeout, MaximumAttemptsReached, RetryPolicyNotSet, InternalServerError, CancelRequested.
2. THE RetryState enum SHALL derive Clone, Debug, PartialEq.
3. THE RetryState enum SHALL be defined in the kernel command module.

### Requirement 1.4: ContinueAsNew Workflow Command Variant

**User Story:** As a Tokeira developer, I want a ContinueAsNew variant in the WorkflowCommand enum, so that workflow code can checkpoint state into a successor run.

#### Acceptance Criteria

1. THE WorkflowCommand enum SHALL include a `ContinueAsNew` variant with the following fields:
   - `new_run_id` of type `RunId` (provided by the runtime in the command payload)
   - `workflow_type` of type `WorkflowType` (may differ from the current run)
   - `task_queue` of type `TaskQueueName` (may differ from the current run)
   - `input` of type `Payloads`
   - `memo` of type `Memo` (to carry over)
   - `search_attributes` of type `SearchAttributes` (to carry over)
   - `workflow_execution_timeout` of type `Option<Duration>`
   - `workflow_run_timeout` of type `Option<Duration>`
   - `workflow_task_timeout` of type `Duration`

### Requirement 1.5: WorkflowExecutionTimedOut Command Variant

**User Story:** As a Tokeira developer, I want a WorkflowExecutionTimedOut variant in the Command enum, so that the runtime can notify the kernel when a workflow timeout expires.

#### Acceptance Criteria

1. THE Command enum SHALL include a `WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest)` variant.
2. THE WorkflowExecutionTimedOutRequest struct SHALL include a `timeout_type` field of type `WorkflowTimeoutType`.
3. THE WorkflowExecutionTimedOutRequest struct SHALL include a `retry_state` field of type `RetryState`.
4. THE WorkflowExecutionTimedOutRequest struct SHALL include a `now` field of type `OffsetDateTime`.
5. THE WorkflowExecutionTimedOutRequest struct SHALL derive Clone, Debug, PartialEq.
6. THE WorkflowExecutionTimedOutRequest struct SHALL NOT include a `request` field of type `RequestContext` (this is internal runtime machinery, not an external API command).

### Requirement 1.6: New History Event Variants

**User Story:** As a Tokeira developer, I want new HistoryEventKind variants for ContinueAsNew and workflow timeout events, so that these lifecycle events are recorded in workflow history.

#### Acceptance Criteria

1. THE HistoryEventKind enum SHALL include a `WorkflowExecutionContinuedAsNew` variant with fields:
   - `new_run_id` of type `RunId`
   - `workflow_type` of type `WorkflowType`
   - `task_queue` of type `TaskQueueName`
   - `input` of type `Payloads`
   - `memo` of type `Memo`
   - `search_attributes` of type `SearchAttributes`
   - `workflow_execution_timeout` of type `Option<Duration>`
   - `workflow_run_timeout` of type `Option<Duration>`
   - `workflow_task_timeout` of type `Duration`
2. THE HistoryEventKind enum SHALL include a `WorkflowExecutionTimedOut` variant with fields:
   - `timeout_type` of type `WorkflowTimeoutType`
   - `retry_state` of type `RetryState`

### Requirement 1.7: Retry Metadata in WorkflowExecutionFailed Event

**User Story:** As a Tokeira developer, I want the WorkflowExecutionFailed event to carry retry metadata, so that the runtime can make retry decisions on workflow failure.

#### Acceptance Criteria

1. THE `WorkflowExecutionFailed` HistoryEventKind variant SHALL include a `retry_state` field of type `RetryState`.
2. THE `WorkflowExecutionFailed` HistoryEventKind variant SHALL include an `attempt` field of type `u32`.

---

## ContinueAsNew Workflow Command Behavior

### Requirement 2.1: ContinueAsNew Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle ContinueAsNew, so that workflows can checkpoint state into a successor run with fresh history.

#### Acceptance Criteria

1. WHEN a ContinueAsNew workflow command is received within WorkflowTaskCompleted, THE Kernel SHALL emit a WorkflowExecutionContinuedAsNew event carrying the new_run_id, workflow_type, task_queue, input, memo, search_attributes, workflow_execution_timeout, workflow_run_timeout, and workflow_task_timeout from the command.
2. WHEN a ContinueAsNew workflow command is received, THE Kernel SHALL close the current run with ExecutionStatus::ContinuedAsNew by calling the TransitionBuilder's `close` method (set terminal status, clear pending WFT, clear StickyAffinity, emit ProjectionOp::CloseExecution).
3. WHEN a ContinueAsNew workflow command is received, THE apply_workflow_command function SHALL return `true` (indicating the run is closed), so that subsequent workflow commands in the same WFT completion are rejected with CommandsAfterClose.
4. THE Kernel SHALL NOT create the successor run; the runtime reads the WorkflowExecutionContinuedAsNew event and issues a Start command for the successor.
5. THE Kernel SHALL NOT emit any DispatchOp for ContinueAsNew (no WFT is scheduled for the current run).
6. THE Kernel SHALL NOT emit any RequestDedupeOp for ContinueAsNew (it is a workflow command, not an external API command).

---

## WorkflowExecutionTimedOut Command Behavior

### Requirement 3.1: WorkflowExecutionTimedOut Happy Path

**User Story:** As a Tokeira developer, I want the Kernel to handle workflow execution timeouts, so that workflows that exceed their configured timeout are terminated by the server.

#### Acceptance Criteria

1. WHEN a WorkflowExecutionTimedOut command is received for an open run, THE Kernel SHALL emit a WorkflowExecutionTimedOut event carrying the timeout_type and retry_state from the request.
2. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL close the run with ExecutionStatus::TimedOut by calling the TransitionBuilder's `close` method.
3. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL emit a ProjectionOp::CloseExecution with TimedOut status.
4. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL NOT schedule a workflow task; the worker is not consulted.
5. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL NOT emit any RequestDedupeOp (this is internal runtime machinery).

### Requirement 3.2: WorkflowExecutionTimedOut Entity Cleanup

**User Story:** As a Tokeira developer, I want WorkflowExecutionTimedOut to clean up all open entities, so that no orphaned activities or timers remain after a timeout.

#### Acceptance Criteria

1. WHEN a WorkflowExecutionTimedOut command is received and open activities exist, THE Kernel SHALL emit an ActivityOp::Delete for each open activity.
2. WHEN a WorkflowExecutionTimedOut command is received and open timers exist, THE Kernel SHALL emit a TimerOp::Delete for each open timer.
3. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL clear the activities map in next_state (next_state.activities SHALL be empty).
4. WHEN a WorkflowExecutionTimedOut command is received, THE Kernel SHALL clear the timers map in next_state (next_state.timers SHALL be empty).
5. WHEN a WorkflowExecutionTimedOut command is received with no open activities or timers, THE Kernel SHALL emit no ActivityOp or TimerOp.

### Requirement 3.3: WorkflowExecutionTimedOut Rejection Paths

**User Story:** As a Tokeira developer, I want the Kernel to reject invalid WorkflowExecutionTimedOut commands, so that timeouts against non-existent or already-closed runs are caught.

#### Acceptance Criteria

1. WHEN a WorkflowExecutionTimedOut command is received for a missing run (LoadedRun::Absent), THE Kernel SHALL reject with MissingRun.
2. WHEN a WorkflowExecutionTimedOut command is received for a closed run, THE Kernel SHALL reject with RunClosed.

---

## Retry Metadata Emission

### Requirement 4.1: FailWorkflow Retry Metadata

**User Story:** As a Tokeira developer, I want the Kernel to emit retry metadata when FailWorkflow closes a run with a retry policy, so that the runtime can make informed retry decisions.

#### Acceptance Criteria

1. WHEN a FailWorkflow workflow command is received and the workflow has a retry_policy, THE Kernel SHALL emit the current attempt count in the WorkflowExecutionFailed event's `attempt` field.
2. WHEN a FailWorkflow workflow command is received and the workflow has a retry_policy, THE Kernel SHALL emit RetryState::InProgress in the WorkflowExecutionFailed event's `retry_state` field.
3. WHEN a FailWorkflow workflow command is received and the workflow has no retry_policy, THE Kernel SHALL emit RetryState::RetryPolicyNotSet in the WorkflowExecutionFailed event's `retry_state` field.
4. WHEN a FailWorkflow workflow command is received and the workflow has no retry_policy, THE Kernel SHALL emit the current attempt count in the WorkflowExecutionFailed event's `attempt` field.
5. THE Kernel SHALL NOT evaluate retry policy logic (max attempts, non-retryable error types, backoff); retry decisions are a runtime concern.

### Requirement 4.2: WorkflowExecutionTimedOut Retry Metadata

**User Story:** As a Tokeira developer, I want the WorkflowExecutionTimedOut event to carry retry metadata, so that the runtime can decide whether to retry after a timeout.

#### Acceptance Criteria

1. THE WorkflowExecutionTimedOut event SHALL carry the retry_state provided by the runtime in the command request.
2. THE Kernel SHALL NOT compute or override the retry_state; the runtime provides the correct value based on its retry policy evaluation.

---

## BasicKernel Integration

### Requirement 5.1: BasicKernel Apply Routing for WorkflowExecutionTimedOut

**User Story:** As a Tokeira developer, I want BasicKernel::apply to route WorkflowExecutionTimedOut commands to a dedicated handler method, so that the command dispatch is consistent with existing patterns.

#### Acceptance Criteria

1. WHEN a WorkflowExecutionTimedOut command is received, THE BasicKernel::apply match arm SHALL delegate to an `apply_workflow_execution_timed_out` method.
2. THE `apply_workflow_execution_timed_out` method SHALL follow the same pattern as `apply_terminate`: call `expect_open`, construct a TransitionBuilder, emit event, call `close`, clean up entities, and call `finish`.

### Requirement 5.2: Workflow Command Dispatch for ContinueAsNew

**User Story:** As a Tokeira developer, I want the apply_workflow_command function to handle ContinueAsNew, so that this workflow command is processed during WorkflowTaskCompleted.

#### Acceptance Criteria

1. THE `apply_workflow_command` function SHALL include a match arm for `WorkflowCommand::ContinueAsNew` that emits WorkflowExecutionContinuedAsNew and calls `close(ExecutionStatus::ContinuedAsNew)`.
2. THE `apply_workflow_command` function's ContinueAsNew match arm SHALL return `Ok(true)` to indicate the run is closed.

---

## Downstream Breakage and Compile Checkpoint

### Requirement 6.1: Exhaustive Match Updates

**User Story:** As a Tokeira developer, I want all known exhaustive matches on modified enums to be updated, so that the workspace compiles after the type changes.

#### Acceptance Criteria

1. WHEN `WorkflowCommand` gains the `ContinueAsNew` variant, THE exhaustive match in `apply_workflow_command` in kernel.rs SHALL include a match arm for `ContinueAsNew`.
2. WHEN `Command` gains the `WorkflowExecutionTimedOut` variant, THE exhaustive match in `BasicKernel::apply` in kernel.rs SHALL include a match arm for `WorkflowExecutionTimedOut`.
3. WHEN `ExecutionStatus` gains `ContinuedAsNew` and `TimedOut` variants, known exhaustive matches in tokeira-edge (translate.rs, grpc_properties.rs) SHALL be updated to handle the new variants.
4. WHEN `HistoryEventKind` gains `WorkflowExecutionContinuedAsNew` and `WorkflowExecutionTimedOut` variants, known exhaustive matches across the workspace SHALL be updated.
5. WHEN `WorkflowExecutionFailed` gains `retry_state` and `attempt` fields, ALL construction sites for this variant SHALL be updated to provide the new fields.
6. AFTER all known call-site updates are applied, `cargo check --workspace` SHALL pass. Any additional compile failures discovered by the workspace check SHALL also be fixed before the feature is considered complete.

---

## Structural Invariants

### Requirement 7.1: Event ID Contiguity for ContinueAsNew and WorkflowExecutionTimedOut

**User Story:** As a Tokeira developer, I want event ID contiguity to hold for ContinueAsNew and WorkflowExecutionTimedOut transitions, so that history integrity is maintained.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, event IDs SHALL be contiguous starting from last_event_id + 1.
2. FOR ALL WorkflowExecutionTimedOut transitions, event IDs SHALL be contiguous starting from last_event_id + 1.
3. FOR ALL ContinueAsNew and WorkflowExecutionTimedOut transitions, next_state.last_event_id SHALL equal the last emitted event's event_id.

### Requirement 7.2: Transition Sequence Increment

**User Story:** As a Tokeira developer, I want transition_seq to increment exactly once for these transitions, so that the optimistic concurrency fence is correct.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.
2. FOR ALL WorkflowExecutionTimedOut transitions, expected_seq SHALL equal the input state's transition_seq, and next_state.transition_seq SHALL equal expected_seq + 1.

### Requirement 7.3: ContinueAsNew Terminal State Invariants

**User Story:** As a Tokeira developer, I want ContinueAsNew to satisfy all terminal state invariants, so that the closed run is well-formed.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, next_state.status SHALL be ExecutionStatus::ContinuedAsNew.
2. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, next_state.pending_workflow_task SHALL be None.
3. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, next_state.sticky SHALL be None.
4. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, next_state.closed_at SHALL be Some.

### Requirement 7.4: WorkflowExecutionTimedOut Terminal State Invariants

**User Story:** As a Tokeira developer, I want WorkflowExecutionTimedOut to satisfy all terminal state invariants, so that the timed-out run is well-formed.

#### Acceptance Criteria

1. FOR ALL WorkflowExecutionTimedOut transitions, next_state.status SHALL be ExecutionStatus::TimedOut.
2. FOR ALL WorkflowExecutionTimedOut transitions, next_state.pending_workflow_task SHALL be None.
3. FOR ALL WorkflowExecutionTimedOut transitions, next_state.sticky SHALL be None.
4. FOR ALL WorkflowExecutionTimedOut transitions, next_state.closed_at SHALL be Some.
5. FOR ALL WorkflowExecutionTimedOut transitions, next_state.activities SHALL be empty.
6. FOR ALL WorkflowExecutionTimedOut transitions, next_state.timers SHALL be empty.
7. FOR ALL WorkflowExecutionTimedOut transitions, dispatch_ops SHALL be empty (no WFT is scheduled).

### Requirement 7.5: Entity Cleanup Consistency for WorkflowExecutionTimedOut

**User Story:** As a Tokeira developer, I want the number of cleanup ops emitted by WorkflowExecutionTimedOut to match the number of open entities, so that cleanup is complete and not over-counted.

#### Acceptance Criteria

1. FOR ALL WorkflowExecutionTimedOut transitions, THE number of ActivityOp::Delete ops SHALL equal the number of entries in the input state's activities map.
2. FOR ALL WorkflowExecutionTimedOut transitions, THE number of TimerOp::Delete ops SHALL equal the number of entries in the input state's timers map.
3. FOR ALL WorkflowExecutionTimedOut transitions, every ActivityOp::Delete SHALL reference an activity_id that existed in the input state's activities map.
4. FOR ALL WorkflowExecutionTimedOut transitions, every TimerOp::Delete SHALL reference a timer_id that existed in the input state's timers map.

### Requirement 7.6: WorkflowExecutionTimedOut Emits No Request Dedupe

**User Story:** As a Tokeira developer, I want WorkflowExecutionTimedOut to emit no RequestDedupeOp, so that the internal-command-no-dedupe contract is maintained.

#### Acceptance Criteria

1. FOR ALL WorkflowExecutionTimedOut transitions, request_dedupe_ops SHALL be empty.

### Requirement 7.7: ContinueAsNew Field Pass-Through

**User Story:** As a Tokeira developer, I want the WorkflowExecutionContinuedAsNew event to faithfully carry all fields from the ContinueAsNew workflow command, so that the runtime has complete successor metadata.

#### Acceptance Criteria

1. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's new_run_id SHALL equal the command's new_run_id.
2. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's workflow_type SHALL equal the command's workflow_type.
3. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's task_queue SHALL equal the command's task_queue.
4. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's input SHALL equal the command's input.
5. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's memo SHALL equal the command's memo.
6. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's search_attributes SHALL equal the command's search_attributes.
7. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's workflow_execution_timeout SHALL equal the command's workflow_execution_timeout.
8. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's workflow_run_timeout SHALL equal the command's workflow_run_timeout.
9. FOR ALL WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE WorkflowExecutionContinuedAsNew event's workflow_task_timeout SHALL equal the command's workflow_task_timeout.

### Requirement 7.8: FailWorkflow Retry Metadata Consistency

**User Story:** As a Tokeira developer, I want the retry metadata in WorkflowExecutionFailed to be consistent with the workflow's retry policy presence, so that the runtime receives correct signals.

#### Acceptance Criteria

1. FOR ALL FailWorkflow transitions where the workflow has a retry_policy, THE WorkflowExecutionFailed event's retry_state SHALL be RetryState::InProgress.
2. FOR ALL FailWorkflow transitions where the workflow has no retry_policy, THE WorkflowExecutionFailed event's retry_state SHALL be RetryState::RetryPolicyNotSet.
3. FOR ALL FailWorkflow transitions, THE WorkflowExecutionFailed event's attempt SHALL equal the workflow's current attempt count from WorkflowState.

---

## Property Tests

### Requirement 8.1: ContinueAsNew Closes the Run Property

**User Story:** As a Tokeira developer, I want a property test verifying that ContinueAsNew always closes the run with ContinuedAsNew status, so that the terminal command contract is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a ContinueAsNew command as the last (or only) workflow command, THE next_state.status SHALL be ExecutionStatus::ContinuedAsNew and next_state.closed_at SHALL be Some.

### Requirement 8.2: ContinueAsNew Field Pass-Through Property

**User Story:** As a Tokeira developer, I want a property test verifying that all ContinueAsNew fields are faithfully passed through to the WorkflowExecutionContinuedAsNew event, so that no data is lost or corrupted.

#### Acceptance Criteria

1. FOR ALL valid ContinueAsNew workflow commands with arbitrary field values, WHEN applied within a WorkflowTaskCompleted, THE emitted WorkflowExecutionContinuedAsNew event SHALL carry identical values for new_run_id, workflow_type, task_queue, input, memo, search_attributes, workflow_execution_timeout, workflow_run_timeout, and workflow_task_timeout.

### Requirement 8.3: ContinueAsNew Emits No Dispatch Ops Property

**User Story:** As a Tokeira developer, I want a property test verifying that ContinueAsNew never schedules a WFT for the current run, so that the closed run does not receive further work.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a ContinueAsNew command, THE dispatch_ops SHALL be empty.

### Requirement 8.4: WorkflowExecutionTimedOut Closes the Run Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowExecutionTimedOut always closes the run with TimedOut status, so that the timeout contract is guaranteed.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState and FOR ALL valid WorkflowExecutionTimedOutRequest values, WHEN WorkflowExecutionTimedOut is applied, THE next_state.status SHALL be ExecutionStatus::TimedOut and next_state.closed_at SHALL be Some.

### Requirement 8.5: WorkflowExecutionTimedOut Cleans Up All Open Entities Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowExecutionTimedOut cleans up all open activities and timers, so that no orphaned entities remain.

#### Acceptance Criteria

1. FOR ALL valid open WorkflowState with N open activities and M open timers, WHEN WorkflowExecutionTimedOut is applied, THE activity_ops SHALL contain exactly N ActivityOp::Delete ops and THE timer_ops SHALL contain exactly M TimerOp::Delete ops, and next_state.activities and next_state.timers SHALL both be empty.

### Requirement 8.6: WorkflowExecutionTimedOut Emits No Dispatch Ops Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowExecutionTimedOut never schedules a WFT, so that the worker is never consulted after a timeout.

#### Acceptance Criteria

1. FOR ALL valid WorkflowExecutionTimedOut transitions, THE dispatch_ops SHALL be empty.

### Requirement 8.7: WorkflowExecutionTimedOut Emits No Request Dedupe Property

**User Story:** As a Tokeira developer, I want a property test verifying that WorkflowExecutionTimedOut never emits a RequestDedupeOp, so that the internal-command-no-dedupe contract is maintained.

#### Acceptance Criteria

1. FOR ALL valid WorkflowExecutionTimedOut transitions, THE request_dedupe_ops SHALL be empty.

### Requirement 8.8: FailWorkflow Retry Metadata Property

**User Story:** As a Tokeira developer, I want a property test verifying that FailWorkflow emits correct retry metadata based on retry policy presence, so that the runtime receives consistent signals.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted transitions containing a FailWorkflow command where the workflow has a retry_policy, THE WorkflowExecutionFailed event's retry_state SHALL be RetryState::InProgress and attempt SHALL equal the workflow's attempt count.
2. FOR ALL valid WorkflowTaskCompleted transitions containing a FailWorkflow command where the workflow has no retry_policy, THE WorkflowExecutionFailed event's retry_state SHALL be RetryState::RetryPolicyNotSet.

### Requirement 8.9: ContinueAsNew Is Terminal (CommandsAfterClose) Property

**User Story:** As a Tokeira developer, I want a property test verifying that commands after ContinueAsNew are rejected, so that the terminal command contract is enforced.

#### Acceptance Criteria

1. FOR ALL valid WorkflowTaskCompleted requests containing a ContinueAsNew command followed by any additional workflow command, THE Kernel SHALL reject with CommandsAfterClose.

### Requirement 8.10: WorkflowExecutionTimedOut Event Field Pass-Through Property

**User Story:** As a Tokeira developer, I want a property test verifying that the WorkflowExecutionTimedOut event carries the timeout_type and retry_state from the request, so that no data is lost.

#### Acceptance Criteria

1. FOR ALL valid WorkflowExecutionTimedOut transitions, THE emitted WorkflowExecutionTimedOut event's timeout_type SHALL equal the request's timeout_type, and the event's retry_state SHALL equal the request's retry_state.
