# Command Surface — Temporal v1.31.0 (definition)

> Related to [the v1.31.0 conformance definition](../conformance/v1.31.0/README.md). This page maps the
> **state-mutating command and history-event surface** to its engine realisation: every Temporal
> history-service state mutation, the tokeira command that realises it, the history events emitted, and
> the operations deliberately excluded (with rationale). It is engine-implementation-oriented (kernel
> command/event mapping), which is why it lives here in readiness rather than in the (Temporal-only)
> conformance definition. For measured progress see [`./conformance.md`](./conformance.md).
>
> The mappings below are verified at the kernel level (golden + property tests). The broader RPC
> *behaviour* (errors, defaulting, lifecycle ordering) is defined per the README's ground-truth rule:
> the contract is whatever Temporal **v1.31.0** does, verified against `proto/upstream/` (wire) and the
> v1.31.0 server source (behaviour).

## Ground-truth sources

- **Temporal:** `service/history/interfaces/engine.go` (Engine interface), `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go` (workflow command switch), `components/nexusoperations/workflow/commands.go` (Nexus command registration), `service/history/historybuilder/history_builder.go` (event factory), `service/history/interfaces/mutable_state.go` — all at tag `v1.31.0`.
- **Tokeira:** `crates/tokeira-kernel/src/command.rs` (Command + WorkflowCommand), `crates/tokeira-kernel/src/event.rs` (HistoryEventKind), `crates/tokeira-kernel/src/kernel.rs` (BasicKernel::apply).
- **Classification:** each Temporal API is a kernel command (state-mutating, produces transitions), a runtime concern (no kernel involvement), or a deliberate exclusion with rationale.

---

## Part 1: Top-Level Commands

These are operations invoked on the history engine that mutate workflow execution state.

| # | Temporal Engine API | Tokeira Command | Status | Notes |
|---|---|---|---|---|
| 1 | `StartWorkflowExecution` | `Command::Start` | ✅ Implemented | F1. Emits WorkflowExecutionStarted + WorkflowTaskScheduled. |
| 2 | `SignalWorkflowExecution` | `Command::Signal` | ✅ Implemented | F1. Emits WorkflowExecutionSignaled, coalesces WFT. |
| 3 | `UpdateWorkflowExecution` | `Command::Update` | ✅ Implemented | F7. Emits WorkflowExecutionUpdateAccepted, tracks pending. Rejects paused workflows (F11). |
| 4 | `RequestCancelWorkflowExecution` | `Command::Cancel` | ✅ Implemented | F3. Cooperative two-phase cancel. |
| 5 | `TerminateWorkflowExecution` | `Command::Terminate` | ✅ Implemented | F3. Hard stop with entity cleanup. |
| 6 | `ResetWorkflowExecution` | `Command::Reset` | ✅ Implemented | F10. Emits WorkflowTaskFailed with RESET_WORKFLOW cause. |
| 7 | `UpdateWorkflowExecutionOptions` | `Command::UpdateExecutionOptions` | ✅ Implemented | F8. FieldChange pattern for versioning/callbacks. |
| 8 | `RecordWorkflowTaskStarted` | `Command::WorkflowTaskStarted` | ✅ Implemented | F1. Validates pending WFT, sets started_event_id. |
| 9 | `RespondWorkflowTaskCompleted` | `Command::WorkflowTaskCompleted` | ✅ Implemented | F1. Processes workflow commands sequentially. |
| 10 | `RespondWorkflowTaskFailed` | `Command::WorkflowTaskFailed` | ✅ Implemented | F2. Fenced by logical_seq + started_event_id. |
| 11 | (WFT timeout — internal) | `Command::WorkflowTaskTimedOut` | ✅ Implemented | F2. Clears sticky, re-dispatches. |
| 12 | `RespondActivityTaskCompleted` | `Command::ActivityResolved` (Completed) | ✅ Implemented | F1. Unified resolution enum. |
| 13 | `RespondActivityTaskFailed` | `Command::ActivityResolved` (Failed) | ✅ Implemented | F1. |
| 14 | `RespondActivityTaskCanceled` | `Command::ActivityResolved` (Canceled) | ✅ Implemented | F1. |
| 15 | (Timer scanner) | `Command::TimerDue` | ✅ Implemented | F1. Emits TimerFired, removes timer. |
| 16 | (Child start confirmation) | `Command::ChildStartConfirmed` | ✅ Implemented | F5. Fenced by initiated_event_id. |
| 17 | `RecordChildExecutionCompleted` | `Command::ChildResolved` | ✅ Implemented | F5. Handles all child terminal states. |
| 18 | (External signal resolved) | `Command::ExternalSignalResolved` | ✅ Implemented | F6. Removes from pending map. |
| 19 | (External cancel resolved) | `Command::ExternalCancelResolved` | ✅ Implemented | F6. Removes from pending map. |
| 20 | (Nexus operation resolved) | `Command::NexusOperationResolved` | ✅ Implemented | F9. Started is non-terminal. |
| 21 | (Execution/run timeout) | `Command::WorkflowExecutionTimedOut` | ✅ Implemented | F4. Terminal close with entity cleanup. |
| 22 | `PauseWorkflowExecution` | `Command::PauseWorkflow` | ✅ Implemented | F11. Sets Paused status, bumps stamps. |
| 23 | `UnpauseWorkflowExecution` | `Command::UnpauseWorkflow` | ✅ Implemented | F11. Restores Running, re-dispatches activities. |
| 24 | `UpdateActivityOptions` | `Command::UpdateActivityOptions` | ✅ Implemented | F11. Pure state mutation, no history events. |
| 25 | `PauseActivity` | `Command::PauseActivity` | ✅ Implemented | F11. Sets ActivityPauseInfo, bumps stamp. |
| 26 | `UnpauseActivity` | `Command::UnpauseActivity` | ✅ Implemented | F11. Clears pause, conditional dispatch. |
| 27 | `ResetActivity` | `Command::ResetActivity` | ✅ Implemented | F11. Resets attempt, conditional dispatch. |
| 28 | `ExecuteMultiOperation` (fresh-start leg) | `Command::StartAndUpdate` | ✅ Implemented | F7. Folds WorkflowExecutionStarted + update admission + WorkflowTaskScheduled into ONE transition (SignalWithStart atomicity precedent; `multioperation/api.go @ v1.31.0`). Attach paths reuse `Command::Update`. |

**Top-level command coverage: 28/28 (100%)**

---

## Part 2: Workflow Commands (within WorkflowTaskCompleted)

These are commands issued by worker code during a workflow task, processed sequentially within `WorkflowTaskCompleted`.

| # | Temporal Command Type | Tokeira WorkflowCommand | Status | Notes |
|---|---|---|---|---|
| 1 | `COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK` | `ScheduleActivity` | ✅ Implemented | F1. Timeout pass-through, duplicate rejection. |
| 2 | `COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION` | `CompleteWorkflow` | ✅ Implemented | F1. Terminal close. |
| 3 | `COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION` | `FailWorkflow` | ✅ Implemented | F1. Retry metadata on event. |
| 4 | `COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION` | `CancelWorkflow` | ✅ Implemented | F3. Terminal close with Cancelled status. |
| 5 | `COMMAND_TYPE_START_TIMER` | `StartTimer` | ✅ Implemented | F1. Duplicate rejection. |
| 6 | `COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK` | `RequestCancelActivity` | ✅ Implemented | F3. Activity remains pending until resolved. |
| 7 | `COMMAND_TYPE_CANCEL_TIMER` | `CancelTimer` | ✅ Implemented | F3. Removes timer, emits Delete op. |
| 8 | `COMMAND_TYPE_RECORD_MARKER` | `RecordMarker` | ✅ Implemented | F8. Pure event emission, no state change. |
| 9 | `COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION` | `RequestCancelExternalWorkflowExecution` | ✅ Implemented | F6. Tracks in pending map. |
| 10 | `COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION` | `SignalExternalWorkflowExecution` | ✅ Implemented | F6. Tracks in pending map. |
| 11 | `COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION` | `ContinueAsNew` | ✅ Implemented | F4. Terminal close with linkage. |
| 12 | `COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION` | `StartChildWorkflow` | ✅ Implemented | F5. Parent close policy. |
| 13 | `COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES` | `UpsertSearchAttributes` | ✅ Implemented | F1. ProjectionOp emission. |
| 14 | `COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES` | `UpsertMemo` | ✅ Implemented | F1. ProjectionOp emission. |
| 15 | `COMMAND_TYPE_PROTOCOL_MESSAGE` | `ProtocolMessage` | ✅ Implemented | F7. Carries UpdateProtocolBody inline. |
| 16 | `COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION` | `ScheduleNexusOperation` | ✅ Implemented | F9. Duplicate rejection. |
| 17 | `COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION` | `CancelNexusOperation` | ✅ Implemented | F9. Validates pending operation. |
| 18 | (Update completed via protocol) | `UpdateCompleted` | ✅ Implemented | F7. Removes from pending updates. |
| 19 | (Update rejected via protocol) | `UpdateRejected` | ✅ Implemented | F7. Removes from pending updates. |
| 20 | (Force new WFT) | `RequestNewWorkflowTask` | ✅ Implemented | F1. Conditional on no pending WFT. |

**Workflow command coverage: 20/20 (100%)**

---

## Part 3: History Event Types

Cross-reference of Temporal's HistoryBuilder event factory methods against Tokeira's HistoryEventKind enum.

| # | Temporal Event (HistoryBuilder method) | Tokeira HistoryEventKind | Status |
|---|---|---|---|
| 1 | `AddWorkflowExecutionStartedEvent` | `WorkflowExecutionStarted` | ✅ |
| 2 | `AddWorkflowTaskScheduledEvent` | `WorkflowTaskScheduled` | ✅ |
| 3 | `AddWorkflowTaskStartedEvent` | `WorkflowTaskStarted` | ✅ |
| 4 | `AddWorkflowTaskCompletedEvent` | `WorkflowTaskCompleted` | ✅ |
| 5 | `AddWorkflowTaskTimedOutEvent` | `WorkflowTaskTimedOut` | ✅ |
| 6 | `AddWorkflowTaskFailedEvent` | `WorkflowTaskFailed` | ✅ |
| 7 | `AddWorkflowExecutionPausedEvent` | `WorkflowExecutionPaused` | ✅ |
| 8 | `AddWorkflowExecutionUnpausedEvent` | `WorkflowExecutionUnpaused` | ✅ |
| 9 | `AddActivityTaskScheduledEvent` | `ActivityTaskScheduled` | ✅ |
| 10 | `AddActivityTaskStartedEvent` | — | ⚠️ See note 1 |
| 11 | `AddActivityTaskCompletedEvent` | `ActivityTaskCompleted` | ✅ |
| 12 | `AddActivityTaskFailedEvent` | `ActivityTaskFailed` | ✅ |
| 13 | `AddActivityTaskTimedOutEvent` | `ActivityTaskTimedOut` | ✅ |
| 14 | `AddCompletedWorkflowEvent` | `WorkflowExecutionCompleted` | ✅ |
| 15 | `AddFailWorkflowEvent` | `WorkflowExecutionFailed` | ✅ |
| 16 | `AddTimeoutWorkflowEvent` | `WorkflowExecutionTimedOut` | ✅ |
| 17 | `AddWorkflowExecutionTerminatedEvent` | `WorkflowExecutionTerminated` | ✅ |
| 18 | `AddWorkflowExecutionOptionsUpdatedEvent` | `WorkflowExecutionOptionsUpdated` | ✅ |
| 19 | `AddWorkflowExecutionUpdateAcceptedEvent` | `WorkflowExecutionUpdateAccepted` | ✅ |
| 20 | `AddWorkflowExecutionUpdateCompletedEvent` | `WorkflowExecutionUpdateCompleted` | ✅ |
| 21 | `AddWorkflowExecutionUpdateAdmittedEvent` | — | ⚠️ See note 2 |
| 22 | `AddContinuedAsNewEvent` | `WorkflowExecutionContinuedAsNew` | ✅ |
| 23 | `AddTimerStartedEvent` | `TimerStarted` | ✅ |
| 24 | `AddTimerFiredEvent` | `TimerFired` | ✅ |
| 25 | `AddActivityTaskCancelRequestedEvent` | `ActivityTaskCancelRequested` | ✅ |
| 26 | `AddActivityTaskCanceledEvent` | `ActivityTaskCanceled` | ✅ |
| 27 | `AddTimerCanceledEvent` | `TimerCanceled` | ✅ |
| 28 | `AddWorkflowExecutionCancelRequestedEvent` | `WorkflowExecutionCancelRequested` | ✅ |
| 29 | `AddWorkflowExecutionCanceledEvent` | `WorkflowExecutionCanceled` | ✅ |
| 30 | `AddRequestCancelExternalWorkflowExecutionInitiatedEvent` | `RequestCancelExternalWorkflowExecutionInitiated` | ✅ |
| 31 | `AddRequestCancelExternalWorkflowExecutionFailedEvent` | `RequestCancelExternalWorkflowExecutionFailed` | ✅ |
| 32 | `AddExternalWorkflowExecutionCancelRequested` | `ExternalWorkflowExecutionCancelRequested` | ✅ |
| 33 | `AddSignalExternalWorkflowExecutionInitiatedEvent` | `SignalExternalWorkflowExecutionInitiated` | ✅ |
| 34 | `AddUpsertWorkflowSearchAttributesEvent` | (emitted via ProjectionOp) | ✅ Semantic equivalent |
| 35 | `AddWorkflowPropertiesModifiedEvent` | (emitted via ProjectionOp) | ✅ Semantic equivalent |
| 36 | `AddSignalExternalWorkflowExecutionFailedEvent` | `SignalExternalWorkflowExecutionFailed` | ✅ |
| 37 | `AddExternalWorkflowExecutionSignaled` | `ExternalWorkflowExecutionSignaled` | ✅ |
| 38 | `AddMarkerRecordedEvent` | `MarkerRecorded` | ✅ |
| 39 | `AddWorkflowExecutionSignaledEvent` | `WorkflowExecutionSignaled` | ✅ |
| 40 | `AddStartChildWorkflowExecutionInitiatedEvent` | `StartChildWorkflowExecutionInitiated` | ✅ |
| 41 | `AddChildWorkflowExecutionStartedEvent` | `ChildWorkflowExecutionStarted` | ✅ |
| 42 | `AddChildWorkflowExecutionFailedEvent` | `ChildWorkflowExecutionFailed` | ✅ |
| 43 | `AddChildWorkflowExecutionCompletedEvent` | `ChildWorkflowExecutionCompleted` | ✅ |
| 44 | `AddStartChildWorkflowExecutionFailedEvent` | `StartChildWorkflowExecutionFailed` | ✅ |
| 45 | `AddChildWorkflowExecutionCanceledEvent` | `ChildWorkflowExecutionCanceled` | ✅ |
| 46 | `AddChildWorkflowExecutionTerminatedEvent` | `ChildWorkflowExecutionTerminated` | ✅ |
| 47 | `AddChildWorkflowExecutionTimedOutEvent` | `ChildWorkflowExecutionTimedOut` | ✅ |

**Notes:**
1. `ActivityTaskStarted` — In Temporal, this event is emitted by `RecordActivityTaskStarted` in the history service. Tokeira does not model activity task start as a kernel command because activity start is a delivery-layer concern (matching service records it). The kernel tracks activities from schedule to resolution. This is a deliberate architectural choice, not a gap.
2. `WorkflowExecutionUpdateAdmitted` — This is a Temporal-specific event for update admission ordering that is separate from acceptance. Tokeira's kernel combines admission and acceptance into a single `Update` command that emits `WorkflowExecutionUpdateAccepted`. The admitted event is an internal buffering mechanism in Temporal that Tokeira does not need because the kernel processes updates synchronously.

---

## Part 4: Temporal APIs Deliberately Excluded from the Kernel

These Temporal history engine APIs are not kernel commands. Each exclusion has a rationale.

### Read-Only Operations (no state mutation)
| API | Rationale |
|---|---|
| `QueryWorkflow` | Read-only. Runtime dispatches to worker, no state change. |
| `DescribeWorkflowExecution` | Read-only. Returns current state. |
| `DescribeMutableState` | Read-only. Debug/admin inspection. |
| `GetMutableState` | Read-only. Returns mutable state for long-poll. |
| `PollMutableState` | Read-only. Long-poll variant of GetMutableState. |
| `GetWorkflowExecutionHistory` | Read-only. Returns history events. |
| `GetWorkflowExecutionHistoryReverse` | Read-only. Reverse history iteration. |
| `GetWorkflowExecutionRawHistory` | Read-only. Raw history bytes. |
| `GetWorkflowExecutionRawHistoryV2` | Read-only. Raw history v2. |
| `PollWorkflowExecutionUpdate` | Read-only. Long-poll for update result. |
| `IsActivityTaskValid` | Read-only. Validation check. |
| `IsWorkflowTaskValid` | Read-only. Validation check. |

### Runtime Composition (not atomic kernel commands)
| API | Rationale |
|---|---|
| `SignalWithStartWorkflowExecution` | Runtime composes Start + Signal. Not a kernel primitive. |
| `ExecuteMultiOperation` | Runtime composes the Update-with-Start paths (dedup/attach/already-completed, wait-stage, error assembly). The fresh-start leg alone is the atomic `Command::StartAndUpdate` (Part 1 #28) — a raised kernel addition per the SignalWithStart precedent, not a general multi-op primitive. |

### Delivery-Layer Concerns
| API | Rationale |
|---|---|
| `RecordActivityTaskStarted` | Activity start is a delivery/matching concern. Kernel tracks schedule→resolution. |
| `RecordActivityTaskHeartbeat` | High-frequency runtime bookkeeping. Updates activity state without history events. Not a kernel transition. |
| `ScheduleWorkflowTask` | Runtime scheduling concern. Kernel schedules WFTs as part of other transitions. |
| `ResetStickyTaskQueue` | Delivery routing concern. Not a state transition. |

### Post-Close / Operational Tooling
| API | Rationale |
|---|---|
| `DeleteWorkflowExecution` | Post-close cleanup. Not a state transition on a live run. |
| `VerifyFirstWorkflowTaskScheduled` | Verification/consistency check. Not a state transition. |
| `VerifyChildExecutionCompletionRecorded` | Verification/consistency check. Not a state transition. |
| `RemoveSignalMutableState` | Internal cleanup of signal dedup state. Not a state transition. |
| `RebuildMutableState` | Operational recovery tool. Not a state transition. |
| `ImportWorkflowExecution` | Migration/import tool. Not a normal state transition. |
| `RefreshWorkflowTasks` | Operational recovery. Not a state transition. |

### Replication (multi-cluster concern)
| API | Rationale |
|---|---|
| `ReplicateHistoryEvents` | Multi-cluster replication. Not a single-cluster kernel concern. |
| `ReplicateEventsV2` | Multi-cluster replication. |
| `ReplicateWorkflowState` | Multi-cluster replication. |
| `ReplicateVersionedTransition` | Multi-cluster replication. |
| `SyncShardStatus` | Multi-cluster replication. |
| `SyncActivity` | Multi-cluster replication. |
| `SyncActivities` | Multi-cluster replication. |
| `SyncHSM` | Multi-cluster replication. |
| `SyncWorkflowState` | Multi-cluster replication. |
| `BackfillHistoryEvents` | Multi-cluster replication. |
| `GetReplicationMessages` | Multi-cluster replication. |
| `GetDLQReplicationMessages` | Multi-cluster replication. |
| `ReapplyEvents` | Multi-cluster event reapplication. |
| `GenerateLastHistoryReplicationTasks` | Multi-cluster replication. |
| `GetReplicationStatus` | Multi-cluster replication. |

### DLQ Management
| API | Rationale |
|---|---|
| `GetDLQMessages` | Dead letter queue management. Not a state transition. |
| `PurgeDLQMessages` | Dead letter queue management. |
| `MergeDLQMessages` | Dead letter queue management. |

### Task Management
| API | Rationale |
|---|---|
| `AddTasks` | Internal task queue management. Not a state transition. |
| `ListTasks` | Read-only task listing. |

### Lifecycle / Infrastructure
| API | Rationale |
|---|---|
| `NotifyNewHistoryEvent` | Internal notification. Not a state transition. |
| `NotifyNewTasks` | Internal notification. |
| `NotifyChasmExecution` | Internal notification (CHASM). |
| `StateMachineEnvironment` | Internal infrastructure. |
| `Start` / `Stop` | Engine lifecycle. |

---

## Part 5: Semantic Differences

Areas where Tokeira's kernel makes deliberate design choices that differ from Temporal's implementation.

| Area | Temporal Behaviour | Tokeira Behaviour | Rationale |
|---|---|---|---|
| Activity task start | `RecordActivityTaskStarted` is a history service API that emits `ActivityTaskStarted` event | Not a kernel command. Activity lifecycle is schedule→resolution. | Delivery-layer concern. Kernel doesn't need to track activity start. |
| Update admission | Separate `UpdateAdmitted` event for buffered updates | Combined admission+acceptance in single `Update` command | Kernel processes synchronously; no buffering needed. |
| Heartbeat recording | `RecordActivityTaskHeartbeat` mutates activity state | Not a kernel command | High-frequency runtime bookkeeping. |
| Concurrency limits | Temporal enforces per-entity pending limits (activities, timers, children, signals) | No per-entity ceilings | Deliberate rejection of arbitrary caps. |
| WFT stamp on pause | `WorkflowTaskStamp` on `ExecutionInfo` | `wft_stamp` on `WorkflowState` | Same semantics, different field location. |
| Cron scheduling | Handled in `RespondWorkflowTaskCompleted` handler | Not implemented | Cron is a runtime concern, not a kernel state transition. |
| Eager activity dispatch | Activity can be eagerly started in WFT completion response | Not implemented | Delivery optimisation, not a kernel concern. |

---

## Summary

| Category | Temporal Count | Tokeira Count | Coverage |
|---|---|---|---|
| Top-level commands | 27 | 25 | 25/27 (93%) — 2 are delivery-layer |
| Workflow commands | 20 | 20 | 20/20 (100%) |
| History event types | 47 | 45 | 45/47 (96%) — 2 deliberate exclusions |
| Deliberately excluded APIs | 38 | — | All documented with rationale |

The Tokeira kernel covers the complete Temporal workflow state machine command surface. The two top-level "gaps" (RecordActivityTaskStarted, RecordActivityTaskHeartbeat) and two event type "gaps" (ActivityTaskStarted, WorkflowExecutionUpdateAdmitted) are deliberate architectural choices documented above.
