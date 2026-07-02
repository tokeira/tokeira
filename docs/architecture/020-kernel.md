# 020 Kernel

**Status:** accepted — resolved questions recorded in [005-decisions-and-boundaries](005-decisions-and-boundaries.md)  
**Related docs:** [010-history-as-authority](010-history-as-authority.md), [030-runtime-lanes](030-runtime-lanes.md), [050-dsql-storage](050-dsql-storage.md)

## Purpose

`tokeira-kernel` is the **pure deterministic state machine** at the center of the system.

Its job is not to:

- talk to DSQL,
- route requests to workers,
- manage pollers,
- manage shards,
- manage connection budgets,
- write visibility rows.

Its job is to answer one question:

> **Given a loaded run state and one validated command, what exact transition should happen next?**

That question has to be answered with no hidden I/O and no side effects.

## Why the kernel matters

The kernel is the place where correctness should be easiest to reason about, easiest to test, and easiest to model formally later. If delivery or storage concerns leak inward, the code stops being a state machine and becomes a distributed subsystem in miniature.

Temporal's own architecture docs show that request handling for a workflow execution ultimately reduces to a state transition that appends history, updates mutable state, and creates follow-on tasks.[^history-service] Tokeira isolates that logic into its own crate.

## The core interface

The intended interface is:

```rust
pub trait Kernel {
    fn apply(&self, loaded: LoadedRun, cmd: Command) -> Result<Transition, Reject>;
}
```

The input:

- `LoadedRun`: authoritative current view of one run, already loaded and fenced by runtime/storage,
- `Command`: one semantic mutation request.

The output:

- `Transition`: a bounded, explicit description of what must be committed.

The error:

- `Reject`: the command is stale, invalid, duplicated, or impossible in the current state.

### LoadedRun

The kernel distinguishes two cases:

```rust
pub enum LoadedRun {
    Absent,
    Existing(WorkflowState),
}
```

`Absent` means the run does not yet exist in durable storage. Only the `Start` command accepts this variant. Every other command requires `Existing` with an open status; the kernel rejects commands against absent or closed runs via `expect_open`.

This two-variant enum makes "create a new run" and "mutate an existing run" type-safe at the kernel boundary. Runtime/storage is responsible for loading the correct variant before calling `apply`.

## Kernel inputs

The kernel should only see data that is already part of the deterministic transition boundary:

- run identity,
- current status,
- last event ID,
- current transition sequence,
- pending workflow task summary,
- open activities and timers relevant to the command,
- sticky execution affinity,
- dedupe hints already loaded by storage/runtime,
- the command payload itself.

The kernel should **not** call the clock, the RNG, the database, or network services directly. If a timestamp or deadline matters, runtime/storage should pass it in explicitly as data.

## Transition shape

The transition should be explicit and boring:

```rust
pub struct Transition {
    pub expected_seq: TransitionSeq,
    pub next_state: WorkflowState,
    pub history_events: SmallVec<[HistoryEvent; 8]>,
    pub request_dedupe_ops: SmallVec<[RequestDedupeOp; 1]>,
    pub activity_ops: SmallVec<[ActivityOp; 4]>,
    pub timer_ops: SmallVec<[TimerOp; 4]>,
    pub dispatch_ops: SmallVec<[DispatchOp; 4]>,
    pub projection_ops: SmallVec<[ProjectionOp; 8]>,
}
```

This shape is powerful because it keeps the kernel ignorant of storage layout while still telling storage exactly what must happen.

### `next_state`

The kernel computes the full next `WorkflowState`, not a delta. Storage can do a full state replacement rather than applying patches, which simplifies the DSQL commit path and avoids partial-update ambiguity.

### `expected_seq`

The transition carries the `TransitionSeq` that was current when the kernel began processing. Storage uses this as an optimistic concurrency fence: if the durable sequence has moved past `expected_seq`, the commit is rejected and the runtime must reload and retry.

### Request deduplication

`request_dedupe_ops` is part of the authoritative write set. The request ID is intentionally carried beside history rather than being treated as an edge-only concern. A durable execution platform must survive retries and partial failures without "maybe applied" ambiguity. Persisting request identity in the same fenced commit as the history batch is how Tokeira keeps that story honest.

Any command that originates from an external API call should carry a `RequestContext` with a `request_id`. This includes `Start`, `Signal`, `Update`, `Cancel`, `Terminate`, `UpdateExecutionOptions`, and `Reset`. The kernel emits a `RequestDedupeOp` for each such command.

All remaining commands are internal runtime machinery and do not carry request dedup: `WorkflowTaskStarted`, `WorkflowTaskCompleted`, `WorkflowTaskFailed`, `WorkflowTaskTimedOut`, `ActivityResolved`, `TimerDue`, `ChildStartConfirmed`, `ChildResolved`, `ExternalSignalResolved`, `ExternalCancelResolved`, `NexusOperationResolved`, and `WorkflowExecutionTimedOut`. These are not retryable external requests; they are derived from already-committed state or internal runtime events.

**Note on parent-driven cancellation:** When a parent workflow's close triggers cancellation of a child (via Parent Close Policy), the runtime delivers this as a `Cancel` command to the child run. This is the same top-level `Cancel` command used by external callers, and it carries a `RequestContext` with a runtime-generated request ID. The kernel does not distinguish between externally-initiated and parent-initiated cancellation; both follow the same dedup and event-emission path.

Runtime/storage is responsible for checking the dedupe table before calling `apply` and short-circuiting if the request was already committed.

### Idempotent re-delivery of internal commands

If the runtime crashes after committing a transition but before processing its dispatch ops (e.g., enqueueing a WFT or activity task), it will reload the run and may need to re-derive the dispatch effects. This is not a kernel concern. The runtime handles it by comparing the committed `transition_seq` against its last-processed sequence and re-reading the committed transition's dispatch ops from storage if needed. The kernel is never called twice for the same transition; idempotent recovery is a runtime/storage responsibility.

### Transition builder

Internally, the kernel assembles transitions through a `TransitionBuilder` that:

1. takes ownership of the current `WorkflowState` and a `now` timestamp,
2. provides an `emit(kind)` method that assigns the next contiguous event ID and appends a `HistoryEvent`,
3. provides `schedule_workflow_task()` which emits a `WorkflowTaskScheduled` event, sets the pending WFT on state, and pushes a `DispatchOp`,
4. provides `close(status)` which sets terminal status, clears pending WFT and sticky affinity, and emits a `ProjectionOp::CloseExecution`,
5. on `finish()`, increments `transition_seq` exactly once and returns the assembled `Transition`.

This pattern ensures that a single transition may append multiple history events but only increments the transition sequence once.

## Event IDs and transition sequences

Tokeira should maintain **two** monotonic counters per run:

### Event ID

This is the user-visible position in workflow history. Event IDs are assigned by the kernel at `emit` time, starting from `last_event_id + 1` and incrementing for each event within a transition. They are contiguous within a run and never reused.

### Transition sequence

This is the internal fence/checkpoint number for committed state transitions. It increments exactly once per `apply` call, regardless of how many history events are appended.

Why keep both?

- Event IDs are about workflow semantics and replay.
- Transition sequence is about internal correctness, task tokens, idempotency, and projection replay.

## Pending workflow task model

A pending workflow task should be represented explicitly:

```rust
pub struct PendingWorkflowTask {
    pub logical_seq: LogicalTaskSeq,
    pub scheduled_event_id: i64,
    pub started_event_id: Option<i64>,
    pub attempt: u32,
}
```

This is not the same thing as a queue row. It is the authoritative fact that a WFT exists for the run.

The kernel uses this to validate:

- that a `WorkflowTaskStarted` refers to the current pending task,
- that a completion token is not stale,
- that a signal burst does not create duplicate WFTs.

### The at-most-one-WFT invariant

Tokeira maintains the invariant that at most one workflow task is pending at any time. When a command would normally trigger a WFT (signal arrival, activity resolution, timer firing), the kernel checks whether `pending_workflow_task` is already `Some`. If it is, the kernel does **not** schedule a second one. This dramatically reduces wakeup amplification during signal floods without weakening per-run correctness.

## Sticky execution affinity

When a worker starts a workflow task and provides a `sticky_ttl`, the kernel records a `StickyAffinity` on the run state:

```rust
pub struct StickyAffinity {
    pub worker_identity: WorkerIdentity,
    pub expires_at: OffsetDateTime,
}
```

The lifecycle is:

1. **Set** on `WorkflowTaskStarted` if the worker provides a `sticky_ttl`.
2. **Propagated** when `schedule_workflow_task` emits a `DispatchOp::EnqueueWorkflowTask` with `sticky_preferred`.
3. **Cleared** on `close()` when the workflow reaches a terminal state.

The kernel does not enforce sticky routing. It only records the preference. The delivery broker decides whether to honor it based on worker availability and TTL expiry.

## WorkflowState: complete target shape

The kernel's view of a single run is captured in `WorkflowState`. The following is the current shape:

```rust
pub struct WorkflowState {
    // Identity
    pub run_key: RunKey,
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub task_queue: TaskQueueName,

    // Lifecycle
    pub status: ExecutionStatus,
    pub transition_seq: TransitionSeq,
    pub last_event_id: i64,
    pub started_at: OffsetDateTime,
    pub closed_at: Option<OffsetDateTime>,

    // Timeouts (set at start, enforced by runtime)
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub workflow_task_timeout: Duration,

    // Workflow task
    pub next_workflow_task_seq: LogicalTaskSeq,
    pub pending_workflow_task: Option<PendingWorkflowTask>,
    pub sticky: Option<StickyAffinity>,
    pub pause_info: Option<PauseInfo>,
    pub wft_stamp: u64,

    // User-mutable metadata
    pub memo: Memo,
    pub search_attributes: SearchAttributes,

    // Retry (recorded at start, evaluated by runtime)
    pub retry_policy: Option<RetryPolicy>,
    pub attempt: u32,

    // Open entities
    pub activities: BTreeMap<String, ActivityState>,
    pub timers: BTreeMap<String, TimerState>,
    pub children: BTreeMap<WorkflowId, ChildWorkflowState>,
    pub pending_external_signals: BTreeMap<i64, PendingExternalSignal>,
    pub pending_external_cancels: BTreeMap<i64, PendingExternalCancel>,

    // Open entities (not yet implemented)
    pub pending_updates: BTreeMap<String, PendingUpdate>,
    pub pending_nexus_operations: BTreeMap<String, PendingNexusOperation>,

    // Execution options (not yet implemented)
    pub versioning_override: Option<VersioningOverride>,
    pub completion_callbacks: Vec<CompletionCallback>,
}
```

Activity delivery state also carries pause-local invalidation metadata:

```rust
pub struct ActivityState {
    pub activity_id: String,
    pub schedule_event_id: i64,
    pub task_queue: TaskQueueName,
    pub attempt: u32,
    pub schedule_to_close_timeout: Option<Duration>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
    pub pause_info: Option<ActivityPauseInfo>,
    pub stamp: u64,
}
```

## Determinism rules

The kernel must obey these rules:

### 1. No I/O

All durable reads happen before `apply`, all durable writes happen after.

### 2. No ambient time

If a timer-firing command or timeout uses a timestamp, pass it in as part of the command or loaded state.

### 3. No random IDs created internally unless runtime passes the exact values in

This keeps replay and golden tests stable.

### 4. No hidden dependency on worker identity outside validated tokens / command payloads

Worker identity influences routing and sticky hints, but not the deterministic semantics of the state machine except where the history contract requires it.

## What the kernel should decide

The kernel **should** decide:

- which history events are appended,
- whether a WFT is scheduled,
- whether activities/timers are opened or closed,
- whether workflow execution is completed / failed / canceled / continued-as-new,
- which projection mutations follow from the state change,
- which request IDs are persisted for deduplication.

## What the kernel should not decide

The kernel should **not** decide:

- which lane hosts the actor,
- which poller gets the task,
- whether sync match or backlog is used,
- how DSQL retries a conflict,
- how a projection sink stores its row,
- whether a request ID has already been committed (that is a storage/runtime concern checked before `apply`).

### Queries are outside the kernel boundary

Temporal's Query is a read-only operation that does not mutate state, produce history events, or create a transition. Queries are handled entirely by the runtime: the runtime loads the run's state (or uses a cached worker replay), dispatches the query to the worker, and returns the result. The kernel is never called for a query because there is nothing to transition. This is a deliberate exclusion, not an oversight.

## Command taxonomy

The kernel command set matches workflow semantics, not transport surfaces. Commands are **semantic**: by the time something reaches the kernel, routing, auth, idempotency lookup, and request shaping should already have happened.

The full command set is:

| Command | Origin | Requires open run | Carries request dedupe |
|---|---|---|---|
| `Start` | External caller | No (requires `Absent`) | Yes |
| `Signal` | External caller | Yes | Yes |
| `Update` | External caller | Yes | Yes |
| `Cancel` | External caller or parent | Yes | Yes |
| `Terminate` | External caller or operator | Yes | Yes |
| `WorkflowTaskStarted` | Runtime/delivery | Yes | No |
| `WorkflowTaskCompleted` | Worker via runtime | Yes | No |
| `WorkflowTaskFailed` | Runtime (non-determinism, bad commands) | Yes | No |
| `WorkflowTaskTimedOut` | Runtime (start-to-close timeout) | Yes | No |
| `ActivityResolved` | Worker via runtime | Yes | No |
| `TimerDue` | Timer scanner | Yes | No |
| `ChildStartConfirmed` | Runtime (child run created) | Yes | No |
| `ChildResolved` | Runtime (child run closed) | Yes | No |
| `ExternalSignalResolved` | Runtime (signal delivered or failed) | Yes | No |
| `ExternalCancelResolved` | Runtime (cancel delivered or failed) | Yes | No |
| `NexusOperationResolved` | Runtime (Nexus op completed/failed/canceled) | Yes | No |
| `WorkflowExecutionTimedOut` | Runtime (execution/run timeout) | Yes | No |
| `UpdateExecutionOptions` | External caller or operator | Yes | Yes |
| `PauseWorkflow` | External caller or operator | Yes | Yes |
| `UnpauseWorkflow` | External caller or operator | Yes | Yes |
| `UpdateActivityOptions` | External caller or operator | Yes | Yes |
| `PauseActivity` | External caller or operator | Yes | Yes |
| `UnpauseActivity` | External caller or operator | Yes | Yes |
| `ResetActivity` | External caller or operator | Yes | Yes |
| `Reset` | Operator (eventually) | Yes | Yes |

`ContinueAsNew` is not a top-level kernel command. It is expressed exclusively as a workflow command within `WorkflowTaskCompleted` (see dedicated section below).

The following sections describe the exact behavior for each command.

### `Start`

**Precondition:** `LoadedRun::Absent`. Reject with `RunAlreadyExists` if the run already exists.

**Behavior:**

1. Initialize `WorkflowState` with `ExecutionStatus::Running`, `TransitionSeq::ZERO`, `last_event_id: 0`, empty activity/timer maps, identity fields from the request, and timeout configuration (`workflow_execution_timeout`, `workflow_run_timeout`, `workflow_task_timeout`). If a retry policy or cron schedule is provided, record those on state as well.
2. Emit `RequestDedupeOp` for the request ID.
3. Emit `WorkflowExecutionStarted` event carrying workflow type, task queue, input, memo, and search attributes.
4. Emit `ProjectionOp::UpsertExecution` with `Running` status and initial memo/search attributes.
5. Schedule a workflow task (emit `WorkflowTaskScheduled`, set pending WFT, push `DispatchOp::EnqueueWorkflowTask`).

**Events produced:** `WorkflowExecutionStarted`, `WorkflowTaskScheduled`.

**Rationale:** Start always schedules a WFT immediately because the workflow code must begin executing. The projection upsert ensures the execution is visible to list queries from the moment it is created. Timeout values are recorded on `WorkflowState` but enforced by the runtime (via timer scanners or dedicated timeout checks), not by the kernel.

### `Signal`

**Precondition:** Run must exist and be open.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID (dedupe is durable at *admission*, independent of whether the event buffers below).
2. If a WFT is currently **started** (a worker holds it), append a `WorkflowExecutionSignaled` entry to `WorkflowState.buffered_events` instead of emitting it — see [Buffered events](#buffered-events). No WFT is scheduled (one is by definition pending).
3. Otherwise emit the `WorkflowExecutionSignaled` event immediately, carrying signal name, input, request ID, and caller identity.
4. If no WFT is currently pending, schedule one.
5. If a WFT is already pending, do **not** create a second one.

**Events produced:** `WorkflowExecutionSignaled` (immediately or at flush), and optionally `WorkflowTaskScheduled`.

**Rationale:** The at-most-one-WFT invariant prevents wakeup amplification during signal floods. The pending WFT will deliver coalesced signals when the worker picks it up. This is a deliberate semantic choice, not a performance optimization that could be relaxed.

### Buffered events

The kernel adopts Temporal's **buffered-event model** (spec: `kernel-event-buffering`, superseding the earlier deliberate no-buffering deviation). A worker holding a started WFT has a history view frozen at `started_event_id`; externally-originated events admitted in that window are held durably on `WorkflowState.buffered_events` — without event ids — and flushed into history when the WFT closes.

- **Predicate** (`should_buffer`, mirroring `bufferEvent`, `historybuilder/event_store.go:263 @ v1.31.0`): workflow state-change events, workflow-task events, and events generated directly from a worker command or protocol message never buffer. Externally-originated events buffer while a WFT is *started*. Phase 1 scopes the bufferable set to `WorkflowExecutionSignaled` and `WorkflowExecutionCancelRequested`; Phase 2 extends to activity/child/Nexus completion-class events with the `reorderBuffer` rule (`event_store.go:411`).
- **Flush** happens at every WFT close — `WorkflowTaskCompleted`, `WorkflowTaskFailed`, `WorkflowTaskTimedOut`, and the force-close below — emitting buffered events in admission order with contiguous ids immediately after the close event (`failWorkflowTask`, `workflow/util.go:26 @ v1.31.0`). On completion, flushed events land after the completion's command events and before any follow-up `WorkflowTaskScheduled`, and their presence triggers that follow-up WFT.
- **Forced closes** (`Terminate`, workflow timeout, and the `RespondWorkflowTaskFailed(GRPC_MESSAGE_TOO_LARGE)` route): a started WFT is failed first with cause `ForceCloseCommand`, buffered events flush, and only then is the terminal event appended (`TerminateWorkflow`/`TimeoutWorkflow`, `workflow/util.go:71,115 @ v1.31.0`).
- **Close discards**: buffered events surviving to a worker-commanded close are dropped, matching v1.31.0 (`FlushBufferToCurrentBatch` workflowFinished branch, `event_store.go:139`). Closed runs always carry an empty buffer.

### `Update`

**Precondition:** Run must exist and be open.

Temporal's Update is a synchronous, tracked write request.[^update] Unlike a signal, the caller waits for the workflow to process the update and return a result or error. Unlike a query, the update can mutate workflow state and is recorded in history.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Emit `WorkflowExecutionUpdateAccepted` event carrying the update ID, update name, and input.
3. If no WFT is currently pending, schedule one. The WFT will carry the update to the worker for processing.
4. If a WFT is already pending, coalesce (same invariant as signals).

The update is not yet *completed* at this point. Completion happens when the worker processes the update during a workflow task and the kernel receives the result via `WorkflowTaskCompleted` carrying an `UpdateCompleted` or `UpdateRejected` workflow command.

**Events produced:** `WorkflowExecutionUpdateAccepted`, and optionally `WorkflowTaskScheduled`.

**Pending updates model:**

The kernel should track pending updates in `WorkflowState`, analogous to pending activities:

```rust
pub struct PendingUpdate {
    pub update_id: String,
    pub accepted_event_id: i64,
    pub name: String,
}
```

**Rationale:** Updates are the most complex message type because they span two transitions: acceptance (when the update arrives) and completion (when the worker finishes processing). The kernel must track the pending set so it can validate that completion references a real accepted update and reject duplicates.

### `Cancel`

**Precondition:** Run must exist and be open.

Cancellation is a graceful stop request.[^cancel] It does not immediately close the workflow. Instead, it records the request and gives the workflow code an opportunity to run cleanup logic.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Emit `WorkflowExecutionCancelRequested` event carrying the reason, and if applicable, the external workflow execution that initiated the cancellation.
3. If no WFT is currently pending, schedule one.
4. If a WFT is already pending, coalesce.

**Events produced:** `WorkflowExecutionCancelRequested`, and optionally `WorkflowTaskScheduled`.

**What happens next:** The worker receives the cancellation request during its next WFT. The workflow code may run cleanup logic and eventually issue a `CancelWorkflow` workflow command, or ignore the cancellation entirely. The kernel does not enforce that a cancel request leads to a closed workflow.

**Rationale:** Cancel is intentionally a two-phase operation. The kernel records the request but does not close the run. This preserves the Temporal contract that cancellation is cooperative.

### `Terminate`

**Precondition:** Run must exist and be open.

Termination is a hard stop.[^terminate] The workflow code does not get a chance to run cleanup logic. The server closes the run immediately.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Emit `WorkflowExecutionTerminated` event carrying the reason, optional details, and the identity of the caller.
3. Close the run with `ExecutionStatus::Terminated`: set terminal status, clear pending WFT, clear sticky affinity.
4. Emit `ProjectionOp::CloseExecution` with `Terminated` status.
5. Clear activity and timer maps in `next_state`. Emit `ActivityOp::Delete` for each open activity and `TimerOp::Delete` for each open timer.
6. For open child workflows, apply Parent Close Policy (see `ChildResolved`).

**Events produced:** `WorkflowExecutionTerminated`.

**Rationale:** Terminate is the "kill -9" of workflow lifecycle. The kernel does not schedule a WFT because the worker is not consulted.

### `WorkflowTaskStarted`

**Precondition:** Run must exist and be open. A pending WFT must exist with `started_event_id == None`.

**Validation:**

- Reject with `NoPendingWorkflowTask` if no WFT is pending.
- Reject with `WorkflowTaskSeqMismatch` if the request's `logical_seq` does not match the pending WFT's `logical_seq`.
- Reject with `WorkflowTaskAlreadyStarted` if the pending WFT already has a `started_event_id`.

**Behavior:**

1. Increment the pending WFT's `attempt` counter.
2. Emit `WorkflowTaskStarted` event carrying the logical sequence, scheduled event ID, attempt number, and worker identity.
3. Set `started_event_id` on the pending WFT to the emitted event's ID.
4. If the worker provides a `sticky_ttl`, record `StickyAffinity` on the run state.

**Events produced:** `WorkflowTaskStarted`.

### `WorkflowTaskFailed`

**Precondition:** Run must exist and be open. A pending WFT must exist and must have been started.

This command is issued by the runtime when a workflow task fails due to non-determinism errors, invalid commands, payload validation failures, or other server-detected problems. It is also the mechanism used by Reset.

**Behavior:**

1. Emit `WorkflowTaskFailed` event carrying the scheduled/started event IDs, failure cause, failure details, worker identity, and optionally `base_run_id`/`new_run_id`/`fork_event_version` for resets.
2. Clear `started_event_id` on the pending WFT (reverts to scheduled-but-not-started).
3. Push `DispatchOp::EnqueueWorkflowTask` to re-dispatch the WFT for retry.

**Events produced:** `WorkflowTaskFailed`.

**Rationale:** WFT failure is not terminal for the workflow. The server retries the WFT, giving the worker another chance to replay and produce valid commands.

### `WorkflowTaskTimedOut`

**Precondition:** Run must exist and be open. A pending WFT must exist and must have been started.

This command is issued by the runtime when a started workflow task exceeds its start-to-close timeout without the worker responding.

**Behavior:**

1. Emit `WorkflowTaskTimedOut` event carrying the scheduled/started event IDs and timeout type.
2. Clear `started_event_id` on the pending WFT.
3. Clear `StickyAffinity` on the run state (the worker is presumed unavailable).
4. Push `DispatchOp::EnqueueWorkflowTask` to re-dispatch without sticky preference.

**Events produced:** `WorkflowTaskTimedOut`.

**Rationale:** WFT timeout is the primary mechanism for recovering from worker failures. Clearing sticky affinity ensures the retried WFT goes to the normal task queue.

### `WorkflowTaskCompleted`

**Precondition:** Run must exist and be open. A pending WFT must exist and must have been started.

**Validation:**

- Reject with `NoPendingWorkflowTask` if no WFT is pending.
- Reject with `WorkflowTaskNotStarted` if the pending WFT has no `started_event_id`.
- Reject with `WorkflowTaskSeqMismatch` if the token's `logical_seq` does not match.
- Reject with `WorkflowTaskTokenMismatch` if the token's `attempt` or `started_event_id` does not match.

**Behavior:**

1. Emit `WorkflowTaskCompleted` event.
2. Clear the pending WFT from state.
3. Apply each worker-issued workflow command in order (see table below).
4. If any workflow command closes the run, reject subsequent commands with `CommandsAfterClose`.
5. If `force_new_workflow_task` is set and the run is still open with no pending WFT, schedule a new WFT.

**Workflow commands:**

| Workflow command | Behavior | Closes run? |
|---|---|---|
| `ScheduleActivity` | Emit `ActivityTaskScheduled`, create `ActivityState`, push `ActivityOp::Upsert` and `DispatchOp::EnqueueActivityTask`. Carries schedule-to-close, schedule-to-start, start-to-close, and heartbeat timeouts as pass-through fields in the event and dispatch op. Reject with `DuplicateActivityId` if already open. | No |
| `StartTimer` | Emit `TimerStarted`, create `TimerState`, push `TimerOp::Upsert`. Reject with `DuplicateTimerId` if already open. | No |
| `UpsertMemo` | Update memo on state, emit `ProjectionOp::UpsertExecution`. | No |
| `UpsertSearchAttributes` | Update search attributes on state, emit `ProjectionOp::UpsertExecution`. | No |
| `CompleteWorkflow` | Emit `WorkflowExecutionCompleted`, close run with `Completed`. | Yes |
| `FailWorkflow` | Emit `WorkflowExecutionFailed`, close run with `Failed`. | Yes |
| `CancelWorkflow` | Emit `WorkflowExecutionCanceled`, close run with `Canceled`. | Yes |
| `ContinueAsNew` | See dedicated section below. | Yes |
| `RequestNewWorkflowTask` | If run is open and no WFT is pending, schedule one. Otherwise no-op. | No |
| `StartChildWorkflow` | See `ChildResolved` section. | No |
| `RequestCancelActivity` | Emit `ActivityTaskCancelRequested`. Activity remains pending until resolved. | No |
| `CancelTimer` | Emit `TimerCanceled`, remove timer from state, push `TimerOp::Delete`. | No |
| `SignalExternalWorkflow` | See dedicated section. | No |
| `RequestCancelExternalWorkflow` | See dedicated section. | No |
| `RecordMarker` | See dedicated section. | No |
| `ScheduleNexusOperation` | See dedicated section. | No |
| `CancelNexusOperation` | See dedicated section. | No |
| `ProtocolMessage` | See dedicated section. | No |
| `UpdateCompleted` | Emit `WorkflowExecutionUpdateCompleted`, remove from pending updates. | No |
| `UpdateRejected` | Emit `WorkflowExecutionUpdateRejected`, remove from pending updates. | No |

**Rationale:** `WorkflowTaskCompleted` is the most complex command because it is the only path through which workflow code can express intent. The sequential application with early termination on close ensures the kernel never processes commands against a run already closed within the same transition.

### `ActivityResolved`

**Precondition:** Run must exist and be open. The referenced activity must exist in the open activities map.

**Behavior:**

1. Match on the resolution type: emit `ActivityTaskCompleted`, `ActivityTaskFailed`, `ActivityTaskTimedOut`, or `ActivityTaskCanceled`.
2. Remove the activity from the state's activities map.
3. Push `ActivityOp::Delete` for the resolved activity.
4. If no WFT is currently pending, schedule one.

**Rationale:** Activity resolution always triggers a WFT because the workflow code needs to observe the result. The kernel does not retry activities; retry policy is enforced by the runtime.

### `TimerDue`

**Precondition:** Run must exist and be open. The referenced timer must exist in the open timers map.

**Behavior:**

1. Emit `TimerFired` event.
2. Remove the timer from the state's timers map.
3. Push `TimerOp::Delete` for the fired timer.
4. If no WFT is currently pending, schedule one.

**Rationale:** Timer firing is a server-side event. The timer scanner detects that a timer's `fire_at` has passed and issues this command.

### `WorkflowExecutionTimedOut`

**Precondition:** Run must exist and be open.

This command is issued by the runtime when the workflow's execution timeout or run timeout expires.

**Behavior:**

1. Emit `WorkflowExecutionTimedOut` event carrying the timeout type and retry state.
2. Close the run with `ExecutionStatus::TimedOut`.
3. Emit `ProjectionOp::CloseExecution` with `TimedOut` status.
4. Clean up open entities (same as `Terminate`).
5. For open child workflows, apply Parent Close Policy.
6. If the workflow has a retry policy and should be retried, emit metadata for the runtime to create a retry run.

**Rationale:** Workflow-level timeouts are enforced by the server, not by the worker. The kernel treats it as a terminal close.

### Workflow-level retry

Temporal retries workflows on both timeout and failure if a retry policy is configured. Tokeira handles this the same way as Continue-As-New: the kernel closes the current run with the appropriate terminal status and emits linkage metadata. The runtime reads that metadata and decides whether to create a retry run.

Specifically:

- When `FailWorkflow` closes a run and the workflow has a `retry_policy`, the kernel emits the current `attempt` count and retry state in the `WorkflowExecutionFailed` event. The runtime checks the retry policy (max attempts, non-retryable error types, backoff) and, if a retry is warranted, issues a `Start` command for the new run with an incremented `attempt` and `continued_execution_run_id` linking back.
- When `WorkflowExecutionTimedOut` closes a run, the same pattern applies: the kernel emits retry state, the runtime decides.
- The kernel does not evaluate retry policy logic. It records the attempt count and terminal status. The runtime owns the retry decision, backoff calculation, and successor creation.

This keeps the kernel focused on single-run transitions while giving the runtime full control over retry semantics.

### `UpdateExecutionOptions`

**Precondition:** Run must exist and be open.

This command allows updating workflow execution options on a running workflow, such as versioning overrides and completion callbacks.[^events]

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Emit `WorkflowExecutionOptionsUpdated` event carrying versioning override and/or completion callbacks.
3. Update the relevant fields on `WorkflowState`.

**Rationale:** This is a server-side mutation that does not come from workflow code. It allows operators to modify execution behavior without a WFT round-trip.

### `PauseWorkflow`

**Precondition:** Run must exist and be open.

**Behavior:**

1. If the run is already paused with the same request ID, treat the command as an idempotent no-op.
2. If the run is already paused with a different request ID, reject with `AlreadyPaused`.
3. Emit `RequestDedupeOp` for the request ID.
4. Emit `WorkflowExecutionPaused` carrying identity, reason, and request ID.
5. Set `status = Paused`, populate `pause_info`, and increment `wft_stamp`.
6. Increment every open activity's `stamp` and emit `ActivityOp::Upsert` for each so stale deliveries can be invalidated.
7. Emit `ProjectionOp::UpsertExecution` with `Paused` status.
8. Do not schedule or redispatch a workflow task.

### `UnpauseWorkflow`

**Precondition:** Run must exist and be open with `ExecutionStatus::Paused`.

**Behavior:**

1. Reject with `NotPaused` if the run is not paused.
2. Emit `RequestDedupeOp` for the request ID.
3. Emit `WorkflowExecutionUnpaused` carrying identity, reason, and request ID.
4. Set `status = Running`, clear `pause_info`, and increment `wft_stamp`.
5. Increment every open activity's `stamp`, emit `ActivityOp::Upsert`, and emit `DispatchOp::EnqueueActivityTask` for each activity.
6. Emit `ProjectionOp::UpsertExecution` with `Running` status.
7. Schedule a new WFT only if no WFT is already pending.

### `UpdateActivityOptions`

**Precondition:** Run must exist and be open. The referenced activity must exist in the open activities map.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Apply `FieldChange` updates to the activity task queue and timeout fields.
3. Increment the activity's `stamp`.
4. Emit `ActivityOp::Upsert`.
5. Emit no history event and do not schedule a WFT.

### `PauseActivity`

**Precondition:** Run must exist and be open. The referenced activity must exist in the open activities map.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Set `pause_info` on the activity using the provided identity, reason, and time.
3. Increment the activity's `stamp`.
4. Emit `ActivityOp::Upsert`.
5. Emit no history event and do not schedule a WFT.

### `UnpauseActivity`

**Precondition:** Run must exist and be open. The referenced activity must exist in the open activities map.

**Behavior:**

1. Reject with `ActivityNotPaused` if the activity is not paused.
2. Emit `RequestDedupeOp` for the request ID.
3. Clear the activity's `pause_info`.
4. Increment the activity's `stamp`.
5. Emit `ActivityOp::Upsert`.
6. If the workflow itself is not paused, emit `DispatchOp::EnqueueActivityTask`. If the workflow is paused, defer redispatch until `UnpauseWorkflow`.

### `ResetActivity`

**Precondition:** Run must exist and be open. The referenced activity must exist in the open activities map.

**Behavior:**

1. Emit `RequestDedupeOp` for the request ID.
2. Reset the activity attempt counter to `1`.
3. Accept `reset_heartbeat` for API compatibility, but treat it as a no-op until heartbeat details are persisted in state.
4. Increment the activity's `stamp`.
5. Emit `ActivityOp::Upsert`.
6. If the workflow itself is not paused, emit `DispatchOp::EnqueueActivityTask`. If the workflow is paused, defer redispatch until `UnpauseWorkflow`.

### `ChildStartConfirmed`

**Precondition:** Run must exist and be open. The referenced child must exist in the open children map with no `started_event_id` yet.

This command is issued by the runtime when it has successfully created the child workflow run.

**Behavior:**

1. Emit `ChildWorkflowExecutionStarted` event carrying the child workflow ID, run ID, and workflow type.
2. Update the child entry in the open children map to record the started event ID.
3. If no WFT is currently pending, schedule one.

**Rejection:** If the child could not be started (e.g., workflow ID conflict), the runtime issues this command with a failure variant, and the kernel emits `StartChildWorkflowExecutionFailed`, removes the child from the open set, and schedules a WFT.

### `ChildResolved`

**Precondition:** Run must exist and be open. The referenced child workflow must exist in the open children map.

Child workflows are started via the `StartChildWorkflow` workflow command during `WorkflowTaskCompleted`. The kernel tracks open children in `WorkflowState`:

```rust
pub struct ChildWorkflowState {
    pub child_workflow_id: WorkflowId,
    pub child_run_id: Option<RunId>,
    pub initiated_event_id: i64,
    pub started_event_id: Option<i64>,
    pub parent_close_policy: ParentClosePolicy,
}
```

**Starting a child (via WorkflowTaskCompleted):**

1. Emit `StartChildWorkflowExecutionInitiated` event.
2. Add the child to the open children map (with `child_run_id: None`, `started_event_id: None`).
3. Push a `DispatchOp::StartChildWorkflow` so the runtime can create the child run.

The runtime then issues `ChildStartConfirmed` (see above) to record the start or failure.

**Resolution (this command):**

When the child run reaches a terminal state, the runtime issues `ChildResolved`.

1. Match on the child's terminal status: emit `ChildWorkflowExecutionCompleted`, `Failed`, `Canceled`, `Terminated`, or `TimedOut`.
2. Remove the child from the open children map.
3. If no WFT is currently pending, schedule one.

**Parent Close Policy:**

When a parent workflow closes, the kernel applies the Parent Close Policy for each open child:[^child-workflows]

- `Terminate`: emit ops to terminate the child.
- `RequestCancelExternalWorkflow`: emit ops to send a cancel request to the child.
- `Abandon`: do nothing; the child continues independently.

**Rationale:** From the parent's perspective, the entire chain of Continue-As-New runs for a child is treated as a single execution. The kernel only sees the final resolution.

### `SignalExternalWorkflowExecution` (workflow command)

Workflow code can signal another workflow execution. This is an awaitable operation.

**Behavior (within WorkflowTaskCompleted):**

1. Emit `SignalExternalWorkflowExecutionInitiated` event.
2. Track the pending external signal in `WorkflowState`:

```rust
pub struct PendingExternalSignal {
    pub initiated_event_id: i64,
    pub target_workflow_id: WorkflowId,
    pub target_run_id: Option<RunId>,
    pub signal_name: String,
}
```

3. Push `DispatchOp::SignalExternalWorkflow` so the runtime can deliver the signal.

**Resolution (via `ExternalSignalResolved` command):**

When the runtime confirms delivery or reports failure, it issues the `ExternalSignalResolved` top-level kernel command. The kernel then:

1. If successful: emit `ExternalWorkflowExecutionSignaled` and remove the entry from the pending set.
2. If failed: emit `SignalExternalWorkflowExecutionFailed` and remove the entry from the pending set.
3. In both cases, if no WFT is pending, schedule one.

### `RequestCancelExternalWorkflowExecution` (workflow command)

Workflow code can request cancellation of another workflow execution (not limited to children). This is also awaitable.

**Behavior (within WorkflowTaskCompleted):**

1. Emit `RequestCancelExternalWorkflowExecutionInitiated` event.
2. Track the pending external cancel request in `WorkflowState`.
3. Push `DispatchOp::RequestCancelExternalWorkflow`.

**Resolution (via `ExternalCancelResolved` command):**

When the runtime confirms the cancel request was delivered, it issues the `ExternalCancelResolved` top-level kernel command. The kernel then:

1. If successful: emit `ExternalWorkflowExecutionCancelRequested` and remove the entry from the pending set.
2. If failed: emit `RequestCancelExternalWorkflowExecutionFailed` and remove the entry from the pending set.
3. If no WFT is pending, schedule one.

### `RecordMarker` (workflow command)

Markers are the SDK's mechanism for recording opaque, SDK-interpreted data in history. The server treats them as pass-through. The SDK uses the marker type to distinguish between:[^markers]

- **Side effects:** Record non-deterministic operation results for stable replay.
- **Mutable side effects:** Values that can change across workflow tasks.
- **Local activities:** Record locally-executed activity results so replay can skip re-execution.
- **Version markers / patches:** Record which code version was used, enabling safe rolling deployments.

**Behavior (within WorkflowTaskCompleted):**

1. Emit `MarkerRecorded` event carrying the marker name, details map, failure details, and header.
2. No state change. No dispatch ops. No projection ops.

**Rationale:** The kernel must support markers even though it does not interpret them. Without markers, there is no path for local activities, side effects, or workflow versioning. All semantic interpretation happens in the SDK during replay.

### `ScheduleNexusOperation` (workflow command)

Nexus is Temporal's mechanism for cross-namespace workflow invocation through typed service contracts.[^nexus]

**Behavior (within WorkflowTaskCompleted):**

1. Emit `NexusOperationScheduled` event.
2. Track the pending Nexus operation in `WorkflowState`:

```rust
pub struct PendingNexusOperation {
    pub operation_id: String,
    pub scheduled_event_id: i64,
    pub endpoint: String,
    pub service: String,
    pub operation: String,
}
```

3. Push `DispatchOp::ScheduleNexusOperation`.

**Resolution (via `NexusOperationResolved` command):**

When the runtime has a result, it issues the `NexusOperationResolved` top-level kernel command. The kernel emits the appropriate event:

- `NexusOperationStarted` (async operation accepted — remains pending),
- `NexusOperationCompleted` (remove from pending),
- `NexusOperationFailed` (remove from pending),
- `NexusOperationCanceled` (remove from pending),
- `NexusOperationTimedOut` (remove from pending).

On terminal resolution, schedule a WFT if none is pending.

### `CancelNexusOperation` (workflow command)

Requests cancellation of a pending Nexus operation.

**Behavior (within WorkflowTaskCompleted):**

1. Emit `NexusOperationCancelRequested` event.
2. Push `DispatchOp::CancelNexusOperation`.

The operation remains pending until it resolves.

### `ProtocolMessage` (workflow command)

ProtocolMessage is the carrier mechanism for update events within a WFT completion. It controls where update acceptance, completion, and rejection events appear in history relative to other workflow commands in the same transition.[^update]

The worker sends `ProtocolMessage` commands interleaved with other workflow commands (ScheduleActivity, StartTimer, etc.). Each ProtocolMessage carries an `UpdateProtocolBody` that determines what event to emit:

- `UpdateProtocolBody::Accepted { update_id, update_name, input }` — emit `WorkflowExecutionUpdateAccepted`, add `PendingUpdate` to state. Reject with `DuplicateUpdateId` if the update_id is already pending.
- `UpdateProtocolBody::Completed { update_id, result }` — emit `WorkflowExecutionUpdateCompleted`, remove from pending. Reject with `UnknownUpdate` if not found.
- `UpdateProtocolBody::Rejected { update_id, failure }` — emit `WorkflowExecutionUpdateRejected`, remove from pending. Reject with `UnknownUpdate` if not found.

Since the kernel processes workflow commands sequentially, the position of the ProtocolMessage in the command list directly determines where the update event lands in the event sequence. No buffering or reordering is needed.

`UpdateCompleted` and `UpdateRejected` also exist as standalone workflow commands for cases where ordering relative to other commands doesn't matter. Both paths produce the same events and state changes.

### `ContinueAsNew` (workflow command)

Temporal documents Continue-As-New as a way to checkpoint workflow state into a new run with the same Workflow ID but a fresh history and Run ID.[^continue-as-new]

Continue-As-New is expressed as a workflow command within `WorkflowTaskCompleted`, not as a top-level kernel command.

**Behavior:**

1. Emit `WorkflowExecutionContinuedAsNew` event carrying the new run ID, workflow type, task queue, input, memo, search attributes, workflow execution timeout, workflow run timeout, and workflow task timeout.
2. Close the current run with `ExecutionStatus::ContinuedAsNew`.
3. Emit `ProjectionOp::CloseExecution` with `ContinuedAsNew` status.

**What the kernel does not do:** The kernel does not create the successor run. The runtime reads the event and issues a `Start` command for the successor.

**Chain metadata:** The successor's `WorkflowExecutionStarted` event carries `continued_execution_run_id`, `first_execution_run_id`, and `initiator`. This metadata is set by the runtime, not the kernel.

**Rationale:** Keeping continue-as-new as close-current + start-successor means the kernel never reasons about two runs simultaneously.

### `Reset` (eventually)

Reset terminates the current workflow execution, discards history after a chosen event ID, and starts a new execution that replays from that point.[^reset]

**Behavior (planned):**

1. Emit `RequestDedupeOp` for the request ID.
2. Emit `WorkflowTaskFailed` event with a `RESET_WORKFLOW` cause, referencing the fork event ID.
3. Close the current run with `ExecutionStatus::Terminated`.
4. Emit metadata for the runtime to create the reset run.

**What the kernel does not do:** The kernel does not copy history or construct the reset run's initial state. The runtime handles that.

**Rationale:** Reset is architecturally similar to Continue-As-New: close the current run, let the runtime create the successor. Deferred because it requires careful interaction with the history loading path.

## Reject taxonomy

The kernel's `Reject` error type is a precise, enumerated set of rejection reasons that the runtime can act on programmatically.

### Existence checks

- `RunAlreadyExists`: `Start` was called but the run already exists.
- `MissingRun`: a command was issued for a run that does not exist.
- `RunClosed(status)`: a command was issued for a run that has already reached a terminal state.

### Sequence fencing

- `WorkflowTaskSeqMismatch { expected, got }`: the command references a WFT logical sequence that does not match the current pending WFT.
- `WorkflowTaskAlreadyStarted { logical_seq }`: a `WorkflowTaskStarted` was issued but the pending WFT already has a `started_event_id`.
- `WorkflowTaskNotStarted { logical_seq }`: a `WorkflowTaskCompleted` was issued but the pending WFT has not been started.

### Token validation

- `WorkflowTaskTokenMismatch`: the completion token's `attempt` or `started_event_id` does not match the pending WFT.

### Uniqueness constraints

- `DuplicateActivityId(id)`: a `ScheduleActivity` command references an activity ID that is already open.
- `DuplicateTimerId(id)`: a `StartTimer` command references a timer ID that is already open.

### Entity resolution

- `UnknownActivity(id)`: an `ActivityResolved` command references an activity ID not in the open set.
- `UnknownTimer(id)`: a `TimerDue` command references a timer ID not in the open set.

### Ordering constraints

- `CommandsAfterClose { index }`: a workflow command was issued after a preceding command in the same WFT completion already closed the run.

### Implemented additions (Features 2-11)

- `DuplicateChildWorkflowId(WorkflowId)`: a `StartChildWorkflow` command references a child workflow ID already in the open set. (Feature 5)
- `UnknownChild(WorkflowId)`: child resolution or confirmation for a child not in the open set. (Feature 5)
- `StaleChildConfirmation { child_workflow_id, expected_initiated_event_id }`: child start confirmation with mismatched initiated_event_id. (Feature 5)
- `UnknownExternalSignal(i64)`: external signal resolution for a signal not in the pending set. (Feature 6)
- `UnknownExternalCancel(i64)`: external cancel resolution for a cancel not in the pending set. (Feature 6)
- `UnknownUpdate(String)`: update completion for an update not in the pending set. (Feature 7)
- `DuplicateUpdateId(String)`: duplicate update acceptance. (Feature 7)
- `DuplicateNexusOperationId(id)`: a `ScheduleNexusOperation` command references an operation ID already pending. (Feature 9)
- `NexusOperationAlreadyStarted(id)`: duplicate `Started` callback for a pending Nexus operation. (Feature 9)
- `WorkflowPaused`: update rejected while workflow status is paused. (Feature 11)
- `AlreadyPaused`: duplicate pause request with a different request ID. (Feature 11)
- `NotPaused`: unpause request against a non-paused workflow. (Feature 11)
- `ActivityNotPaused(String)`: unpause request against an activity that is not paused. (Feature 11)

### Future additions

- `ContinueAsNewConstraintViolation`: e.g., open children that cannot be abandoned.
- `CancellationRace`: concurrent cancel/terminate/complete conflicts.
- `ResetConstraintViolation`: invalid fork event ID or incompatible history.

## Activity retry and heartbeat boundary

The kernel does not retry activities. When an activity fails and the retry policy says to retry, the runtime handles this outside the kernel:

- Within a single `ActivityTaskScheduled` event, retries do not produce new history events. The runtime re-dispatches the activity task with an incremented attempt counter.
- If the retry policy is exhausted or a non-retryable error occurs, the runtime issues `ActivityResolved` with the appropriate failure.

### Activity heartbeat timeout

Heartbeat processing lives entirely in the runtime. When heartbeats stop arriving within the configured `heartbeat_timeout`, the runtime issues `ActivityResolved` with a `TimedOut` resolution and `HEARTBEAT` timeout type. The kernel does not need to know about heartbeat intervals or deadlines.

### Activity heartbeat details in state

When an activity is retried after a heartbeat timeout, the last heartbeat details should be available to the retry attempt. This is a runtime concern, but `ActivityState` may eventually need a `last_heartbeat_details` field that the runtime updates outside the kernel's transition path.

## Concurrency limits

Temporal enforces default per-execution limits on pending entities (2,000 activities, 2,000 children, 30 Nexus operations, etc.).[^commands] Those limits are artifacts of Temporal's internal architecture: shard-level mutable state blob sizes, transfer queue fan-out pressure, and History Service memory budgets.

Tokeira does not adopt per-execution pending-entity ceilings. The DSQL-backed storage model with per-run fenced transactions, append-friendly side tables, and no shared mutable-state blobs does not share the constraints that motivated them. A workflow that legitimately needs 10,000 concurrent activities or 5,000 child workflows should not be artificially constrained by limits inherited from a different system.

The kernel is therefore limit-free. It will schedule as many activities, children, timers, external signals, Nexus operations, or pending updates as the workflow code requests. The kernel's job is correctness, not capacity planning.

This does not mean Tokeira has no resource controls. The distinction is between two different categories:

- **Per-execution pending-entity ceilings** (e.g., "no more than 2,000 activities per workflow"): Tokeira does not impose these. They are arbitrary and architecture-specific. Neither the kernel nor the runtime should enforce them.
- **System-level admission and backpressure** (e.g., rate limiting workflow starts per namespace, throttling dispatch when DSQL connection budget is exhausted, shedding load under memory pressure): These are legitimate operational controls that protect the platform as a whole. They belong in the runtime's admission control layer ([055-admission-control](055-admission-control.md)) and operate on aggregate resource metrics, not per-workflow entity counts.

The kernel does not accept a limits configuration and does not reject commands based on entity counts. The runtime does not reject individual workflow commands based on pending-entity counts either. If a workflow creates enough fan-out to stress the system, the response is backpressure on the workflow's task delivery and transaction throughput, not an artificial cap on what the workflow is allowed to express.

## Projection ops belong here

Projection logic does **not** mean projection storage belongs in the kernel. But the *meaning* of a projection mutation does belong here.

The kernel emits:

- `ProjectionOp::UpsertExecution` (on start, memo/search-attr changes)
- `ProjectionOp::CloseExecution` (on any terminal transition)

The projection plane consumes these ops and applies them to whatever storage backend it uses.

## Dispatch ops

The kernel emits `DispatchOp` values that tell the runtime what task delivery actions must follow from the committed transition. The kernel is ignorant of how delivery happens (sync match, sticky routing, durable backlog); it only declares intent.

Current dispatch ops:

- `EnqueueWorkflowTask { queue, logical_seq, sticky_preferred }`: a WFT needs to reach a worker.
- `EnqueueActivityTask { queue, activity_id, schedule_event_id, attempt }`: an activity task needs to reach a worker.
- `StartChildWorkflow { child_workflow_id, namespace_id, workflow_type, task_queue, input }`: the runtime should create a child run.
- `TerminateChild { child_workflow_id, child_run_id, reason }`: terminate a child per Parent Close Policy.
- `CancelChild { child_workflow_id, child_run_id, reason }`: cancel a child per Parent Close Policy.
- `SignalExternalWorkflow { target_workflow_id, target_run_id, signal_name, input }`: send a signal to another workflow.
- `RequestCancelExternalWorkflow { target_workflow_id, target_run_id }`: send a cancel request to another workflow.

Future dispatch ops:

- `ScheduleNexusOperation { ... }`: invoke a Nexus endpoint.
- `CancelNexusOperation { ... }`: cancel a pending Nexus operation.

The `QueueKey` in each dispatch op carries `namespace_id`, `task_queue`, `task_kind`, and placeholder `deployment`/`build_id` fields. These placeholders are the extension point for worker versioning and deployment-based routing.

## Implementation maturity

Not all commands and workflow commands described in this document are implemented yet. The following table tracks the current state:

| Command / Feature | Status |
|---|---|
| `Start` | Implemented (F1) |
| `Signal` | Implemented (F1) |
| `WorkflowTaskStarted` | Implemented (F1) |
| `WorkflowTaskCompleted` | Implemented (F1) |
| `WorkflowTaskFailed` | Implemented (F2) |
| `WorkflowTaskTimedOut` | Implemented (F2) |
| `ActivityResolved` | Implemented (F1) |
| `TimerDue` | Implemented (F1) |
| `Cancel` | Implemented (F3) |
| `Terminate` | Implemented (F3) |
| `WorkflowExecutionTimedOut` | Implemented (F4) |
| `ChildStartConfirmed` | Implemented (F5) |
| `ChildResolved` | Implemented (F5) |
| `ExternalSignalResolved` | Implemented (F6) |
| `ExternalCancelResolved` | Implemented (F6) |
| `UpdateExecutionOptions` | Implemented (F8) |
| `PauseWorkflow` | Implemented (F11) |
| `UnpauseWorkflow` | Implemented (F11) |
| `UpdateActivityOptions` | Implemented (F11) |
| `PauseActivity` | Implemented (F11) |
| `UnpauseActivity` | Implemented (F11) |
| `ResetActivity` | Implemented (F11) |
| `Update` | Implemented (F7) |
| `NexusOperationResolved` | Implemented (F9) |
| `Reset` | Implemented (F10) |
| `ScheduleActivity` (workflow cmd) | Implemented (F1) |
| `StartTimer` (workflow cmd) | Implemented (F1) |
| `CompleteWorkflow` (workflow cmd) | Implemented (F1) |
| `FailWorkflow` (workflow cmd) | Implemented (F1, enhanced F4 with retry metadata) |
| `CancelWorkflow` (workflow cmd) | Implemented (F3) |
| `ContinueAsNew` (workflow cmd) | Implemented (F4) |
| `RequestNewWorkflowTask` (workflow cmd) | Implemented (F1) |
| `UpsertMemo` (workflow cmd) | Implemented (F1) |
| `UpsertSearchAttributes` (workflow cmd) | Implemented (F1) |
| `StartChildWorkflow` (workflow cmd) | Implemented (F5) |
| `RequestCancelActivity` (workflow cmd) | Implemented (F3) |
| `CancelTimer` (workflow cmd) | Implemented (F3) |
| `SignalExternalWorkflow` (workflow cmd) | Implemented (F6) |
| `RequestCancelExternalWorkflow` (workflow cmd) | Implemented (F6) |
| `RecordMarker` (workflow cmd) | Implemented (F8) |
| `ScheduleNexusOperation` (workflow cmd) | Implemented (F9) |
| `CancelNexusOperation` (workflow cmd) | Implemented (F9) |
| `ProtocolMessage` (workflow cmd) | Implemented (F7) |
| `UpdateCompleted` (workflow cmd) | Implemented (F7) |
| `UpdateRejected` (workflow cmd) | Implemented (F7) |
| Request deduplication | Implemented (F1) |
| Sticky execution affinity | Implemented (F1) |
| Timeout configuration | Implemented (F1) |
| Retry policy recording | Implemented (F1, enhanced F4) |
| Activity timeout pass-through | Implemented (F1) |
| ActivityResolution TimedOut/Canceled | Implemented (F1) |
| WFT failure/timeout fencing | Implemented (F2) |
| WorkflowTaskFailedCause enum | Implemented (F2) |
| WorkflowTaskTimeoutType enum | Implemented (F2) |
| Open children tracking | Implemented (F5) |
| Parent Close Policy | Implemented (F5) |
| Pending external signals tracking | Implemented (F6) |
| Pending external cancel requests tracking | Implemented (F6) |
| Pending updates tracking | Implemented (F7) |
| Pending Nexus operations tracking | Implemented (F9) |
| Pause / unpause workflow lifecycle | Implemented (F11) |
| Activity pause / reset / mutable options | Implemented (F11) |

## Test strategy

The kernel is the easiest part of the system to test exhaustively.

### Golden transition tests

Load explicit run state + command → assert exact transition. These tests pin the kernel's behavior for each command and serve as regression guards.

### Property tests

Check invariants such as:

- event IDs are contiguous within a transition,
- transition sequence increments exactly once,
- there is at most one pending WFT after any transition,
- closed workflows do not schedule new activities or WFTs,
- `next_state.last_event_id` equals the last emitted event's ID,
- `next_state.transition_seq` equals `expected_seq + 1`,
- every `ActivityOp::Upsert` has a corresponding entry in `next_state.activities`,
- every `ActivityOp::Delete` has no corresponding entry in `next_state.activities`.

### Model-based tests

Drive a simplified reference machine and compare transition outcomes. This is particularly valuable for multi-step sequences (start → signal → WFT start → WFT complete with activities → activity resolve → WFT start → WFT complete with close).

### Serialization tests

Ensure kernel-produced transitions remain backward-compatible as internal DTOs evolve.

## Relationship to formal modeling

The kernel is where a later TLA+ or Stateright model will map most directly. The closer the kernel stays to "pure state machine," the easier it will be to prove or exhaustively search interesting invariants.

Key properties to model:

- **WFT uniqueness:** at most one pending WFT at any time.
- **WFT retry convergence:** a failed or timed-out WFT is always rescheduled; the workflow is never stuck.
- **Event ID monotonicity:** event IDs are strictly increasing within a run.
- **Terminal state absorption:** once a run is closed, no further transitions are possible.
- **Activity/timer lifecycle:** every open entity is eventually resolved or cleaned up on close.
- **Update lifecycle:** every accepted update is eventually completed or rejected.
- **Child lifecycle:** every initiated child is eventually resolved, and parent close policy is applied on parent termination.
- **External signal/cancel lifecycle:** every initiated external signal or cancel request is eventually confirmed or failed.
- **Nexus operation lifecycle:** every scheduled Nexus operation is eventually resolved.
- **Marker transparency:** markers do not affect kernel state; they are pure history entries.

## Review questions

1. Should `Transition` include a stronger typed outbox abstraction instead of separate `dispatch_ops` and `projection_ops`?
2. Should `ContinueAsNew` be expressed as one command that emits a linked successor start, or as a terminal event plus a runtime-generated start command? (This document currently specifies the latter.)
3. Do we want the kernel to assign final event IDs directly, or only event-count deltas while storage stamps final IDs? (This document currently specifies direct assignment.)
4. Should `Update` acceptance and WFT scheduling be atomic within a single `apply` call, or should acceptance be a separate transition from WFT scheduling?
5. Should `Terminate` clean up open children inline (emitting ops for each child), or should it emit a single "terminate all children" op and let the runtime handle fan-out?
6. ~~Should `Reset` be a kernel command or purely a runtime orchestration that composes `Terminate` + `Start`?~~ **Resolved:** Reset is a top-level kernel command (`Command::Reset`) with its own `apply_reset` handler. It emits a `WorkflowTaskFailed` event with `RESET_WORKFLOW` cause and reset metadata (`base_run_id`, `new_run_id`, `fork_event_id`), closes the run, and cleans up all entities. The runtime reads the committed event to create the successor.
7. Should `WorkflowTaskFailed` always reschedule the WFT, or should there be a maximum attempt count after which the workflow is considered stuck and requires operator intervention?
8. Should `RecordMarker` remain fully opaque to the kernel, or should the kernel understand local activity markers enough to track them as a distinct pending entity type?
9. Is Nexus support a near-term priority, or should it be deferred until the core command set (activities, timers, children, signals, updates) is fully implemented?
10. ~~Should `ProtocolMessage` be modeled as a first-class workflow command in the kernel, or should the kernel handle update ordering internally without exposing the protocol message abstraction?~~ **Resolved:** ProtocolMessage is a first-class workflow command that carries an `UpdateProtocolBody` inline. Its position in the command list determines where the update event lands in history.

## References

[^history-service]: Temporal History Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md  
[^continue-as-new]: Temporal Continue-As-New docs: https://docs.temporal.io/workflow-execution/continue-as-new  
[^update]: Temporal Workflow Update docs: https://docs.temporal.io/encyclopedia/workflow-message-passing  
[^cancel]: Temporal Cancellation docs: https://docs.temporal.io/develop/python/cancellation  
[^terminate]: Temporal community discussion on Cancel vs Terminate: https://community.temporal.io/t/what-exactly-happens-when-we-do-terminate-and-cancel-a-workflow/12203  
[^child-workflows]: Temporal Child Workflows docs: https://docs.temporal.io/child-workflows  
[^reset]: Temporal Workflow Reset: https://docs.temporal.io/develop/php/cancellation  
[^events]: Temporal Event reference: https://docs.temporal.io/references/events  
[^commands]: Temporal Commands reference: https://docs.temporal.io/references/commands  
[^markers]: Temporal MarkerRecorded event: https://docs.temporal.io/references/events#markerrecorded  
[^nexus]: Temporal Nexus: https://docs.temporal.io/nexus/operations
