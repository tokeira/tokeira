# 020 Kernel

**Status:** draft for architecture review  
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

Temporal’s own architecture docs show that request handling for a workflow execution ultimately reduces to a state transition that appends history, updates mutable state, and creates follow-on tasks.[^history-service] Tokeira isolates that logic into its own crate.

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

## Kernel inputs

The kernel should only see data that is already part of the deterministic transition boundary:

- run identity,
- current status,
- last event ID,
- current transition sequence,
- pending workflow task summary,
- open activities and timers relevant to the command,
- dedupe hints already loaded by storage/runtime,
- the command payload itself.

The kernel should **not** call the clock, the RNG, the database, or network services directly. If a timestamp or deadline matters, runtime/storage should pass it in explicitly as data.

## Command taxonomy

The kernel command set should match workflow semantics, not transport surfaces.

A good starting set is:

- `Start`
- `Signal`
- `Update`
- `Cancel`
- `Terminate`
- `WorkflowTaskStarted`
- `WorkflowTaskCompleted`
- `ActivityResolved`
- `TimerDue`
- `ChildResolved`
- `ContinueAsNew`
- `Reset` (eventually)

The important distinction is that commands are **semantic**. For example, `WorkflowTaskCompleted` is not just an RPC. It means “the worker holding this exact task token completed the currently started workflow task and proposes these workflow commands.”

## Transition shape

The transition should be explicit and boring:

```rust
pub struct Transition {
    pub expected_seq: TransitionSeq,
    pub history_events: SmallVec<[HistoryEvent; 8]>,
    pub hot_patch: WorkflowHotPatch,
    pub activity_ops: SmallVec<[ActivityOp; 4]>,
    pub timer_ops: SmallVec<[TimerOp; 4]>,
    pub dispatch_ops: SmallVec<[DispatchOp; 4]>,
    pub projection_ops: SmallVec<[ProjectionOp; 8]>,
}
```

This shape is powerful because it keeps the kernel ignorant of storage layout while still telling storage exactly what must happen.

## Event IDs and transition sequences

Tokeira should maintain **two** monotonic counters per run:

### Event ID

This is the user-visible position in workflow history.

### Transition sequence

This is the internal fence/checkpoint number for committed state transitions.

Why keep both?

- Event IDs are about workflow semantics and replay.
- Transition sequence is about internal correctness, task tokens, idempotency, and projection replay.

A single transition may append multiple history events but only increment transition sequence once.

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
- which projection mutations follow from the state change.

## What the kernel should not decide

The kernel should **not** decide:

- which lane hosts the actor,
- which poller gets the task,
- whether sync match or backlog is used,
- how DSQL retries a conflict,
- how a projection sink stores its row.

## Example: signal coalescing

The signal path is an important example of kernel behavior.

If the run is open and receives a signal:

1. append `WorkflowExecutionSignaled`,
2. if no WFT is pending, schedule one,
3. if a WFT is already pending, do **not** create a second one.

That logic belongs in the kernel because it is semantic, not transport-dependent.

## Example: workflow task completion

On `WorkflowTaskCompleted`:

1. validate token against pending WFT,
2. append `WorkflowTaskCompleted`,
3. apply worker-issued workflow commands in order,
4. update activity/timer/open-child state,
5. decide whether another WFT must be scheduled,
6. emit derived dispatch and projection operations.

The kernel should return a `Reject` if the token is stale or the command list is impossible.

## Continue-As-New

Temporal documents Continue-As-New as a way to checkpoint workflow state into a new run with the same Workflow ID but a fresh history and Run ID.[^continue-as-new] That fits naturally into the kernel:

- close the current run with a terminal-but-linked event,
- emit a start transition for the successor run,
- carry over appropriate memo/search attributes / arguments / chain metadata.

The kernel should define the semantics; storage/runtime should decide whether it is physically committed as one or two linked transactions.

## Projection ops belong here

Projection logic does **not** mean projection storage belongs in the kernel. But the *meaning* of a projection mutation does belong here.

For example, if a workflow closes successfully, the kernel should emit something like:

- `ProjectionOp::UpsertExecution`
- `ProjectionOp::CloseExecution`
- `ProjectionOp::SetSearchAttr { ... }` if attributes changed

That keeps the semantic mapping from workflow state to projection deltas in one place.

## Test strategy

The kernel is the easiest part of the system to test exhaustively.

Recommended layers:

### Golden transition tests

Load explicit run state + command -> assert exact transition.

### Property tests

Check invariants such as:

- event IDs are contiguous,
- transition sequence increments once,
- there is at most one pending WFT,
- closed workflows do not schedule new activities.

### Model-based tests

Drive a simplified reference machine and compare transition outcomes.

### Serialization tests

Ensure kernel-produced transitions remain backward-compatible as internal DTOs evolve.

## Relationship to formal modeling

The kernel is where a later TLA+ or Stateright model will map most directly. The closer the kernel stays to “pure state machine,” the easier it will be to prove or exhaustively search interesting invariants.

## Review questions

1. Should `Transition` include a stronger typed outbox abstraction instead of separate `dispatch_ops` and `projection_ops`?
2. Should `ContinueAsNew` be expressed as one command that emits a linked successor start, or as a terminal event plus a runtime-generated start command?
3. Do we want the kernel to assign final event IDs directly, or only event-count deltas while storage stamps final IDs?

## References

[^history-service]: Temporal History Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md  
[^continue-as-new]: Temporal Continue-As-New docs: https://docs.temporal.io/workflow-execution/continue-as-new
