# Design Document: Kernel Pause Workflow

## Overview

This design closes the gaps between the existing `apply_pause_workflow` /
`apply_unpause_workflow` kernel transitions and Temporal v1.31.0 workflow pause
behavior.

Verified Temporal behavior is intentionally simple:

- Pause sets workflow status to `Paused`, emits `WorkflowExecutionPaused`, and
  does not create a workflow task.
- Unpause sets workflow status to `Running`, emits `WorkflowExecutionUnpaused`,
  and creates a workflow task.
- Signals received while paused are written to history normally, but they do
  not schedule a workflow task.
- In-flight activities, timers, child workflow events, and other server-side
  events continue to be processed, but workflow-task scheduling is suppressed
  until unpause.
- Queries are rejected while the workflow is paused with status `Paused`.

The kernel remains pure. All new kernel logic is deterministic state
manipulation with no I/O, async, metrics, or network access.

## Architecture

```text
Edge
  pause/unpause gRPC handlers
  query rejection for paused workflows
  describe response maps pause status/info
        |
Runtime
  pause_workflow / unpause_workflow submit kernel commands
  post-commit dispatch uses existing broker path
        |
Kernel
  PauseWorkflow: status Paused, pause event, no WFT
  UnpauseWorkflow: status Running, unpause event, WFT
  Signal while paused: signal event, no WFT
  Server-side events while paused: record event/state, no WFT
        |
Projection
  ExecutionStatus = "Paused"
```

## Kernel Semantics

### Pause

`apply_pause_workflow` keeps the existing request-id-gated idempotency:

```rust
if state.status == ExecutionStatus::Paused {
    if state
        .pause_info
        .as_ref()
        .is_some_and(|info| info.request_id == req.request.request_id.0)
    {
        return Ok(TransitionBuilder::new(state, req.now).finish());
    }
    return Err(Reject::AlreadyPaused);
}
```

For a running workflow, pause emits `WorkflowExecutionPaused`, sets
`state.status = ExecutionStatus::Paused`, stores internal `PauseInfo` including
`request_id`, bumps `wft_stamp`, updates activity stamps as the existing kernel
does today, and emits `ProjectionOp::UpsertExecution` with paused status and
pause search-attribute state. It does not schedule a WFT.

`Reject::AlreadyPaused` remains in the kernel and maps to gRPC
`FAILED_PRECONDITION`.

### Unpause

`apply_unpause_workflow` keeps the existing precondition:

```rust
if state.status != ExecutionStatus::Paused {
    return Err(Reject::NotPaused);
}
```

For a paused workflow, unpause emits `WorkflowExecutionUnpaused`, sets
`state.status = ExecutionStatus::Running`, clears `pause_info`, bumps
`wft_stamp`, emits `ProjectionOp::UpsertExecution` with running status and an
updated standard status field, and schedules one WFT through
`builder.schedule_workflow_task()` if no WFT is already pending.

`Reject::NotPaused` remains in the kernel and maps to gRPC
`FAILED_PRECONDITION`.

### Signals While Paused

Paused signals use the normal signal history path:

```rust
builder.request_dedupe_ops.push(RequestDedupeOp {
    request_id: req.request.request_id.clone(),
});
builder.emit(HistoryEventKind::WorkflowExecutionSignaled {
    signal_name: req.signal_name,
    input: req.input,
    header: None,
    request_id: req.request.request_id.0,
    identity: req.request.caller_identity,
});
if builder.state.status != ExecutionStatus::Paused
    && builder.state.pending_workflow_task.is_none()
{
    builder.schedule_workflow_task();
}
```

This preserves the exact same signal metadata currently modeled by the normal
running signal path while preventing workflow code from processing the signal
until unpause creates a WFT. The paused path constructs the signal history event
the same way the running path constructs it, using the fields currently carried
by `SignalRequest` and `WorkflowExecutionSignaled`.

For `SignalWithStart` targeting an existing paused run, the start branch is not
taken; the existing run is signalled using the same paused signal behavior.

### WFT Suppression While Paused

WFT suppression is a kernel-wide invariant, not a per-handler convention. Any
kernel path that would normally wake workflow code must record its state/history
changes while paused, but it must not enqueue a WFT.

The current kernel call-site audit found WFT scheduling from start and
signal-with-start paths, signal, cancel, workflow-task completion follow-up,
activity resolution, child start/resolution, external signal/cancel resolution,
Nexus terminal resolution, workflow-task failure retry, timer due, query-task
scheduling, and workflow-command follow-up scheduling. New wakeup paths must use
the same scheduling helper.

The invariant is enforced at the `TransitionBuilder::schedule_workflow_task()`
chokepoint:

```rust
fn schedule_workflow_task(&mut self) {
    if self.state.status == ExecutionStatus::Paused {
        return;
    }
    // Existing scheduled-event, pending-WFT, and dispatch-op logic.
}
```

This central gate prevents current and future callers from accidentally waking a
paused workflow. `apply_unpause_workflow` is the only intentional exemption: it
sets status to `ExecutionStatus::Running` before calling
`schedule_workflow_task()`, so the helper naturally permits the unpause WFT.

Any direct `DispatchOp::EnqueueWorkflowTask` push must either be replaced with
`schedule_workflow_task()` or guarded by the same paused-state check. Direct
enqueue paths are reviewed in this spec because they bypass the central helper.

## Edge Behavior

### gRPC Handlers

The edge replaces the current placeholder unary handlers for
`PauseWorkflowExecution` and `UnpauseWorkflowExecution` with real handlers following the existing
translate-delegate-translate pattern.

The handlers validate namespace and workflow ID, resolve routing through the
standard router, build kernel `PauseWorkflowRequest` / `UnpauseWorkflowRequest`
values, and call the runtime adapter.

Expected mappings:

| Kernel/runtime error | gRPC status |
|---|---|
| `Reject::AlreadyPaused` | `FAILED_PRECONDITION` |
| `Reject::NotPaused` | `FAILED_PRECONDITION` |
| closed workflow | existing closed-workflow mapping |
| missing namespace/workflow ID | `INVALID_ARGUMENT` |
| missing execution | `NOT_FOUND` |

### Query Rejection

Query rejection is a runtime decision because the runtime already loads the run
before dispatching a query task. The edge does not load workflow state to infer
pause status.

The runtime query result type is extended with a rejection variant:

```rust
pub enum QueryResult {
    Completed { result: Payloads },
    Failed { message: String },
    Rejected { status: ExecutionStatus },
}
```

When `TokeiraRuntime::query_workflow` loads a run whose status is
`ExecutionStatus::Paused`, it returns
`QueryResult::Rejected { status: ExecutionStatus::Paused }` before publishing a
query task or scheduling a query WFT.

The edge translation maps `QueryResult::Rejected { status }` to
`QueryWorkflowResponse { query_rejected: Some(QueryRejected { status }) }`,
which becomes `WORKFLOW_EXECUTION_STATUS_PAUSED` for paused workflows.

### DescribeWorkflowExecution

The describe translation maps:

- `ExecutionStatus::Paused` to
  `WORKFLOW_EXECUTION_STATUS_PAUSED` (proto enum value 8).
- `PauseInfo` to `workflow_execution_info.pause_info`.
- `PauseInfo.pause_time` to `WorkflowExecutionPauseInfo.paused_time`.
- `PauseInfo.identity` and `PauseInfo.reason` to their matching proto fields.

The proto pause info does not expose request ID. The kernel keeps request ID
internally only for pause idempotency.

### Capability Surface

The vendored Temporal v1.31 proto exposes `workflow_pause` on
`NamespaceInfo.Capabilities`, not on `GetSystemInfoResponse.Capabilities`.
Tokeira should report namespace capability `workflow_pause = true` through the
namespace description path once pause/unpause handlers are implemented.

## Runtime Behavior

`TokeiraRuntime` exposes `pause_workflow` and `unpause_workflow` methods that
resolve the execution to a `RunKey`, submit `Command::PauseWorkflow` or
`Command::UnpauseWorkflow` to the owning lane, and return the commit result.

The existing post-commit path processes dispatch ops from unpause through the
standard broker path. No special broker bypass is required.

## Projection and Visibility

Pause and unpause projection updates use the standard execution status query
surface:

- Pause writes `ProjectionOp::UpsertExecution { status: ExecutionStatus::Paused, .. }`.
- Unpause writes `ProjectionOp::UpsertExecution { status: ExecutionStatus::Running, .. }`.
- Visibility filtering supports `ExecutionStatus = "Paused"` and
  `ExecutionStatus = "Running"` through the standard status field.

The visibility filter parser must accept `ExecutionStatus = "Paused"`. Rollup
and query label helpers must map `ExecutionStatus::Paused` to `"Paused"`.

## Data Models

### WorkflowState

Pause does not add any signal queueing state to `WorkflowState`; signal events
are written to history immediately. `PauseInfo` remains internal kernel state
and continues to include `request_id` for request-id-gated pause idempotency.

### Reject Enum

| Variant | Status |
|---|---|
| `AlreadyPaused` | Keep; map to `FAILED_PRECONDITION` |
| `NotPaused` | Keep; map to `FAILED_PRECONDITION` |

## Testing Strategy

### Property-Based Tests

The kernel is pure and deterministic, so pause behavior should be covered with
property tests:

- Signal while paused emits normal signal history and no WFT dispatch.
- Pause with the same request ID is a no-op success; pause with a different
  request ID returns `AlreadyPaused`.
- Unpause while not paused returns `NotPaused`.
- Any command that would normally schedule a WFT records history/state while
  paused but produces no WFT dispatch, except successful unpause after status is
  restored to running.
- Unpause schedules a WFT when no WFT is pending.

### Unit and Integration Tests

- Edge translation: proto → edge request → kernel command round trip.
- Edge validation: missing namespace/workflow ID returns `INVALID_ARGUMENT`.
- QueryWorkflow rejects paused executions with `QueryRejected` status `Paused`.
- Namespace capabilities include `workflow_pause: true`.
- DescribeWorkflowExecution maps paused status to proto value 8 and populates
  `workflow_execution_info.pause_info`.
- Visibility accepts `ExecutionStatus = "Paused"` and returns paused workflows
  through the standard status filter.
- Runtime adapter methods route through the standard submit path.

## Correctness Properties

### Property 1: Signal While Paused Records History Without WFT

For any workflow in `ExecutionStatus::Paused` and any valid signal, applying the
signal command SHALL emit one normal `WorkflowExecutionSignaled` history event,
record a request-dedupe op, preserve all signal metadata, and produce zero
`DispatchOp::EnqueueWorkflowTask` entries.

**Validates: Requirements 1.1, 1.2, 1.3, 1.4**

### Property 2: Pause Idempotency Is Request-ID-Gated

For any paused workflow, applying `PauseWorkflow` with the stored pause request
ID SHALL produce a no-op success transition. Applying `PauseWorkflow` with a
different request ID SHALL return `Reject::AlreadyPaused`.

**Validates: Requirements 2.1, 2.2**

### Property 3: Unpause Requires Paused State

For any non-paused open workflow, applying `UnpauseWorkflow` SHALL return
`Reject::NotPaused`.

**Validates: Requirement 2.3**

### Property 4: Paused Workflow Suppresses WFT on All Wakeup Paths

For any paused workflow and any command variant that would normally schedule a
WFT, excluding `UnpauseWorkflow`, the resulting transition SHALL contain zero
`DispatchOp::EnqueueWorkflowTask` entries while still recording the command's
state/history effects when the command is otherwise valid. The property text and
test implementation SHALL explicitly state that `UnpauseWorkflow` is excluded
because it transitions the run to `Running` before scheduling the wakeup WFT.
The generated command set SHALL cover the full command enum, including cancel
requests, external signal/cancel resolutions, Nexus terminal resolutions, timer
due, activity and child resolutions, query-task scheduling, and workflow-command
follow-up scheduling.

**Validates: Requirements 1.1, 7.1, 7.2, 7.3**

### Property 5: Unpause Wakes Workflow

For any paused workflow with no pending WFT, applying `UnpauseWorkflow` SHALL
emit `WorkflowExecutionUnpaused`, set status to `Running`, clear pause info,
write a running-status projection update, and enqueue one WFT through the
standard dispatch op.

**Validates: Requirements 1.5, 4.3, 6.3, 7.4**
