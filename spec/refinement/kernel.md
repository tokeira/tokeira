# Refinement: `tokeira-kernel`

## Problem

Tokeira has deliberately separated concerns:

- `tokeira-kernel` owns **semantic state transitions**,
- `tokeira-storage` owns **atomic durability**,
- `tokeira-runtime` owns **activation, routing, and delivery**,
- `tokeira-projection` owns **derived read models**.

That separation is good, but it creates a practical risk:

> people may look at a TLA+ spec and assume it models the whole system,
> or look at the Rust kernel and assume it already includes storage/runtime semantics.

Both would be mistakes.

We therefore need an explicit bridge that says:

- which specs are meant to refine `tokeira-kernel`,
- which parts of a spec belong elsewhere,
- how abstract spec variables correspond to Rust state.

## Goal

Create a readable refinement map between the first TLA+ specs and `tokeira-kernel`, so that contributors can answer three questions quickly:

1. **Does this behavior belong in the kernel?**
2. **Which spec should this code refine?**
3. **If I change the kernel, what spec work is required?**

## The key idea

The first TLA+ spec is **not** a model of DSQL, brokers, leases, ECS, or projections.
It is a model of the meaning of:

```rust
Kernel::apply(loaded, command) -> Result<Transition, Reject>
```

That is the semantic heart of Tokeira.

In other words:

- the spec describes **what transition is allowed**,
- the kernel is the **executable refinement** of that transition,
- storage/runtime/projection later refine how that semantic transition becomes durable and observable.

## What `tokeira-kernel` owns

The kernel owns all logic of the form:

> Given the current semantic run state and one authoritative command,
> is the command allowed, and if so, what semantic transition does it produce?

That includes:

- command enable/disable guards,
- state evolution,
- history event suffix generation,
- pending workflow-task evolution,
- activity/timer summary evolution,
- semantic close behavior,
- the decision to schedule a workflow task when an external event wakes the run.

It does **not** include:

- current-run pointer uniqueness,
- request dedupe lookup,
- bundle lease fencing,
- poller reservation handling,
- sticky matching policy,
- projector checkpoint movement,
- DSQL transaction boundaries.

## Which specs map directly to the kernel

### `00_execution_contract.tla`

This is the closest spec-to-kernel fit.

It should refine directly to:

- `Command`
- `WorkflowState` / `LoadedRun`
- `Transition`
- `Reject`

If the spec says an action is disabled, the kernel should reject it.
If the spec says an action is enabled, the kernel should produce the matching semantic delta.

### `10_history_authority.tla` (semantic half only)

The kernel contributes the **semantic write set**:

- history events,
- next semantic state,
- dedupe intent,
- activity/timer ops,
- dispatch ops,
- projection ops.

But the **atomicity** of that write set belongs to storage, not the kernel.

### `20_current_execution.tla` (semantic half only)

The kernel may express the semantic intent of starting a run or closing a run.
But the invariant

> at most one current open run for `(namespace, workflow_id)`

belongs to storage around the `current_execution` mapping.

## Which specs do **not** map directly to the kernel

These specs consume kernel outputs, but are not kernel specs:

- `30_bundle_lease.tla`
- `40_broker_reservations.tla`
- `70_projection_prefix.tla`
- later admission / archival / autoscaling specs

Those belong primarily to runtime, storage, or projection.

## Refinement map

The table below is intentionally simple.
It is not a machine-checked proof artifact.
It is a practical map for contributors.

| Spec concept | Meaning in the spec | Rust representation |
|---|---|---|
| `st` | The current semantic run state | `WorkflowState` inside `LoadedRun::Existing` |
| `history` | The committed semantic event sequence for the run | Persisted history plus `Transition.history_events` as the next suffix |
| `status` | Whether the run is absent, running, or closed | `WorkflowState.status` |
| `transitionSeq` | Monotonic semantic transition counter | `WorkflowState.transition_seq` |
| `lastEventId` | The last committed event id in the run history | `WorkflowState.last_event_id` |
| `nextWFTSeq` | The next logical workflow-task sequence to allocate | `WorkflowState.next_workflow_task_seq` |
| `pendingWFT` | The currently pending workflow task, if any | `WorkflowState.pending_workflow_task` |
| `activities` | The summary set of scheduled/open activities | `WorkflowState.activities` |
| `timers` | The summary set of armed timers | `WorkflowState.timers` |
| `Start` | Start a new run | `Command::Start` |
| `Signal` | Record an external signal | `Command::Signal` |
| `WorkflowTaskStarted` | Mark a pending workflow task as started | `Command::WorkflowTaskStarted` |
| `WorkflowTaskCompleted` | Apply workflow-code commands and clear the started task | `Command::WorkflowTaskCompleted` |
| `ActivityResolved` | Reflect an activity result back into the run | `Command::ActivityResolved` |
| `TimerDue` | Reflect a due timer into the run | `Command::TimerDue` |
| `Reject` | Action is disabled in this state | `Err(Reject::...)` |
| `next state` | The semantic state after one command | `Transition.next_state` |
| `event suffix` | The semantic events added by the transition | `Transition.history_events` |

## Important mismatches to remember

The spec is intentionally smaller than the Rust state.
That is okay.

For example, the first spec does **not** model:

- memo values,
- search attributes,
- worker identity details,
- sticky TTLs,
- request contexts,
- payload contents.

Those fields still exist in Rust, but they are not central to the first semantic contract.

A good rule is:

> if omitting a field would not change whether a command is allowed or what semantic history suffix it produces, the field probably does not belong in the first spec.

## How to use this when changing the kernel

### If you add a new command

You should usually:

1. add or update a spec action,
2. update this refinement map,
3. add Rust tests that mirror the spec scenario.

### If you change a guard

Example: a command that was previously allowed is now rejected in some state.

You should:

1. update the spec action guard,
2. update or add an invariant if needed,
3. update the Rust `Reject` behavior.

### If you change only storage/runtime mechanics

Example: changing how broker reservations are persisted.

You should **not** edit `00_execution_contract.tla` unless the semantic transition relation changed.
You probably want a later spec instead.

## Rules of thumb

### Rule 1: keep the kernel spec-shaped

The kernel API should stay close to the spec vocabulary.
If a command or state field becomes impossible to explain in spec language, the boundary may be drifting.

### Rule 2: keep storage/runtime concerns out of the kernel spec

If a question starts with:

- who owns the bundle,
- which poller got the task,
- whether the write retried,
- whether the projector checkpoint moved,

then you are probably outside `00_execution_contract`.

### Rule 3: every correctness bug should land somewhere

A bug found in review should result in at least one of:

- a new spec invariant,
- a spec guard update,
- a new refinement note here,
- a new Rust test derived from a spec scenario.

## TODOs for the next refinement docs

### TODO(spec)
Add `refinement/history_authority.md` once the storage spec exists.

### TODO(spec)
Add `refinement/runtime.md` for bundle ownership, lane activation, and broker reservations.

### TODO(spec)
Add trace-based tests that take a small action sequence from the spec and assert the kernel produces the matching abstract state.

### TODO(spec)
Write down how `ContinueAsNew` should split between kernel semantics and storage enforcement.

## Bottom line

The first TLA+ spec is **not** the whole of Tokeira.
It is the specification of the semantic core that `tokeira-kernel` is supposed to implement.

That is exactly why it is worth writing.
