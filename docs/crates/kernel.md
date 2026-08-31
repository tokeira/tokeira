# tokeira-kernel

Pure deterministic workflow transition engine. Given a loaded run and a command,
the kernel derives the authoritative next state, history events, and explicit
post-transition effects.

## Where it sits

The kernel is the semantic core of the authoritative runtime and storage plane.
`tokeira-runtime` decides when and under which shard fence to invoke it;
`tokeira-storage` commits the resulting transition.

CHASM is a peer substrate in `tokeira-chasm`, not an extension of this crate.
Standalone activities therefore do not add I/O or component machinery to the
workflow kernel.

## Surface map

| Module | Contract |
|---|---|
| `state` | `WorkflowState`, `LoadedRun`, pending workflow/activity/timer/child/Nexus/update state |
| `command` | External and worker-authored inputs such as start, signal, workflow-task completion, timeouts, reset, and Nexus resolution |
| `event` | Durable `HistoryEvent` and `HistoryEventKind` values |
| `kernel` | `Kernel`, `BasicKernel`, rejection types, and history-prefix replay |
| `transition` | `Transition` plus derived dispatch, activity, timer, and request-deduplication operations |

The main entry point is `BasicKernel::apply`: it consumes a loaded state and one
command and returns either a complete transition or a typed rejection. Reset
materialization uses deterministic history-prefix replay to derive the successor
state.

## Invariants

- No I/O, async work, storage, networking, metrics, clocks, or nondeterministic
  inputs enter the kernel.
- A transition advances the run's sequence and appends ordered history without
  reusing event identifiers.
- Terminal workflow states reject further semantic mutation.
- Workflow-task, activity, timer, child-workflow, external-operation, update, and
  Nexus decisions are reflected in state and history together.
- Dispatch and timer operations are descriptions of derived work, not side
  effects executed by the kernel.

## It does not own

The crate does not load or persist runs, acquire shard leases, poll workers,
deliver tasks, evaluate wall-clock deadlines, expose RPCs, or materialize
visibility. Runtime and storage must preserve the transition's ordering and
fences.

## Pointers

- [Crate root](../../crates/tokeira-kernel/src/lib.rs)
- [Kernel-specific contract](../../crates/tokeira-kernel/AGENTS.md)
- [CHASM substrate](chasm.md)
- [Runtime](runtime.md)
- [Storage](storage.md)
