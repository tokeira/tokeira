# AGENTS — tokeira-kernel

Crate-local rules. The root `AGENTS.md` still applies; this refines it for the
kernel. On conflict, the stricter rule wins (here, that is almost always this file).

## The one boundary: the kernel is pure

`tokeira-kernel` is a deterministic state machine. It owns *semantic* correctness
and nothing else. The following are forbidden in this crate, with no exceptions:

- **No I/O, async, storage, network, time, or metrics.** No `async fn`, no `tokio`,
  no `std::time::now`, no logging side effects in transition logic. A transition is
  a pure function of `(state, command) -> (new_state, events, effects)`.
- **No side-effecting commands.** A `Command` records *what happened*; applying it
  derives the authoritative new state and the `transition` effects that downstream
  layers must honor. A command MUST NOT be a request to *perform* a side effect
  (persist a heartbeat, dispatch a task, write a queue). If you are tempted to add a
  command whose purpose is to make something happen elsewhere, it does not belong here.
- **No non-determinism.** Same `(state, command)` must always yield the same result,
  so replay reconstructs state exactly. No `HashMap` iteration that escapes into
  output ordering, no RNG, no wall clock.

## Where the forbidden thing belongs instead

- Durable persistence (including activity heartbeat details, dispatch progress) →
  `tokeira-runtime`, via a fenced `repo.commit_transition` (see that crate's
  `AGENTS.md`). The kernel may *author the history event*; it never *persists* it.
- Task dispatch / queue writes → `tokeira-runtime` (broker, publisher).
- Anything touching DSQL or the in-memory store → `tokeira-storage`.

Precedent: the activity-heartbeat work was explicitly kept out of the kernel for
exactly this reason — heartbeat details persist on `ActivityState` via a runtime
fenced commit, not a kernel command.

## Behaviour ground truth

Workflow-transition behaviour follows the targeted Temporal release (root §8):
`proto/upstream/` for wire shape, the local `../temporal` checkout at the
`TEMPORAL_SERVER_COMPAT` tag for runtime behaviour. Cite the source path + tag in a
comment when a transition decision is non-obvious.

## Reading order

`state.rs` (durable input) → `command.rs` (what happened) → `kernel.rs` (derives new
state) → `transition.rs` (effects downstream layers honor). New logic that does not
fit this shape is a signal it belongs in another crate.
