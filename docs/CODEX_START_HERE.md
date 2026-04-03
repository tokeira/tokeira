# Codex start here

This document is the intended entry point for machine-assisted contributions.

## The safest places to contribute first

### `tokeira-kernel`
Good first work:
- implement child workflow commands,
- add update handling,
- add continue-as-new,
- add retry policy modelling,
- extend timer and activity resolution semantics.

Why this is safe:
- the kernel is pure,
- the input/output surface is small,
- regressions are easy to catch with deterministic tests.

### `tokeira-storage::memory`
Good first work:
- improve dev-store fidelity,
- add sweep queries for due timers and dispatchable activity tasks,
- add optimistic-concurrency style conflict injection for tests.

Why this is safe:
- it does not commit us to a production database implementation,
- it helps runtime tests and examples.

### `tokeira-runtime`
Good first work:
- improve broker fairness,
- add a durable backlog abstraction,
- implement timer and activity pumps,
- record activity task heartbeat
- add work admission and backpressure policies.

Why this is safe:
- the runtime already depends on the small kernel/storage seams,
- many enhancements can be layered without changing the kernel.

### `tokeira-projection`
Good first work:
- add page tokens,
- implement richer filtering,
- add rollups,
- add SQL planning types without binding to a concrete SQL driver.

Why this is safe:
- projection is intentionally outside the correctness path.

## Invariants to preserve

- Never let transport or storage details leak into the kernel.
- Never make the projection path authoritative.
- Never let pollers or waiters become durable correctness objects.
- Never assume a lane owns a run forever.
- Never make inactivity expensive.

## TODO style

The codebase deliberately uses rich TODO comments such as:

- `TODO(correctness): ...`
- `TODO(perf): ...`
- `TODO(storage): ...`
- `TODO(edge): ...`
- `TODO(ops): ...`

Please extend that convention instead of adding generic TODOs with no intent.
