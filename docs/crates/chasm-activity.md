# tokeira-chasm-activity

Pure standalone-activity state machine built as a CHASM component. It defines
the activity's durable state, legal transitions, retry and timeout decisions,
staged tasks, validation, and visibility contribution.

## Where it sits

The crate is an application state machine in the authoritative runtime and
storage plane. `tokeira-runtime` drives it through the CHASM engine, while the
edge translates Activity Execution RPCs through its activity bridge.

## Surface map

| Module | Contract |
|---|---|
| `component` | `ActivityExecution`, `ActivityLibrary`, visibility reconstruction |
| `state` | `ActivityState`, `ActivityStatus`, lifecycle mapping |
| `statemachine` | `ActivityEvent`, `TimeoutType`, legal target transitions |
| `validator` | `ActivityRequest`, timeout normalization and request validation |
| `backoff` and `retry` | Exponential interval calculation and `RetryOutcome` |
| `timeouts` | Due timeout and next deadline derived from durable state |
| `tasks` | Dispatch side effect plus heartbeat, schedule-to-close, schedule-to-start, and start-to-close timers |
| `config` | `ActivityConfig` constants used by the archetype |

## Contracts

- The state machine and its helpers are deterministic and side-effect-free.
- Illegal lifecycle transitions are rejected rather than repaired implicitly.
- Retry decisions combine the retry policy, failure, attempt, and remaining
  schedule-to-close time.
- Timer validators decide whether a staged task is still valid against current
  component state.
- The component uses one data field containing the complete `ActivityState`
  message; the CHASM substrate still supplies node, clock, and outbox semantics.
- Visibility can be rebuilt from authoritative activity state.

## It does not own

The crate contains no CHASM engine internals, storage access, broker delivery,
wall-clock scanner, or RPC handling. Runtime executes its transitions and
tasks; storage commits them; edge owns wire translation.

## Pointers

- [Crate root](../../crates/tokeira-chasm-activity/src/lib.rs)
- [CHASM substrate](chasm.md)
- [Runtime CHASM facade](../../crates/tokeira-runtime/src/chasm/mod.rs)
- [Edge activity bridge](../../crates/tokeira-edge/src/chasm_activity.rs)
