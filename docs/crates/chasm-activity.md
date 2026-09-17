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
| `tasks` | Dispatch side effect plus heartbeat, schedule-to-close, schedule-to-start, and start-to-close timers; the callback delivery task and its retry timer |
| `callbacks` | `CallbackSpec`, `CallbackTarget`, `CallbackAttemptOutcome`, delivery-failure classification |
| `config` | `ActivityConfig` constants used by the archetype |

## Completion callbacks

An activity may carry callbacks attached at start, delivered when it closes.
State lives in the activity root itself rather than in a separate component
(`ActivityCallback`, in attach order, state values 0–5 matching
`chasm/lib/callback/proto/v1/message.proto @ v1.32.0`). A target is either
`Internal` — another component in the same engine, applied in process — or
`Nexus`, delivered over HTTP by an edge executor.

Two rules are worth stating on their own.

- **Delivery does not change the outcome.** Status and close time are the
  activity's public result and are settled independently of whether a callback
  has been delivered.
- **A terminal activity stays Running until its callbacks settle.**
  `lifecycle_of` reports `Running` while any callback is unsettled, so the
  Running-execution rebuild scan keeps finding the activity and can re-deliver
  after a restart. Without callbacks it is exactly `lifecycle_for`.

Callbacks are gated: `ActivityConfig::enable_callbacks` is off by default,
matching upstream's `activity.enableCallbacks` at `v1.32.0`. Attachment is
bounded by `max_callbacks_per_execution`, default 2000.

## Version target

A request may name an exact worker release. The target rides the staged dispatch
as `DeploymentVersionTarget` (deployment name and build id) and reaches the edge,
where it becomes the routing key a poller must match. The library itself only
carries and validates it; admission belongs to the edge.

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
