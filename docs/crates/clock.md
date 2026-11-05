# tokeira-clock

The wall clock the engine reads the current time from.

## Where it sits

This is a cross-cutting leaf crate whose only dependency is `time`. Storage,
the runtime, the edge and the engine depend on it. The kernel and
`tokeira-types` do not: the kernel takes time from the commands it is given,
and leaving this crate out of its dependencies means it has no clock to read.

## Surface map

| Item | Role |
|---|---|
| `Clock` | A source of the current UTC time, shared as `Arc<dyn Clock>` |
| `SystemClock` | The host's UTC clock, which production uses everywhere |
| `system_clock()` | A shared `SystemClock`, for constructors that default to it |

## Contracts

- A deadline and the check against it read the same clock. Code that stamps a
  deadline and code that judges it hold the same `Clock`.
- Production uses `SystemClock` everywhere. A constructor that takes no clock
  uses it.
- Times are `time::OffsetDateTime`, the type every stored deadline and every
  kernel command carries.
- A clock makes no promise that time moves forward between two readings.

## Who reads it

One node holds one clock, and everything below reads that clock.

- **Storage.** `InMemoryStore::with_clock` and
  `InMemoryWorkerComputeRepository::with_clock` judge expiries, bundle lease
  expiry among them, and stamp reset successors. The DSQL stores are given
  `SystemClock`.
- **Runtime.** `TokeiraRuntime` takes the clock when it is built and passes it
  to the lanes, the broker, the publishers, the timer and timeout scanners,
  heartbeat maintenance, speculative workflow-task timeouts, the deployment
  registry and worker compute. `deadline::sleep_until` takes the clock that
  stamped the deadline it waits for.
- **Edge.** `EdgeInterceptors::with_clock` sets the edge's clock. Every request
  stamps its `received_at` and its command times from it, and so does
  `NexusHttpHandler::with_clock`.
- **Engine.** `TokeiradHandle::start_in_memory_with_transport` takes a clock
  and gives it to the node's runtime, edge and store handle. The CHASM plane
  reads it too, unless an embedder sets `EngineBuilder::clock`. Every other
  entry point uses `SystemClock`.

## What stays on the host's clock

These read `OffsetDateTime::now_utc()` directly. In each case the time is
stamped and judged on host clocks only, or never judged at all:

- Fairness snapshot and adjustment times, which are recorded and never compared
  with a deadline.
- Membership budgets. The controller stamps `valid_until`, and the node judges
  it, each on its own host's clock.
- Peer tokens. The signing and verifying nodes both read their host's clock.
- Health `observed_at`, Nexus endpoint timestamps and task-queue config
  `update_time`, which are reported and never judged.
- Cron schedule validation at the edge, and log throttling for run growth.

The placement controller stamps node heartbeats with `std::time::Instant`.
Nothing judges them yet: no caller expires a node's grace period, so they can
stay on the host's clock until something does.

## It does not own

The crate does not schedule anything, sleep, or measure elapsed time.
Monotonic waits and durations use tokio's clock, which a simulation drives on
its own. The crate does not decide what is due; the code holding the clock
does.

## Pointers

- [Crate root](../../crates/tokeira-clock/src/lib.rs)
- [Storage](storage.md)
- [Runtime](runtime.md)
- [Edge](edge.md)
