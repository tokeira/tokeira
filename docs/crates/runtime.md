# tokeira-runtime

Lane-based orchestration for workflow and CHASM execution. The runtime is where
pure transition semantics meet ownership, scheduling, clocks, durable commits,
and derived delivery.

## Where it sits

The crate belongs to the authoritative runtime and storage plane. It serializes
work for an execution, invokes the appropriate pure state machine, persists the
result under a fence, and only then publishes derived effects.

## Execution surfaces

| Area | Representative contracts |
|---|---|
| Workflow lanes | `TokeiraRuntime`, lane executors, run actors, mailbox coalescing, bounded OCC retry |
| CHASM | `ChasmEngine`, `TypedEngine`, `DispatchMultiplexer`, `ChasmTimerSweeper`, `OutboxRebuildScanner`, repair scanners, visibility adapter |
| Delivery | `InMemoryBroker`, `InMemoryActivityBroker`, durable backlog and drain paths, dispatch publisher |
| Scheduling | Native schedule store and engine, overlap policies, backfill, cron and next-time evaluation |
| Time | Workflow, workflow-task, activity, heartbeat, timer, Nexus, callback, and speculative-timer scanners |
| Ownership | Shard membership, bundle leases, epochs, recovery, shutdown coordination |
| Worker control | Task-queue configuration, worker registry, deployment routing, compute control, fairness and rate limits |
| Interaction | Queries, buffered queries, updates, batch operations, Nexus dispatch and HTTP completion |

## Commit contract

For workflow commands, a lane loads the run, invokes `tokeira-kernel`, and calls
`RunRepository::commit_transition` with the expected transition sequence and
shard epoch. A conflict reloads state and recomputes the transition. CHASM uses
the parallel `ChasmEngine` and `ChasmNodeRepository` CAS-fenced node batch.

Only a successful commit can publish broker work, timers, visibility, or other
side effects. Queues are disposable; the authoritative transition log and
durable backlog provide recovery.

## CHASM side effects

A committed transition stages tasks in the node itself; dispatching them is a
derived effect that runs afterwards, never as part of the decision.

`DispatchMultiplexer` routes a staged side-effect task to the `SideEffectExecutor`
registered for its task type. Registration is late-bound, so bootstrap can build
the multiplexer, then the engine, then register every executor before anything is
served. Executors hold a weak engine handle — a stopped engine is a logged no-op —
and an unknown task type simply stays in the durable outbox for the next pass.

`ChasmEngine::apply_side_effect_outcome` is the primitive an executor calls with a
result. It applies the outcome only while the exact side-effect task is still
held, and commits the component bytes and the task's removal under one fence, so a
duplicate or late delivery is inert. It reports `Applied`, `NotHeld` when the task
has already gone, or `ExecutionMissing`. A conflict reloads and reruns the pure
handler within the configured bound; a handler error persists nothing.

`OutboxRebuildScanner` re-derives pending effects from committed state — at start
and on a sweep thereafter — so an effect lost to a restart comes back without
operator action. `ChasmTimerSweeper` does the same for armed timers, evaluating
due deadlines from durable state through the engine's configured clock.

## Delivery and schedules

`InMemoryBroker` carries workflow and query work, while
`InMemoryActivityBroker` carries activity work. Both deduplicate logical work
and keep pollers process-local. Work that must outlive live pollers moves through
the durable backlog and scanner paths.

Schedules are a native runtime engine. Schedule state, overlap bookkeeping, and
time evaluation produce ordinary workflow starts; the edge only translates the
public Schedule APIs.

## It does not own

The runtime does not define wire behaviour, pure workflow or component rules,
physical storage schemas, or visibility query semantics. It also is not the
server process: `tokeira-engine` composes runtime services and transports.

## Pointers

- [Crate root](../../crates/tokeira-runtime/src/lib.rs)
- [Runtime-specific contract](../../crates/tokeira-runtime/AGENTS.md)
- [Kernel](kernel.md)
- [CHASM](chasm.md)
- [Storage](storage.md)
- [Engine facade](engine.md)
