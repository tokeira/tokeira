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
| CHASM | `ChasmEngine`, `TypedEngine`, timer sweeper, repair scanners, visibility adapter |
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
