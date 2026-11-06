# 060 Connection Management

**Status:** accepted  
**Related docs:** [050-dsql-storage](050-dsql-storage.md), [055-admission-control](055-admission-control.md), [030-runtime-lanes](030-runtime-lanes.md)

## Purpose

Aurora DSQL connection management is a correctness and operability boundary. DSQL has a cluster-wide new-connection rate limit, a cluster-wide active connection limit, and IAM-authenticated sessions with finite safe lifetimes. Tokeira therefore treats the DSQL reservoir as the sole owner of physical database connections. There is no sqlx `PgPool` in the runtime path.

The design goal is simple: commits should either get an already-safe connection or receive immediate storage backpressure. A request should not trigger an unbounded connection-open cascade.

## Reservoir Ownership

The storage crate owns a `Reservoir` per `DsqlStore`. The reservoir owns raw `PgConnection` values created through `aurora_dsql_sqlx_connector::connection::connect_with`. The connector mints an IAM token for each connection as it connects; Tokeira never caches tokens and does not instrument token refresh directly.

The AWS credentials that sign the tokens are a different matter. `ConnectionFactory` resolves them once through the default credentials chain and reuses them until five minutes before they expire. Left to itself, the connector would build a new credentials chain for every connection, and its signer bypasses the SDK's credential cache, so a refill of fifty connections would fetch credentials fifty times from the ECS credentials endpoint or STS.

Every connect is bounded by a 30-second timeout. Neither sqlx nor the connector bounds one, and a peer that accepts TCP and never answers would otherwise hold a creation slot forever.

Connection flow:

```mermaid
flowchart LR
    A["Refiller"] --> B["DynamoDB slot block"]
    B --> C["DynamoDB rate bucket"]
    C --> D["ConnectionFactory"]
    D --> E["Ready channel"]
    E --> F["DsqlPermit"]
    F --> G["Return processor"]
    G --> E
    F --> H["Retire"]
    G --> H
    I["Expiry scanner"] --> H
```

The reservoir has three background tasks:

- `Refiller`: reserves capacity, waits for a distributed rate token, creates a raw connection, and places it in the ready channel. Up to eight creations run at once, so refilling fifty connections after a mass retirement takes a handful of connect latencies rather than fifty. Creations still running count against the target, so the refiller never overshoots it. Consecutive failures back off exponentially, from 250 ms doubling to 10 seconds with jitter, so nodes refused together retry apart; the first success ends the backoff at once.
- `Expiry scanner`: scans at most half of the target ready channel per pass and retires connections inside the guard window.
- `Return processor`: accepts returned permits and validates only lifetime plus the bad and broken flags. It performs no network ping and never waits for room: the ready channel holds twice the target, which is more than the refiller and every admitted caller can fill together.

## Admission Path

Every storage operation first acquires a class permit from `ClassBudgets`, then performs a non-blocking reservoir checkout. If the class semaphore is saturated, the operation queues at the class boundary. If the reservoir is empty after admission, the class permit is dropped and `ReservoirError::Empty` is returned immediately.

A class permit travels with its connection. When an operation finishes, its permit goes to the return processor along with the connection and returns to the class only once the connection is back in the ready channel or retired. If the permit came back first, the next caller of that class could be admitted while the connection it needs is still in the return queue, and find the reservoir empty although the class budgets were respected.

An empty reservoir reaches the client as `RESOURCE_EXHAUSTED`, with cause `RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT` and scope `RESOURCE_EXHAUSTED_SCOPE_SYSTEM`: the director marks the error `PersistenceCapacityExhausted`, and the frontend answers it the way Temporal answers a persistence rate limit (`common/persistence/persistence_rate_limited_clients.go` @ v1.31.0). SDKs retry it with backoff.

Default classes match the runtime `DbClass` enum:

- `control`
- `commit`
- `read`
- `projection`
- `maintenance`

The default allocation is derived from the target ready count and preserves at least one permit per class. Commit gets the largest share because the transition log is the authoritative write path.

## Broken Connections

A connection whose transport failed, or whose session the server ended with a FATAL error, must not reach another caller, but nothing on the connection says so afterwards: sqlx has no synchronous health check, and the return path does no network I/O. So every storage operation runs inside `in_operation`, which records each connection the operation checks out. When the operation fails with an IO, TLS or protocol error, or a FATAL error response, every recorded connection is flagged broken. The return processor, checkout and the expiry scanner each retire a flagged connection when they meet it, and the refiller replaces it.

The flag is set once the operation has failed, after its permit has returned the connection, so a caller that checks the connection out in that short window fails on it the same way and flags it again. An operation that held more than one connection retires them all; such failures are rare, and a spare reconnect costs less than handing the next caller a dead connection.

## Distributed Rate Bucket

Coordination has two modes, selected by the deployment shape rather than configuration: served multi-node `tokeirad` coordinates through the DynamoDB-backed machinery below; a single-owner embedded engine coordinates process-locally (see [Embedded Coordination](#embedded-coordination)).

DSQL connection creation is limited cluster-wide. A local token bucket per process would only move the thundering-herd problem from one process to many processes, so Tokeira uses a DynamoDB-backed token bucket.

The table is `{project}-dsql-rate-limiter` with partition key `pk` and TTL attribute `ttl_epoch`. The global bucket row stores tokens and the last refill timestamp. Refiller tasks use consistent reads and conditional writes. Conditional-write conflicts are expected and treated as retry pressure, not as hard errors.

The required acquisition order is:

1. acquire the local in-flight creation semaphore,
2. reserve one slot through the slot block manager,
3. wait for the distributed token bucket,
4. create the raw DSQL connection.

If any step after slot reservation fails, the slot is released before retry/backoff.

## Slot Block Manager

Active connection budget is coordinated through `{project}-dsql-conn-lease`, also keyed by `pk` with TTL on `ttl_epoch`. Nodes acquire blocks of slots with conditional writes and renew those blocks periodically. A connection that is successfully created consumes exactly one reserved slot until it is retired or discarded.

The invariant is strict: every path that drops a created connection releases exactly one slot.

Slot release happens when:

- a connection creation fails or times out,
- refiller fails to place a created connection in the ready channel,
- expiry scanner retires a ready connection,
- expiry scanner cannot requeue a scanned connection,
- return processor discards a bad, broken or guard-window connection,
- checkout or the expiry scanner retires a broken connection,
- return processor cannot place a returned connection back in the ready channel,
- `DsqlPermit::drop` discards a connection that exceeded its lifetime,
- `DsqlPermit::drop` cannot send the connection to the return processor.

If renewal loses a block because another node claimed it, the manager removes the block from local ownership, reduces total slot capacity, emits `tokeira_dsql_slot_block_lost_total`, and continues. Existing in-flight connections are not killed; refill naturally stops if used slots exceed remaining capacity.

## Embedded Coordination

A single-owner embedded engine has no peer processes to coordinate with, so it
constructs no DynamoDB resources. `Engine::embedded()` installs a process-local
coordinator (`ProcessLocalConnectionCoordinator`,
`crates/tokeira-storage/src/dsql/connection_coordinator.rs`): a monotonic token
bucket admits connection creation and an atomic slot budget caps active
connections, behind the same acquire-slot-then-token order the distributed path
uses. The module is deliberately absent from the distributed `tokeirad` path —
the reservoir, rate limiter, and slot-block manager above are unchanged by it.

This applies to every embedded storage mode: in-memory, managed DSQL, and
adopted DSQL. The embedded engine is the sole owner of its cluster's connection
budget, so process-local admission is the whole coordination problem.

## Lifetime Rules

The reservoir uses conservative internal constants:

- target ready connections: 50
- in-flight connection creations: 8
- base lifetime: 10 minutes
- lifetime jitter: up to 2 minutes
- guard window: 45 seconds
- scan interval: 1 second
- scan budget: 25 entries per pass
- warmup timeout: 30 seconds
- connect timeout: 30 seconds
- refill backoff: 250 ms, doubling per consecutive failure to 10 seconds
- ready channel capacity: twice the target ready count

The safety invariant is:

```text
base_lifetime + lifetime_jitter + guard_window < 15 minutes
```

That ensures the reservoir retires idle/returned connections before they can be handed to new work too close to the DSQL token/session safety boundary. Jitter prevents synchronized mass retirement.

## Schema Commands

Schema setup/status commands do not use the reservoir. They create one raw admin `PgConnection` through `ConnectionFactory` and pass it to the migration runner. This keeps schema operations simple and avoids warming a runtime reservoir for one-off CLI work.

## Metrics

Connection metrics use existing `tokeira_dsql_pool_*` names where a signal already existed. Important signals include:

- `tokeira_dsql_pool_connections_total`
- `tokeira_dsql_pool_checkout_duration_seconds`
- `tokeira_dsql_pool_empty_reservoir_total`
- `tokeira_dsql_pool_connections_created_total`
- `tokeira_dsql_pool_connections_retired_total`
- `tokeira_dsql_pool_connections_returned_total`
- `tokeira_dsql_pool_rate_limiter_tokens`
- `tokeira_dsql_pool_rate_limiter_rate`
- `tokeira_dsql_class_permit_wait_duration_seconds`
- `tokeira_dsql_connection_error_total`
- `tokeira_dsql_slot_blocks_owned`
- `tokeira_dsql_slot_block_lost_total`

These metrics are emitted unconditionally through the `metrics` crate. Operators should read empty-reservoir rate as immediate storage backpressure and slot-block loss as reduced node capacity.

What a connection is used for is counted in round trips. `tokeira_dsql_operation_round_trips` records, for each DSQL operation, the statements it sent plus the BEGIN of each transaction, one sample per transaction it ended, labelled by operation, by the command or request that caused it, and by whether the transaction committed or rolled back. The process's tracing subscriber counts them from SQLx's statement events, whatever the log filter (`tokeira_observability::statement_accounting`). `tokeira_dsql_unattributed_statements_total` counts statements sent outside every operation and should stay at zero. A commit's latency is mostly its round trips, so a rise in a commit's samples is a rise in its latency, and `rolled_back` samples are round trips spent on attempts that lost.

## Shutdown

The reservoir aborts its background tasks on drop. It also exposes explicit shutdown paths through `Reservoir::shutdown`, `DsqlConnectionDirector::shutdown`, and `DsqlStore::shutdown` so embedding code can release DynamoDB slot blocks before dropping the store.
