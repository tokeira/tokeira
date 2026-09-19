# tokeira-engine

Embeddable Temporal-compatible engine facade and the shared service bootstrap
used by `tokeirad`.

## Where it sits

This cross-cutting composition crate connects the compatibility edge,
authoritative runtime and storage, projection, authentication, configuration,
and observability. It owns the assembled service graph, not the semantics of
the crates it wires together.

## Two shapes

`Engine::embedded()` starts an engine that binds nothing, with an in-memory
duplex endpoint, in-memory authoritative storage, runtime workers, and the
projection services. Optional in-memory snapshot policy restores and persists
the authoritative store across process restarts. A host may attach a TCP
listener afterwards with `Engine::listen`; it serves a clone of the same
routes the duplex endpoint dispatches into, so workers in other processes
reach the one engine.

`TokeiradHandle::start_in_memory` and `run_from_cli` use the same service graph
and attach listener transports. Depending on configuration, the daemon can add
gRPC, HTTP/JSON, gRPC-Web, and Nexus HTTP boundaries. Embedded and
listener-backed calls therefore reach the same edge handlers and runtime
semantics.

## Key surface

| Contract | Purpose |
|---|---|
| `Engine` | Owns a running in-process engine, startup report, logs, background work, and graceful shutdown |
| `TemporalEndpoint` | Cloneable raw-protobuf request endpoint with no socket or name resolution |
| `Engine::service_override` | Callback transport for the Temporal Rust SDK when the feature is enabled |
| `Engine::listen` / `EngineListener` | Optional host-attached TCP listener over the same services; bound address, stop, and drain |
| `EmbeddedEngineConfig` | Explicit in-memory, managed DSQL, or existing DSQL startup decision |
| `EngineStartupReport` | Redacted storage, cluster, schema, and ownership admission evidence |
| `TokeiradHandle` | Listener-backed in-memory server handle used by integration hosts |
| `run_from_cli` | Production daemon bootstrap from the shared CLI/config contracts |
| `Engine::builder` / `EngineBuilder` | CHASM extension registration: `library`, `side_effect_executor`, `clock`, `build` |
| `Engine::chasm::<C>` | Typed handle on a registered root component: start, update, read, and `reference` by business id |
| `Engine::chasm_visibility::<C>` | Read-only projected list/count for a registered root and namespace |
| `tokeira_engine::chasm` | Everything an extension library names — the component and task contracts, the `Component` derive, `namespace_id_for` — so it depends on `tokeira-engine` alone |

## Registering a CHASM component

Behind the `chasm-extensions` feature, a host can register its own durable
component rather than only using the ones the engine ships. The surface is
deliberately small and explicitly unstable — it carries no semver promise.

`Engine::builder(config)` takes libraries, side-effect executors and an optional
clock, then `build()` starts the ordinary service stack. Everything is decided
before admission opens: built-in registration is sealed first, so an extension
cannot replace a built-in name or identity; a colliding component or task
identity fails the build; a duplicate executor task type fails the build; and
storage holding unknown archetypes, incompatible attribute declarations or
unserviceable outboxes is refused rather than served. Executors all register
before persisted effects are rebuilt.

`Engine::chasm::<C>()` then returns a typed handle on a registered root
component, in the same process as the caller — no client, no workflow start, and
no queue between the caller and the decision. `reference(namespace_id,
business_id)` on that handle resolves the current run for a business id from the
authoritative current-run pointer, so a caller that did not perform the start, or
restarted since, reaches the execution again; the reference it returns is the one
the latest commit minted. The current run may already be closed, and `None`
means no run was started under that archetype or the current one was deleted.

An extension library is written against `tokeira_engine::chasm` alone. The
module re-exports the component contract and the bounds `Engine::chasm`
requires, the field and metadata types, task authoring and the executor
contract, the typed handle with its outcomes, the visibility query types, and
`namespace_id_for`, which derives the `NamespaceId` every execution key and
visibility handle for a namespace name carries. The `Component` derive is
re-exported too; because its generated code names the substrate by absolute
path, a library reaching it through the engine declares
`#[chasm(fqn = "...", crate = "::tokeira_engine::chasm")]`. The module's
rustdoc states the wire encodings on the activity boundary: a staged
`StartActivityTask` carries protobuf-encoded `temporal.api.common.v1`
`Payloads`, `Header` and `RetryPolicy`, and a `TaskOutcome` delivered for an
activity carries `Payloads` or a `temporal.api.failure.v1.Failure`. The engine's
own compile check (`tests/chasm_embedder.rs`) is the model: a root component
derived through the re-export, its library, one pure task, one side-effect task
with its executor, and the control-plane calls, with every internal crate
shadowed so a direct import cannot slip in.

`Engine::chasm_visibility::<C>(namespace_id)` returns a `ComponentVisibility`
handle over the same projection store the running engine writes, in memory or
DSQL. `list(ComponentQuery)` provides bounded keyset pagination and
`count(Some(predicate))` counts matching executions. For example, an extension
can filter `DesiredGeneration = 3 AND DeploymentStatus = 'Failed'` using its
declared typed attributes. The shared grammar also supports `ExecutionStatus`
as an archetype-defined keyword. Result summaries preserve that keyword and
open/closed lifecycle separately; a failed operation can leave its resource open.

The handle fixes namespace and archetype after query compilation. Continuations
are bound to that scope and the trimmed predicate; page size defaults to 100
and must be 1–1000. Invalid predicates, page sizes and continuations have named
errors. Cursors are not credentials: the embedding application authorizes its
namespace access before obtaining a handle. This in-process API does not run
Temporal transport interceptors, and workflow/activity RPCs retain their own
archetype isolation.

Visibility is a repairable, eventually consistent read model. Separate pages
and counts do not promise a frozen snapshot during concurrent changes. Use
the typed component handle for authoritative details and commands; never use
a projected row to admit a mutation. This surface adds no embedded in-memory
snapshot restart support.

`EngineBuilder::clock` sets the CHASM plane's nanosecond time source. It is the
plane's definitive clock: transitions, pure deadlines, delayed dispatch, callback
registration and sweeper passes all read it, which is what makes a simulated
clock usable in tests. Workflow time is unchanged, and two real-time uses are
sanctioned and named in the spec — a component long poll's transport deadline,
and the task-token provenance lifetime that storage enforces on real time.

## Contracts

- `Engine::embedded()` and `Engine::start_with_config()` are in-memory-storage
  entry points that bind nothing; every `Engine::start*` path binds nothing.
- `Engine::listen` is the only bind. It serves the engine's own routes, so
  authorization, namespaces, task queues, and storage ownership are shared with
  the in-process endpoint; a failed bind leaves the engine unchanged, and
  engine shutdown stops attached listeners before its own drain.
- `Engine::start_with_embedded_config()` is the explicit boundary for managed
  or existing Aurora DSQL storage; durable modes do not silently downgrade.
- Endpoint clones reject new calls after engine shutdown.
- Graceful shutdown coordinates admission, runtime tasks, leases, ownership,
  connections, and any configured final in-memory snapshot.
- Transport choice does not create a second implementation of workflow,
  schedule, standalone-activity, or visibility behaviour.

## It does not own

The crate does not define public wire types, compatibility policy, state-machine
rules, repository semantics, or projection queries. Changes to those contracts
belong in their owning crates.

## Pointers

- [Crate root](../../crates/tokeira-engine/src/lib.rs)
- [Embedded Tokeira](../../README.md#embedded-tokeira)
- [Architecture overview](../architecture/000-overview.md)
- [Configuration](config.md)
- [Managed DSQL](managed-dsql.md)
