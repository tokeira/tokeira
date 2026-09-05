# Embedded Engine Listener — Requirements

## Introduction

An embedded Tokeira engine serves the Temporal API only through its in-process endpoint.
A host that also runs Temporal workers in other processes, such as a container started by
the host or a worker on another machine, cannot reach that endpoint. This feature lets a
host attach an optional TCP listener to an already-running embedded engine so external
Temporal clients and workers reach the same engine over gRPC, while in-process callback
clients keep working unchanged.

This is transport over the existing engine. It does not add a second engine, a second
storage owner, a new authentication scheme, or platform-specific address advertising. The
zero-listener default of every existing start path is unchanged.

Compatibility authority: Temporal server v1.31.0 (`TEMPORAL_SERVER_COMPAT`) for the
served API behaviour, which this feature does not alter. The listener is a Tokeira-owned
surface with no Temporal analog, so its contract is defined here and verified against the
current engine code cited below.

Sibling work: this feature narrows two statements of
[managed-embedded-dsql](../managed-embedded-dsql/requirements.md) that define the embedded
engine as never binding a listener; those statements are amended to "binds no listener
unless the host attaches one". The
[continue-as-new-advice](../continue-as-new-advice/requirements.md) feature uses this
listener for its transport-independence evidence and otherwise has no dependency on it.

## Glossary

- **Embedded Engine:** A running [`Engine`](../../../crates/tokeira-engine/src/lib.rs)
  started by `Engine::start`, `Engine::start_with_config`, or
  `Engine::start_with_embedded_config`, in any embedded storage mode.
- **In-process Endpoint:** The `TemporalEndpoint` returned by `Engine::endpoint` and the
  `service_override` adapter built from it. Calls enter the engine without a socket.
- **Service Routes:** The tonic router assembled once per engine from the Workflow,
  Operator, and Admin gRPC services. The in-process endpoint dispatches into it today.
- **Listener:** A TCP socket bound by the host's request that serves the Service Routes
  of one Embedded Engine over gRPC.
- **Listener Handle:** The value returned when a listener is attached. It reports the
  bound address and owns the listener's stop and drain.
- **Bound Address:** The socket address actually bound, including the concrete port when
  the host requested port `0`. When the host requested the unspecified address
  (`0.0.0.0` or `[::]`), it is that unspecified address with the concrete port; the host
  substitutes a reachable interface address when it advertises the listener, because only
  the host knows which address other processes can reach.
- **Engine Shutdown:** `Engine::shutdown` or `Drop for Engine`, which closes in-process
  admission, drains, releases leases and ownership, and closes storage.
- **Listener Stop:** Stopping one listener without shutting the engine down.

## Target State

| Area | Verified current behaviour | Required behaviour |
|---|---|---|
| Transport | Every `Engine::start*` path builds `StackTransport::Embedded` and returns only an `InProcessGrpcService`; no socket is bound ([lib.rs:3475-3487](../../../crates/tokeira-engine/src/lib.rs)). | Unchanged by default. A host may attach one or more listeners to a running engine after startup. |
| Service graph | The in-process service assembles the same tonic `Routes` the daemon mounts ([in_process.rs:93-106](../../../crates/tokeira-edge/src/in_process.rs)); the daemon builds a second server from the same generated services ([lib.rs:3540-3558](../../../crates/tokeira-engine/src/lib.rs)). | A listener serves a clone of the engine's own Service Routes. No service, interceptor, runtime, or storage object is constructed a second time. |
| Storage ownership | Embedded DSQL startup resolves the cluster, applies schema, acquires the ownership claim, and builds the stack once ([lib.rs:937-1140](../../../crates/tokeira-engine/src/lib.rs)). | Attaching a listener touches none of those phases. |
| Listener-backed alternative | `TokeiradHandle::start_in_memory_with_config` forces in-memory storage and builds a separate stack ([lib.rs:2038-2064](../../../crates/tokeira-engine/src/lib.rs)). | Unchanged. It remains the daemon's in-memory facade, not a way to expose an embedded engine. |
| Shutdown | `Engine::shutdown` closes in-process admission, drains handlers, joins tasks, releases shards and ownership, then closes storage, within a 30 s deadline ([lib.rs:839-864](../../../crates/tokeira-engine/src/lib.rs), [lib.rs:874-935](../../../crates/tokeira-engine/src/lib.rs)). Network handlers are not covered by in-process admission. | Engine Shutdown stops and drains every attached listener before the existing sequence continues. Listener Stop leaves the engine and its in-process endpoint usable. |
| Address | `infrastructure.network.grpc_addr` is daemon configuration and the embedded path never reads it. | The bind address is an explicit call parameter. No configuration field is added or reinterpreted. |

Out of scope: HTTP/JSON, gRPC-Web, and Nexus HTTP transports on the listener; TLS
termination; advertising a container-reachable address; a second engine; worker
versioning; Docker or ECS knowledge in the engine.

## Evidence From Current Code

- **Service assembly (authoritative):** `InProcessGrpcService::new` builds
  `Routes::new(workflow.into_service()).add_service(operator...).add_service(admin...)`
  and stores it as `Arc<Mutex<Routes>>`
  ([in_process.rs:73-106](../../../crates/tokeira-edge/src/in_process.rs)). tonic 0.11
  declares `Routes` as `Clone` (`tonic-0.11.0/src/transport/service/router.rs:19-20`)
  and `Server::add_routes(&mut self, routes: Routes) -> Router<L>`
  (`tonic-0.11.0/src/transport/server/mod.rs:399-404`).
- **Authorization placement:** `EdgeInterceptors::configured(namespaces, authorization.grpc,
  principal_attribution)` is passed into `WorkflowService` and `OperatorService`
  ([lib.rs:3188-3193](../../../crates/tokeira-engine/src/lib.rs),
  [lib.rs:3273-3299](../../../crates/tokeira-engine/src/lib.rs),
  [lib.rs:3322](../../../crates/tokeira-engine/src/lib.rs)). Authentication and
  authorization therefore run inside the services, on every transport.
- **Daemon listener path:** `build_service_stack_with_storage` binds
  `TcpListener::bind(addr)`, resolves `local_addr`, and serves with
  `serve_with_incoming_shutdown` on a oneshot
  ([lib.rs:3496-3564](../../../crates/tokeira-engine/src/lib.rs)). The daemon adds
  `NexusHttpLayer`, `HttpApiLayer`, `CorsLayer`, `GrpcWebLayer`, reflection, and the
  flag-gated `WireCoverageLayer`.
- **Embedded stack contents:** `EmbeddedStack` retains the in-process service, cancel
  token, log broadcast, recovery task, task groups, and shard cleanup
  ([lib.rs:2453-2461](../../../crates/tokeira-engine/src/lib.rs)). `Engine` retains the
  endpoint, cancel token, snapshot policy, startup report, and shutdown coordinator
  ([lib.rs:412-420](../../../crates/tokeira-engine/src/lib.rs)).
- **In-process admission:** only `InProcessGrpcService::call` takes an admission permit
  ([in_process.rs:137-139](../../../crates/tokeira-edge/src/in_process.rs)); network
  handlers on a tonic server are drained by tonic's graceful shutdown instead.
- **Zero-listener property:** the embedded startup effect model asserts
  `listener_attempts == 0` ([lib.rs:4742](../../../crates/tokeira-engine/src/lib.rs)).
- **Existing test patterns:** an SDK `Connection` over `service_override`
  ([embedded.rs:193-200](../../../crates/tokeira-engine/tests/embedded.rs)) and a
  `WorkflowServiceClient` over TCP against a bound port
  ([embedded.rs:357-389](../../../crates/tokeira-engine/tests/embedded.rs)).
- **Dependencies:** [managed-embedded-dsql](../managed-embedded-dsql/requirements.md)
  owns startup, ownership, and shutdown ordering; this feature inserts listener stop into
  that ordering and changes nothing else in it.

## Contract Policy

### `Engine::listen(addr)`

| Input / output | Target policy | Error if invalid | Side-effect impact |
|---|---|---|---|
| `addr: SocketAddr` | Bind exactly this address; port `0` requests an ephemeral port; the unspecified address is bound and reported as given. | Bind failure returns `EngineListenError::Bind` carrying the OS error; the engine is unchanged. | A successful bind spawns one server task tied to the engine's cancellation. |
| engine already shutting down | Refuse before binding. | `EngineListenError::ShutDown`. | None. |
| returned `EngineListener` | Reports the Bound Address; owns stop and drain. | n/a | Registered with the engine so Engine Shutdown can stop it. |

### `EngineListener`

| Operation | Target policy | Error if invalid | Side-effect impact |
|---|---|---|---|
| `bound_addr()` | The address from `TcpListener::local_addr` after bind. | n/a | None. |
| `shutdown(self)` | Stop accepting, drain in-flight calls within the listener deadline, join the server task, deregister from the engine. | `EngineListenerShutdownError::DrainTimeout` when in-flight calls outlive the deadline; `EngineListenerShutdownError::Task` when the server task failed. | The engine and its in-process endpoint keep serving. |
| drop without `shutdown` | Signal the listener to stop; do not await. | n/a | The server task exits on its own; the engine stays usable. |

## Requirements

### Requirement 1: Attach a listener to a running engine

**User Story:** As an embedding host, I want to expose my running engine on a TCP address
I choose, so that workers in other processes can poll the same engine my in-process
clients use.

#### Acceptance Criteria

1.1 WHEN the host calls `Engine::listen` with a socket address on a running engine, THE
engine SHALL bind that address and serve its Workflow, Operator, and Admin gRPC services
on it.

1.2 WHEN the requested port is `0`, THE engine SHALL bind an ephemeral port and report the
concrete port through the Listener Handle's Bound Address.

1.3 WHEN `Engine::listen` succeeds, THE Listener Handle SHALL report the Bound Address
returned by the operating system for the bound socket.

1.4 THE engine SHALL accept `Engine::listen` in every embedded storage mode: in-memory,
managed DSQL, and existing DSQL.

1.5 THE engine SHALL accept more than one concurrent listener on distinct addresses.

1.6 THE listener SHALL serve the gRPC reflection service for the pinned Temporal file
descriptor set, as the daemon does.

1.7 THE engine SHALL NOT read `infrastructure.network.grpc_addr` or any other
configuration field to decide whether or where to listen.

1.8 WHEN the requested address is the unspecified address (`0.0.0.0` or `[::]`), THE
Listener Handle SHALL report that unspecified address with the concrete bound port and
SHALL NOT substitute an interface address.

### Requirement 2: One engine, one service graph

**User Story:** As an embedding host, I want network clients and in-process clients to
see exactly the same engine, so that a workflow started through one path is observed and
executed through the other.

#### Acceptance Criteria

2.1 THE listener SHALL serve a clone of the engine's own Service Routes; it SHALL NOT
construct a second Workflow, Operator, or Admin service.

2.2 WHILE a listener is attached, THE engine SHALL keep serving the In-process Endpoint
with unchanged behaviour.

2.3 WHEN a workflow is started through the In-process Endpoint, THE listener SHALL
deliver its workflow tasks to a worker polling the same task queue over the network.

2.4 WHEN a worker over the listener completes an execution, THE In-process Endpoint SHALL
observe the same result through `GetWorkflowExecutionHistory` and
`DescribeWorkflowExecution`.

2.5 THE listener SHALL apply the engine's configured authentication and authorization to
every call exactly as the In-process Endpoint does, because both dispatch into the same
interceptor-bearing services.

2.6 THE listener SHALL preserve gRPC request metadata, response metadata, status codes,
and status details for every served RPC.

2.7 WHEN a network caller cancels a long-poll RPC, THE listener SHALL drop the handler so
its long-poll admission is released, matching the daemon's behaviour for the same RPC.

### Requirement 3: Attaching a listener changes no storage or ownership state

**User Story:** As an operator of a durable embedded engine, I want the listener to be
pure transport, so that exposing the engine cannot create a second storage owner or alter
the admitted storage configuration.

#### Acceptance Criteria

3.1 WHEN `Engine::listen` is called on a DSQL-backed engine, THE engine SHALL NOT open a
new storage connection pool, acquire an ownership claim, run a migration, or change the
admitted storage configuration.

3.2 WHEN `Engine::listen` is called, THE engine SHALL NOT change the startup report.

3.3 THE listener SHALL NOT register or advertise a node endpoint for placement or
routing.

### Requirement 4: Failure leaves the engine usable

**User Story:** As an embedding host, I want a failed bind to be an ordinary error, so
that my engine keeps running and I can retry on another address.

#### Acceptance Criteria

4.1 IF binding the requested address fails, THEN `Engine::listen` SHALL return
`EngineListenError::Bind` with the operating-system error and the requested address.

4.2 IF binding fails, THEN THE engine SHALL remain running with its In-process Endpoint
usable and no listener task spawned.

4.3 IF `Engine::listen` is called after Engine Shutdown began, THEN THE engine SHALL
return `EngineListenError::ShutDown` without binding.

4.4 IF a listener's server task exits with an error while the engine is running, THEN
THE Listener Handle's `shutdown` SHALL report `EngineListenerShutdownError::Task` and
THE engine SHALL keep serving its In-process Endpoint.

### Requirement 5: Listener stop and engine shutdown

**User Story:** As an embedding host, I want to stop a listener on its own or as part of
engine shutdown, so that no socket or task outlives the lifecycle I control.

#### Acceptance Criteria

5.1 WHEN `EngineListener::shutdown` is called, THE listener SHALL stop accepting
connections, drain in-flight calls within the listener deadline, and join its server
task.

5.2 WHEN `EngineListener::shutdown` completes, THE engine SHALL keep serving the
In-process Endpoint and any other attached listener.

5.3 WHEN Engine Shutdown begins, THE engine SHALL signal every attached listener to stop
before draining in-process handlers.

5.4 WHEN Engine Shutdown runs, THE engine SHALL await every attached listener's server
task within the engine's shutdown deadline before releasing shard leases or ownership.

5.5 IF a listener does not drain within the engine's shutdown deadline, THEN THE engine
SHALL record `EmbeddedShutdownFailure::ListenerDrain` and continue the remaining shutdown
steps.

5.6 WHEN an `Engine` is dropped without `shutdown`, THE engine SHALL signal every attached
listener to stop.

5.7 WHEN an `EngineListener` is dropped without `shutdown`, THE listener SHALL stop
accepting connections and release its socket when its task exits.

5.8 WHEN Engine Shutdown begins, THE listener SHALL release in-flight long polls the same
way the daemon does, so that drain completes within the deadline rather than waiting for
poll timeouts.

5.9 WHEN a listener has stopped, THE engine SHALL hold no task, socket, or registration
for it.

### Requirement 6: Published surface

**User Story:** As a downstream consumer of published crates, I want the listener API on
`tokeira-engine` alone, so that I depend on no engine-internal crate.

#### Acceptance Criteria

6.1 THE `tokeira-engine` crate SHALL export `Engine::listen`, `EngineListener`,
`EngineListenError`, and `EngineListenerShutdownError` as public items.

6.2 THE listener API SHALL expose only `std` and `tokeira-engine` types in its
signatures; no `tokeira-edge`, tonic, or hyper type is required to call it.

6.3 THE crate README and `docs/crates/engine.md` SHALL document the listener with a
complete start, listen, and shutdown example.

6.4 THE existing zero-listener tests and the `listener_attempts == 0` property for
`Engine::start*` SHALL continue to pass unchanged.

### Requirement 7: Evidence on live DSQL and from a container

**User Story:** As the engine owner, I want the listener proven on the durable path, so
that in-memory success is not mistaken for DSQL support.

#### Acceptance Criteria

7.1 THE live managed-DSQL lifecycle test SHALL attach a listener, run a network worker
through one execution, stop the listener, shut the engine down, and restart against the
same history.

7.2 WHILE a listener is attached to a DSQL-backed engine, THE ownership claim SHALL stay
exclusive: a second engine start against the same cluster SHALL fail at the ownership
phase exactly as it does without a listener.

7.3 WHERE a Docker daemon is available to a test host, THE evidence SHALL include a
container connecting to the host's listener using only the published API; THE default
test suite SHALL NOT require Docker.

## Iteration and Feedback Notes

- The Cloud handoff asked for "an optional engine-owned listener attached to an
  already-started Engine". `Engine::listen` is that shape; a start-time option is not
  offered because the routes clone makes post-start attachment strictly simpler and
  keeps every start path zero-listener.
- HTTP/JSON and gRPC-Web are deliberately excluded from the first listener. The daemon
  builds those layers already, so adding a `ListenOptions` value later is additive.
- `EngineListener::shutdown` uses the same 30 s deadline as `Engine::shutdown`. A
  configurable deadline is not offered until a host needs one.
