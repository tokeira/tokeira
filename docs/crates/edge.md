# tokeira-edge

Temporal-compatible public API boundary. The crate admits, authenticates,
validates, routes, and translates requests, then shapes lower-layer results back
onto the wire.

## Where it sits

This is the outer compatibility-edge crate. Observable API behaviour is pinned
to the targeted Temporal server release. Durable workflow and activity meaning
belongs to the authoritative runtime and storage plane.

## Request path

`grpc` and `http_api` decode transport requests and call domain services in
`workflow_service` and `operator_service`. `interceptors` applies
authentication, authorization, and request metadata. `namespace_cache` resolves
names, `routing` and `routing_cache` forward non-local work, and `translate`
converts between wire and domain types. `EdgeError` is the shared boundary for
consistent status mapping.

## Key surfaces

| Area | Representative contracts |
|---|---|
| Public services | `WorkflowService`, `OperatorService`, tonic adapters, in-process gRPC service |
| Admission | `EdgeInterceptors`, `Action`, scoped Worker sessions, request IDs |
| Routing | `EdgeRouter`, `CacheBackedRouter`, `RoutingCache`, namespace resolution |
| Long polls | `LongPollGate`, `HistoryWaitRegistry`, `HistoryNotifyingRepository`, `PollerRegistry` |
| Translation | Request/response DTOs, command conversion, history serialization, status conversion |
| Additional surfaces | Schedule APIs, batch driver, Nexus endpoints and callbacks, standalone-activity bridge, Workflow Rules |
| CHASM executors | `StartActivityExecutor`, `ActivityDispatchExecutor`, `DeliverCallbackExecutor` |
| Conformance | Wire-coverage and functional-conformance reporting types |

## Contracts

- Every public call passes through the admission and authorization seam.
- Blocking calls return before the caller's deadline; the edge supplies wait
  primitives rather than making the runtime block.
- Routing chooses where to send a request but never grants shard ownership.
- The standalone-activity bridge translates Activity Execution RPCs into CHASM
  calls; `tokeira-chasm-activity` owns the activity state machine.
- Visibility list, count, and describe results come from the projection plane.
- HTTP/JSON, gRPC, gRPC-Web, and in-process calls converge on the same service
  handlers.

## CHASM side-effect executors

Three executors live here, beside the activity bridge, and cover four roles.
They are here rather than in the runtime because starting an activity is the
bridge's own logic and the edge already depends on both the runtime and the
activity library; the reverse dependency would be new. Each registers with the
runtime's multiplexer at bootstrap.

| Executor | Effect | What makes a repeat inert |
|---|---|---|
| `StartActivityExecutor` | Start and schedule atomically, using the staging task's deterministic request id | The same request id returns the existing run rather than reinitializing it |
| `ActivityDispatchExecutor` | Enqueue on the bridge queue under task queue and version target | The queue dedupes on key and stamp; a served or superseded stamp does nothing |
| `DeliverCallbackExecutor` | Two arms: a Nexus HTTP post, or an in-process apply to an internal target | The attempt is recorded through the activity's held delivery task; the target's own held-task fence makes an internal replay inert after its first commit |

`StartActivityExecutor` is generic. Its payload, `StartActivityTask`, is defined
in the substrate, so any registered library can stage one and receive the
activity's terminal outcome through its own `SideEffectTaskHandler`.

## Versioned poll and task provenance

A standalone activity may name an exact worker release. The dispatch carries the
target, the task token carries it back, and a poll that names a release is served
only work targeted at that build — another build polling the same queue is not
served it, and a token issued to one release cannot be redeemed by another.

The provenance record backing that check has a deliberate seam. It expires one
start-to-close timeout after the instant the task is **served**, sampled from
real time rather than from the CHASM clock. Two reasons: the stores enforce the
lifetime on real time — the in-memory store filters expired rows on read and the
DSQL store purges them on a maintenance sweep — and a poll can block until a
delayed retry is released, so a record anchored at admission could be born past
its own lifetime. Under the default clock this is the same instant the activity
records as its start; only a simulated clock diverges.

## It does not own

The edge does not decide history ordering, retries, timers, workflow task
durability, CHASM transitions, schedules, or visibility materialization. It also
does not define protobuf messages or compatibility policy.

## Pointers

- [Crate rustdoc source](../../crates/tokeira-edge/src/lib.rs)
- [Authentication and authorization](auth.md)
- [Compatibility metadata](compatibility.md)
- [Runtime](runtime.md)
- [Projection](projection.md)
