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
