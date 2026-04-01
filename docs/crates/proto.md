# tokeira-proto

**Purpose:** Public Temporal-compatible wire types and internal control-plane protos.

## What it owns

- **gRPC service definitions** — `WorkflowService`, `OperatorService` stubs
- **Temporal proto compatibility** — generated bindings for Temporal's public API packages
- **Internal protos** — Tokeira-only runtime and admin control-plane messages
- **Proto-to-domain conversions** — explicit helpers between wire structs and `tokeira-types`

## What it does NOT own

- **Workflow semantics** — proto is a wire format, not a behavior layer
- **Request handling** — that's edge
- **State transitions** — that's kernel

## Module Map

```
tokeira-proto/src/
  public/
    common.rs           — temporal.api.common.v1
    enums.rs            — temporal.api.enums.v1
    workflowservice.rs  — temporal.api.workflowservice.v1
    operatorservice.rs  — temporal.api.operatorservice.v1
  internal/
    admin.rs            — tokeira.admin.v1
    runtime.rs          — tokeira.runtime.v1
  conversions/          — proto ↔ tokeira-types helpers
```

## Temporal Proto Compatibility

The crate generates bindings from Temporal's public proto packages:

| Proto package | Purpose |
|---|---|
| `temporal.api.common.v1` | Payloads, Memo, SearchAttributes, Header |
| `temporal.api.enums.v1` | WorkflowExecutionStatus, EventType, TaskQueueKind, etc. |
| `temporal.api.workflowservice.v1` | All WorkflowService RPC request/response types |
| `temporal.api.operatorservice.v1` | OperatorService RPC request/response types |
| `temporal.api.history.v1` | History event types |
| `temporal.api.taskqueue.v1` | TaskQueue, StickyExecutionAttributes |
| `temporal.api.workflow.v1` | WorkflowExecutionInfo, PendingActivityInfo, etc. |
| `temporal.api.failure.v1` | Failure types |
| `temporal.api.query.v1` | Query types |
| `temporal.api.update.v1` | Update types |
| `temporal.api.nexus.v1` | Nexus operation types (future) |

## Internal Protos

Tokeira defines its own proto packages for internal communication:

| Package | Purpose |
|---|---|
| `tokeira.runtime.v1` | Inter-node runtime messages (shard handoff, placement) |
| `tokeira.admin.v1` | Admin/operator control messages |

These are not part of the Temporal compatibility surface and may change freely.

## Conversions

The `conversions` module provides explicit, small helpers between proto-generated structs and `tokeira-types` domain types. This keeps the rest of the workspace from depending directly on generated protobuf details.

Examples:
- Proto `Payload` ↔ `tokeira_types::Payload`
- Proto `WorkflowExecutionStatus` ↔ `tokeira_types::ExecutionStatus`
- Proto `TaskQueue` ↔ `tokeira_types::TaskQueueName`
- Proto `SearchAttributes` ↔ `tokeira_types::SearchAttributes`

## Design Principles

1. **Separation** — public Temporal protos and internal Tokeira protos are clearly separated
2. **Thin conversions** — proto↔domain conversions are explicit and small, not auto-derived
3. **No business logic** — this crate is purely about wire format and type mapping
4. **Compatibility boundary** — changes to internal protos do not affect the public API surface

## Nexus Proto Support (future)

Nexus operation types from `temporal.api.nexus.v1` will be added when Nexus support is implemented. The edge will use these for inbound Nexus endpoint handling.

## Temporal Feature Coverage

| Feature | Proto participation |
|---|---|
| WorkflowService | All RPC request/response types |
| OperatorService | All RPC request/response types |
| History events | Event type definitions |
| Payloads | Payload, Memo, SearchAttributes wire format |
| Task queues | TaskQueue, StickyExecutionAttributes |
| Failures | Failure type hierarchy |
| Queries | Query request/response types |
| Updates | Update request/response types |
| Nexus | Future: Nexus operation types |
| Worker versioning | Future: deployment/build ID types |
