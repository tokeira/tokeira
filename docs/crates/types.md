# tokeira-types

Shared durable-domain value types. The crate keeps identity, routing, task,
visibility, and worker contracts consistent across the compatibility edge and
the lower engine planes.

## Where it sits

The architecture places `tokeira-types` in the compatibility-edge group
because it defines the transport-neutral vocabulary used at that boundary. It
is also intentionally usable by the kernel, runtime, storage, and projection
crates without depending on any of them.

## Surface map

| Area | Modules and representative types |
|---|---|
| Execution identity | `execution`, `ids`: `ExecutionRef`, `ExecutionStatus`, `NamespaceId`, `RunId`, `RunKey`, `ShardEpoch`, `TransitionSeq` |
| Payloads and requests | `payload`, `request`, `retry`: `Payload`, `Headers`, `Memo`, `RequestContext`, `RetryPolicy` |
| Task queues | `task_queue`, `tokens`: `QueueKey`, `StickyAffinity`, `WorkflowTaskToken`, `ActivityTaskToken` |
| Placement | `routing`, `spread`: `RoutingSnapshot`, `RoutingDelta`, `BundleOwner`, deterministic spread-key helpers |
| Visibility | `search_attributes`, `visibility`: `SearchAttrValue`, `SearchAttributes`, `ProjectionCursor`, `ArchetypeId` |
| Workers | `worker_authorization`, `worker_compute`, `worker_heartbeat`: task origins, compute-controller values, heartbeat observations |
| Other shared records | `observability`, `workflow_rules`: metric naming and transport-neutral Workflow Rule records |

## Contracts

- Identity newtypes prevent accidental interchange of workflow, run, namespace,
  shard, and queue identifiers.
- Serialised collections use deterministic shapes where ordering is part of the
  durable or hashed representation.
- Tokens carry enough identity to reject stale workflow-task and activity-task
  completions.
- Routing snapshots and deltas are data contracts; ownership enforcement belongs
  to the runtime and storage layers.
- Visibility values are typed without importing a SQL or protobuf model.

## It does not own

The crate performs no I/O and has no storage-driver or wire-protocol knowledge.
It does not decide workflow transitions, authorize requests, route network
calls, persist heartbeats, or execute worker-compute actions. Traits carried
here, such as `HeartbeatStore`, define shared value-level contracts whose
implementations live elsewhere.

## Pointers

- [Crate root and module map](../../crates/tokeira-types/src/lib.rs)
- [Architecture overview](../architecture/000-overview.md)
- [Storage](storage.md)
- [Proto bindings](proto.md)
