# tokeira-types

Shared durable-domain value types and identity newtypes used by every other crate in the workspace.

## Purpose

`tokeira-types` keeps cross-crate contracts honest. Every crate that needs to talk about a run, a namespace, a payload, or a task token imports these types rather than inventing its own. The crate carries no behaviour, no I/O, and no dependencies on storage or transport.

## Dependencies

External only: `serde`, `time`, `uuid`. No other tokeira crates.

## Module Structure

| File | Contents |
|---|---|
| `ids.rs` | `NamespaceId`, `RunId`, `RunKey`, `ShardId`, `ShardEpoch`, `TransitionSeq`, `LogicalTaskSeq` |
| `execution.rs` | `NamespaceName`, `WorkflowId`, `WorkflowType`, `ExecutionStatus` (8 variants), `ExecutionRef`, `ExecutionSummary` |
| `payload.rs` | Codec-neutral `Payload`, `Payloads`, `Headers`, `Memo` |
| `search_attributes.rs` | `SearchAttrValue` (7 typed variants), `SearchAttributes` map |
| `task_queue.rs` | `TaskKind`, `TaskQueueName`, `WorkerIdentity`, `BuildId`, `DeploymentId`, `QueueKey`, `StickyAffinity` |
| `tokens.rs` | `WorkflowTaskToken`, `ActivityTaskToken`, `TaskToken` enum |
| `request.rs` | `RequestId`, `RequestContext` (idempotency + caller identity) |
| `retry.rs` | `RetryPolicy` (exponential backoff config with non-retryable error types) |
| `visibility.rs` | `ProjectionCursor` (partition-aware cursor for projection log consumption) |

## Key Types

- `RunKey` — internal durable row key (UUID); storage addresses runs by this, not by `RunId`
- `TransitionSeq` — monotonic OCC fence; storage rejects writes with stale sequence
- `ShardEpoch` — monotonic fence for shard lease ownership
- `QueueKey` — composite key (namespace + queue name + task kind + optional deployment/build_id)
- `ExecutionStatus` — 8-variant enum: Running, Paused, Completed, Failed, Cancelled, Terminated, ContinuedAsNew, TimedOut
- `TaskToken` — unified enum wrapping `WorkflowTaskToken` and `ActivityTaskToken`, serialised as opaque bytes on the wire

## Design Principles

1. No behaviour — types are data, not services
2. No storage details — types don't know about DSQL columns
3. No transport details — types don't know about protobuf
4. Strong typing — newtypes prevent mixing up `RunId` with `WorkflowId`
5. Deterministic serialisation — `BTreeMap` used throughout for stable ordering
