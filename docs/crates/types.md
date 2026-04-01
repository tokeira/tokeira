# tokeira-types

**Purpose:** Shared domain types and identities used across all crates.

## Why Types Are Separate

This crate exists to keep the rest of the workspace honest. The moment a crate invents its own notion of `RunKey` or `LogicalTaskSeq`, cross-crate contracts get fuzzy. The right level for these terms is a small shared crate.

The types here intentionally avoid storage-driver details and transport details. They are usable from the kernel, runtime, storage, edge, proto, and projection crates without pulling in any of their dependencies.

## Module Map

```
tokeira-types/src/
  ids.rs               — RunKey, RunId, NamespaceId, WorkflowId, WorkflowType
  execution.rs         — ExecutionStatus, TransitionSeq, LogicalTaskSeq
  payload.rs           — Payload, Payloads, Memo
  request.rs           — RequestContext, RequestId
  retry.rs             — RetryPolicy
  search_attributes.rs — SearchAttributes
  task_queue.rs        — TaskQueueName, QueueKey, StickyAffinity
  tokens.rs            — WorkflowTaskToken, ActivityTaskToken
  visibility.rs        — Visibility-related summary types
```

## Key Types

### Identity Types

| Type | Purpose | Used by |
|---|---|---|
| `RunKey` | UUID primary key for a run in storage | All crates |
| `RunId` | Temporal-visible run identifier | All crates |
| `NamespaceId` | Internal namespace identifier | All crates |
| `WorkflowId` | User-assigned workflow identifier | All crates |
| `WorkflowType` | Workflow type name (maps to SDK handler) | kernel, runtime, edge |
| `WorkerIdentity` | Worker self-reported identity string | kernel, runtime |

### Execution Types

| Type | Purpose | Used by |
|---|---|---|
| `ExecutionStatus` | Running, Completed, Failed, Canceled, Terminated, ContinuedAsNew, TimedOut | kernel, storage, projection |
| `TransitionSeq` | Internal fence/checkpoint number; increments once per `apply` | kernel, storage, runtime |
| `LogicalTaskSeq` | Monotonic WFT sequence for token validation | kernel, runtime |

### Payload Types

| Type | Purpose | Used by |
|---|---|---|
| `Payload` | Single serialized value with metadata | kernel, edge, proto |
| `Payloads` | Collection of payloads | kernel, edge, proto |
| `Memo` | User-attached key-value metadata | kernel, projection |
| `SearchAttributes` | Typed search attribute map | kernel, projection |

### Task Queue Types

| Type | Purpose | Used by |
|---|---|---|
| `TaskQueueName` | Logical task queue name | kernel, runtime, edge |
| `QueueKey` | Full queue identity: namespace + name + kind + deployment/build_id | runtime (broker) |
| `StickyAffinity` | Worker identity + expiry for sticky routing | kernel, runtime |

### Token Types

| Type | Purpose | Used by |
|---|---|---|
| `WorkflowTaskToken` | Opaque token binding a WFT to a specific run/seq/attempt | runtime, edge |
| `ActivityTaskToken` | Opaque token binding an activity task to a specific run/activity | runtime, edge |

### Request Types

| Type | Purpose | Used by |
|---|---|---|
| `RequestContext` | Carries request ID for dedup | kernel, edge |
| `RequestId` | Unique identifier for idempotent external requests | kernel, storage |

### Retry Types

| Type | Purpose | Used by |
|---|---|---|
| `RetryPolicy` | Max attempts, backoff, non-retryable error types | kernel, runtime |

## Design Principles

1. **No behavior** — types are data, not services. No I/O, no side effects.
2. **No storage details** — types don't know about DSQL columns or table names.
3. **No transport details** — types don't know about protobuf or gRPC.
4. **Strong typing** — newtypes prevent mixing up `RunId` with `WorkflowId` or `NamespaceId`.
5. **Minimal dependencies** — this crate should have very few external dependencies.

## Temporal Feature Coverage

| Feature | Types participation |
|---|---|
| Workflow identity | `RunKey`, `RunId`, `WorkflowId`, `NamespaceId`, `WorkflowType` |
| Execution lifecycle | `ExecutionStatus`, `TransitionSeq` |
| Task queues | `TaskQueueName`, `QueueKey` |
| Sticky execution | `StickyAffinity`, `WorkerIdentity` |
| Payloads | `Payload`, `Payloads`, `Memo` |
| Search attributes | `SearchAttributes` |
| Task tokens | `WorkflowTaskToken`, `ActivityTaskToken` |
| Request dedup | `RequestContext`, `RequestId` |
| Retry | `RetryPolicy` |
| Visibility | Visibility summary types |
