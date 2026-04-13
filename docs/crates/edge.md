# tokeira-edge

Public API compatibility shell for Tokeira. This crate admits and translates requests but does not implement durable workflow semantics. If a change would alter workflow history ordering, retry semantics, timer behavior, or task durability, it belongs in kernel, runtime, or storage instead.

## Dependencies

- `tokeira-kernel` — command types, history events
- `tokeira-proto` — generated protobuf bindings
- `tokeira-runtime` — runtime API types (`StartedWorkflowTask`, `StartedActivityTask`, `UpdateOutcome`, etc.)
- `tokeira-storage` — `RunRepository` for history reads
- `tokeira-types` — identity types, tokens, queue keys
- External: `anyhow`, `async-trait`, `http`, `prost`, `prost-types`, `serde`, `serde_json`, `thiserror`, `time`, `tokio`, `tonic`, `tracing`, `uuid`

## Module Structure

| File | Contents |
|---|---|
| `workflow_service.rs` | `WorkflowService` struct, `WorkflowRuntimeApi` trait, `ExecutionResolver` trait, `VisibilityApi` trait, all RPC handler methods |
| `operator_service.rs` | `OperatorService` struct, `OperatorApi` trait, `InMemoryOperatorApi`, `ClusterInfo`, `SearchAttributeDefinition` |
| `health_service.rs` | gRPC health check endpoints |
| `interceptors.rs` | Authn/authz middleware, `Action` enum for all RPC types |
| `namespace_cache.rs` | Namespace name → ID resolution, `NamespaceCache` trait, `ResolvedNamespace` |
| `poller_registry.rs` | `PollerRegistry` for tracking active pollers per queue, RAII `PollerGuard` |
| `history_wait.rs` | `HistoryWaitRegistry` for long-poll notifications via watch channels, `HistoryNotifyingRepository` wrapper |
| `long_poll.rs` | `LongPollGate` — semaphore-based admission control (default 10k concurrent) |
| `http_proxy.rs` | HTTP `/api/v1/{service}/{method}` route parsing, JSON error responses |
| `request_id.rs` | Request ID assignment and propagation |
| `routing.rs` | `EdgeRouter` trait, execution-home and queue-home routing |
| `errors.rs` | `EdgeError` enum with status codes and action names |
| `translate/mod.rs` | All edge-facing DTO types (30+ request/response structs) |
| `translate/to_internal.rs` | Proto/edge → kernel command translation |
| `translate/from_internal.rs` | Kernel/runtime → proto/edge response translation |
| `translate/history_serializer.rs` | History event serialization with `ActivityTaskStarted` support |
| `grpc/` | gRPC server setup: `workflow_service.rs`, `operator_service.rs`, `runtime_adapter.rs`, `translate.rs`, `errors.rs`, `metadata.rs` |

## Implemented RPC Handlers

### WorkflowService

| RPC | Status |
|---|---|
| `StartWorkflowExecution` | ✅ |
| `SignalWorkflowExecution` | ✅ |
| `SignalWithStartWorkflowExecution` | ✅ |
| `PollWorkflowTaskQueue` | ✅ |
| `RespondWorkflowTaskCompleted` | ✅ |
| `PollActivityTaskQueue` | ✅ |
| `RespondActivityTaskCompleted` | ✅ |
| `RespondActivityTaskFailed` | ✅ |
| `RecordActivityTaskHeartbeat` | ✅ |
| `TerminateWorkflowExecution` | ✅ |
| `RequestCancelWorkflowExecution` | ✅ |
| `QueryWorkflow` | ✅ |
| `UpdateWorkflowExecution` | ✅ |
| `DescribeWorkflowExecution` | ✅ |
| `GetWorkflowExecutionHistory` | ✅ (with long-poll via watch channels, close-event filter) |
| `GetWorkflowExecutionHistoryReverse` | ✅ |
| `ListWorkflowExecutions` | ✅ (delegates to VisibilityApi) |
| `CountWorkflowExecutions` | ✅ (delegates to VisibilityApi) |
| `DeleteWorkflowExecution` | ✅ |
| `ResetWorkflowExecution` | ✅ |
| `GetSystemInfo` | ✅ |
| `GetClusterInfo` | ✅ |
| `ListNamespaces` | ✅ |
| `DescribeNamespace` | ✅ |
| `RegisterNamespace` | ✅ |
| `DescribeTaskQueue` | ✅ (returns active pollers from registry) |

### OperatorService

- `ClusterInfo` — cluster name, version, notes
- `ListSearchAttributes` / `UpsertSearchAttribute` / `RemoveSearchAttribute` — namespace-scoped SA registry

## Key Features

- **Long-poll for GetWorkflowExecutionHistory** — `HistoryWaitRegistry` uses `tokio::sync::watch` channels to notify waiters when new events are committed
- **HistoryNotifyingRepository** — wraps `RunRepository` to automatically notify history waiters on `commit_transition` and `materialize_reset_successor`
- **PollerRegistry** — tracks active pollers per queue with RAII guards for automatic cleanup
- **Close-event filter** — `history_event_filter_type` support for `get_result()` style calls
- **HTTP proxy** — `/api/v1/{service}/{method}` route parsing for gRPC-Web compatibility

## Tests

15 unit tests + 1 integration test.
