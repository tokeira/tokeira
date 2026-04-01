# tokeira-edge

**Purpose:** Temporal-compatible gRPC compatibility shell.

See [020-kernel](../architecture/020-kernel.md) for how commands are defined, and [040-delivery-broker](../architecture/040-delivery-broker.md) for how long polls reach the delivery layer.

## What it owns

- **WorkflowService** gRPC implementation — all public Temporal RPCs
- **OperatorService** gRPC implementation — namespace and search attribute management
- **Health endpoints** — gRPC health checks
- **Authn/authz** — request authentication and authorization before anything reaches runtime
- **Namespace lookup** — resolving namespace names to internal IDs via a cached registry
- **Request ID handling** — assigning request IDs to incoming calls that lack one
- **Long-poll gating** — holding poller connections at the edge without consuming DSQL connections
- **Proto translation** — converting Temporal wire types into internal commands and DTOs
- **HTTP proxy** — optional HTTP-to-gRPC bridge
- **Routing** — directing requests to the correct runtime node based on execution home or queue placement

## What it does NOT own

- Workflow semantics — no history events, no state transitions
- Storage — never talks to DSQL directly
- Delivery decisions — does not decide which poller gets which task
- Projection — does not write visibility rows

## Module Map

```
tokeira-edge/src/
  grpc.rs              — gRPC server setup
  workflow_service.rs  — WorkflowService RPC handlers
  operator_service.rs  — OperatorService RPC handlers
  health_service.rs    — health check endpoints
  interceptors.rs      — authn/authz middleware
  namespace_cache.rs   — namespace name → ID resolution
  request_id.rs        — request ID assignment and propagation
  long_poll.rs         — long-poll lifecycle management
  translate.rs         — proto ↔ internal type conversions
  routing.rs           — execution-home and queue-home routing
  http_proxy.rs        — HTTP proxy layer
  errors.rs            — edge-specific error types
```

## Temporal API Mapping

The edge translates each WorkflowService RPC into an internal operation:

| RPC | Edge action | Downstream |
|---|---|---|
| `StartWorkflowExecution` | Translate → `Command::Start` | runtime → kernel |
| `SignalWorkflowExecution` | Translate → `Command::Signal` | runtime → kernel |
| `TerminateWorkflowExecution` | Translate → `Command::Terminate` | runtime → kernel |
| `RequestCancelWorkflowExecution` | Translate → `Command::Cancel` | runtime → kernel |
| `UpdateWorkflow` | Translate → `Command::Update` | runtime → kernel |
| `QueryWorkflow` | Translate → query dispatch | runtime (no kernel) |
| `PollWorkflowTaskQueue` | Register long-poll waiter | runtime broker |
| `PollActivityTaskQueue` | Register long-poll waiter | runtime broker |
| `RespondWorkflowTaskCompleted` | Translate → `Command::WorkflowTaskCompleted` | runtime → kernel |
| `RespondActivityTaskCompleted` | Translate → `Command::ActivityResolved` | runtime → kernel |
| `RecordActivityTaskHeartbeat` | Forward heartbeat | runtime |
| `ListWorkflowExecutions` | Forward query | projection |
| `GetWorkflowExecutionHistory` | Forward read | storage |

OperatorService RPCs (namespace CRUD, search attribute management) are handled directly by edge with backing stores.

## Connection to Runtime

Edge routes requests to the correct runtime node using two strategies:

1. **Execution-home routing** — for commands targeting a specific workflow execution, route to the node that owns the shard containing that run
2. **Queue-home routing** — for poll requests, route to the node(s) serving that task queue's delivery broker

The edge does not own shard assignment. It discovers placement via the membership/placement layer and forwards accordingly.

## Long-Poll Behavior

Long polls are held at the edge layer without consuming DSQL connections. The edge:

1. Validates the request and resolves the namespace
2. Registers a waiter with the delivery broker
3. Holds the gRPC stream open until a task is matched or the poll times out
4. Returns the matched task or an empty response on timeout

This is one of the biggest structural simplifications — long polls never touch storage.

## Nexus Endpoint Handling (future)

Inbound Nexus requests will be received at the edge, authenticated, and routed to the appropriate runtime node for dispatch. The edge will handle Nexus service discovery and endpoint resolution.

## Worker Versioning / Deployment Routing (future)

The edge will participate in deployment-aware routing by including build ID and deployment metadata in the translated commands and poll registrations. The actual routing decisions happen in the runtime's delivery broker.

## Temporal Feature Coverage

| Feature | Edge participation |
|---|---|
| Workflow CRUD | Translates all RPCs to internal commands |
| Signals | Translates `SignalWorkflowExecution` |
| Queries | Dispatches to runtime (read-only path) |
| Updates | Translates `UpdateWorkflow` |
| Activities | Translates poll/respond RPCs |
| Timers | No direct participation (runtime/kernel) |
| Visibility | Forwards list/count queries to projection |
| Search Attributes | Forwards operator RPCs to projection |
| Namespaces | Owns namespace resolution and caching |
| Long polls | Owns poll lifecycle and gating |
| Sticky execution | Passes sticky hints through to runtime |
| Continue-As-New | Translates response; runtime handles successor |
| Child workflows | No special handling (runtime/kernel) |
| Nexus | Future: endpoint handling |
| Worker versioning | Future: deployment metadata in routing |
