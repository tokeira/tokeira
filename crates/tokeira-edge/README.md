# tokeira-edge

`tokeira-edge` is the public compatibility shell for Tokeira.

The main rule for this crate is:

> keep protocol and admission logic at the edge, keep durable workflow semantics in the runtime/kernel.

That means this crate is responsible for:

- request identification,
- authn/authz interception,
- namespace resolution,
- long-poll admission,
- request/response translation,
- optional front-door routing/forwarding decisions,
- HTTP proxy path parsing for Temporal-style `/api/v1/{service}/{method}` routes.

And it is deliberately **not** responsible for:

- workflow state transitions,
- matching/queue durability,
- timer semantics,
- visibility indexing,
- persistence retries.

Those belong in `tokeira-runtime`, `tokeira-kernel`, `tokeira-storage`, and `tokeira-projection`.

## What is intentionally concrete in this starter crate

Even though this is still a starter crate, it contains concrete shapes for:

- `WorkflowService`
  - `StartWorkflowExecution`
  - `SignalWorkflowExecution`
  - `PollWorkflowTaskQueue`
  - `RespondWorkflowTaskCompleted`
  - `Describe/List/Count` scaffolding
- `OperatorService`
  - cluster info
  - search-attribute management
- `HealthService`
- `EdgeInterceptors`
- `LongPollGate`
- `NamespaceCache`
- `HttpProxy` route parsing
- translation between edge DTOs and internal runtime/kernel requests

## Design choices reflected in the code

### 1. Long polls are admitted at the edge and should not hold database resources
The long-poll gate is intentionally a semaphore over edge memory/connections.
A waiting poller should not force the runtime or storage layer to pin a DSQL session.

### 2. The edge owns wall-clock defaults and request IDs
This keeps runtime/kernel APIs deterministic from the point of view of testing and replay.
If a request omits `request_id` or `now`, the edge supplies them.

### 3. Namespace resolution happens before runtime dispatch
The edge is the right place to reject requests for deleted or unknown namespaces.
That saves downstream work and makes authorization decisions namespace-aware.

### 4. Routing is transport-level, not semantic
The `EdgeRouter` can later decide whether a request should be handled locally or forwarded,
but it does not change workflow semantics. A forwarded request is still the same request.

### 5. Translation is explicit
Rather than smearing conversion logic through handlers, the crate keeps a dedicated
`translate` module. This usually pays off when Temporal-compatible proto structs arrive
later through `tokeira-proto`.

## Likely future integration points

- `tokeira-proto` generated request/response types
- gRPC service implementations via `tonic`
- HTTP JSON codec glue
- concrete authz policies from `tokeira-auth`
- concrete namespace metadata backed by storage/projection
