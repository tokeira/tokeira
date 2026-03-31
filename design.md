# Design: Tokeira Edge (`api` + `poll`)

## Problem

The `tokeira-edge` crate already contains the beginnings of a thin compatibility shell — request interception, namespace resolution, long-poll admission, request/response translation, transport-level routing, and small service facades such as `WorkflowService`, `OperatorService`, and `HealthService`.

What is still missing is a clear design statement for **what Edge is allowed to own** and **how poll traffic differs from normal API traffic**.

Without that design, the failure mode is predictable:

- workflow semantics start to leak into Edge,
- long-poll worker traffic is treated like ordinary API traffic,
- request admission becomes inconsistent across methods,
- routing becomes entangled with correctness,
- and the crate becomes a second runtime instead of a transport shell.

That is precisely the direction Tokeira is trying to avoid.

The architectural intent for Tokeira is:

- **history is authoritative**,
- **the kernel owns workflow semantics**,
- **runtime owns execution and delivery**,
- **projection owns visibility and custom read models**,
- and **Edge is a compatibility and admission boundary**.

This design document makes that boundary explicit.

## Goal

Define `tokeira-edge` as a **thin, explicit, and stable compatibility shell** that:

- preserves Temporal-style public interfaces,
- cleanly separates **`api`** and **`poll`** traffic,
- performs request-scoped concerns exactly once,
- never becomes part of the durable workflow correctness path,
- and remains small enough that most substantive behavior changes belong elsewhere.

Concretely, the design should make it obvious that:

- `api` requests are ordinary request/response calls,
- `poll` requests are admitted long-lived waiter registrations,
- Edge does **not** hold DSQL sessions,
- Edge does **not** implement workflow semantics,
- Edge routing is **transport-level**, not **correctness-level**,
- and `tokeira-edge` can be deployed as two services (`edge-api` and `edge-poll`) without forking the crate.

## Architecture

```text
client/sdk
    |
    v
load balancer / ingress
    |
    +------------------------------+
    |                              |
    v                              v
edge-api                       edge-poll
    |                              |
    |                              |
    +----------+         +---------+
               |         |
               v         v
            runtime / broker / resolver / visibility
               |
               v
      {kernel, storage, projection}
```

| Layer | Crate / Service | Owns |
|------|------------------|------|
| Edge transport | `tokeira-edge` | request admission, authn/authz, namespace resolution, translation, transport routing, long-poll gating |
| Edge deployment role | `edge-api` | non-poll public methods, operator methods, health, normal request/response APIs |
| Edge deployment role | `edge-poll` | long-poll worker methods, waiter admission, poll-specific overload handling |
| Execution | `tokeira-runtime` | run routing, broker interaction, lane scheduling, task start/completion flow |
| Semantics | `tokeira-kernel` | workflow state machine, history events, command validation, transition production |
| Durability | `tokeira-storage` | fenced commits, history persistence, dedupe, authoritative state |
| Read models | `tokeira-projection` | visibility, count/list support, custom sinks |

The crucial point is that **Edge is above correctness**.

Edge may reject, throttle, route, translate, authenticate, and observe.
It may not decide workflow semantics.

## Why `api` and `poll` must be separate

The public API surface contains two very different traffic classes.

### `api`

These methods are ordinary request/response operations:

- start workflow
- signal workflow
- respond workflow task completed
- describe execution
- list/count visibility
- operator operations
- health checks

Their pressure profile is mostly:

- CPU for translation and validation,
- some resolver or visibility reads,
- short-lived runtime calls,
- bounded latency expectations.

### `poll`

These methods are fundamentally different:

- `PollWorkflowTaskQueue`
- later: `PollActivityTaskQueue`, Nexus-like poll flows, etc.

Their pressure profile is:

- open sockets,
- memory-resident waiter objects,
- deadline timers,
- cancellation handling,
- bursty fan-in from many workers,
- and potentially huge concurrency with comparatively low useful work.

Treating these two traffic classes as the same service is how a worker poll storm starts to dominate a frontend fleet.

So the design is:

- **one crate**: `tokeira-edge`
- **two service roles**:
  - `edge-api`
  - `edge-poll`

The crate should make those roles easy to construct from the same building blocks.

## Edge responsibilities

`Tokeira-edge` owns exactly five concerns.

### 1. Request-scoped interception

Every public method should pass through the same request pipeline:

1. assign or recover request id,
2. authenticate caller,
3. resolve namespace metadata,
4. authorize action,
5. annotate request as poll or non-poll.

That is the job of `EdgeInterceptors`.

```rust
pub struct EdgeContext {
    pub request_id: RequestId,
    pub principal: Principal,
    pub namespace: Option<ResolvedNamespace>,
    pub received_at: OffsetDateTime,
    pub is_long_poll: bool,
}
```

The design intent is that handler methods become boring because all common request work has already happened.

### 2. Transport-level routing

Edge may decide **where to send a request**, but not **how the workflow behaves**.

```rust
pub enum RouteTarget {
    Local,
    Remote { target: String },
}

#[async_trait]
pub trait EdgeRouter: Send + Sync + 'static {
    async fn route_workflow(&self, namespace: &str, workflow_id: &str) -> EdgeResult<RouteTarget>;
    async fn route_task_queue(&self, namespace: &str, task_queue: &str) -> EdgeResult<RouteTarget>;
}
```

This is a transport concern.
A remote route means “forward the same request elsewhere”, not “redefine correctness”.

### 3. Translation

Edge speaks external API shapes and runtime-facing internal shapes.

It owns:

- request decoding,
- field normalization,
- defaulting that is purely interface-level,
- response shaping,
- HTTP proxy path parsing.

It does **not** own semantic interpretation beyond what is required to produce a valid runtime/kernel request.

### 4. Long-poll admission

Long polls should be gated before they consume deeper resources.

```rust
#[derive(Clone, Debug)]
pub struct LongPollConfig {
    pub max_concurrent: usize,
    pub acquire_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct LongPollGate {
    sem: Arc<Semaphore>,
    config: LongPollConfig,
}
```

The design intent is explicit:

- long polls consume sockets, memory, and scheduler attention,
- but they must **not** pin DSQL sessions,
- and they must **not** force the runtime to allocate durable state just because a worker is waiting.

### 5. Small service facades

The public API surface should be modeled as small, delegating service objects:

- `WorkflowService`
- `OperatorService`
- `HealthService`
- later possibly `AdminService`, `NexusService`, or `OpenApi/HTTP gateway` helpers

These services should remain orchestration shells, not logic centers.

## What Edge explicitly does **not** own

This is just as important as what it does own.

Edge does **not** own:

- workflow ordering,
- timer semantics,
- signal semantics,
- retry or dedupe correctness,
- task durability,
- sticky correctness,
- archival,
- visibility indexing,
- request replay semantics,
- or DSQL connection management.

If a proposed change affects workflow history or durable task meaning, it belongs below Edge.

## The main service contracts

### `WorkflowRuntimeApi`

This is the central runtime-facing contract used by `WorkflowService`.

```rust
#[async_trait]
pub trait WorkflowRuntimeApi: Send + Sync + 'static {
    async fn start_workflow(&self, req: StartReq) -> Result<ApplyOutcome>;

    async fn signal_workflow(&self, run_key: RunKey, req: SignalReq) -> Result<ApplyOutcome>;

    async fn poll_workflow_task(&self, req: PollRequest) -> Result<Option<StartedWorkflowTask>>;

    async fn complete_workflow_task(
        &self,
        encoded_token: &[u8],
        identity: String,
        commands: Vec<WorkflowCommand>,
        force_new_workflow_task: bool,
    ) -> Result<ApplyOutcome>;
}
```

This is intentionally small.

Why?

Because the edge should not know the runtime’s internal composition of lanes, broker, store, shard ownership, or sweeper logic. It only needs workflow-facing operations.

### `ExecutionResolver`

```rust
#[async_trait]
pub trait ExecutionResolver: Send + Sync + 'static {
    async fn current_run_key(&self, namespace: &str, workflow_id: &str) -> Result<Option<RunKey>>;

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowExecutionDescription>>;
}
```

This exists because the runtime hot path is run-key-centric, while many public APIs begin from `(namespace, workflow_id)`.

Keeping this separate avoids contaminating the runtime hot path with API-oriented lookup concerns.

### `VisibilityApi`

```rust
#[async_trait]
pub trait VisibilityApi: Send + Sync + 'static {
    async fn list_workflows(
        &self,
        req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse>;

    async fn count_workflows(
        &self,
        req: CountWorkflowExecutionsRequest,
    ) -> Result<CountWorkflowExecutionsResponse>;
}
```

Visibility is projection-backed and should remain separate from runtime correctness.

### `OperatorApi`

This exists so operator-facing endpoints can stay thin and delegate to the control/admin plane.

### `HealthReporter`

Health checks are hit constantly by load balancers and operators, so the health path must remain cheap even during partial outages.

## `WorkflowService` design

`WorkflowService` is the main public interface shell.

```rust
pub struct WorkflowService {
    runtime: Arc<dyn WorkflowRuntimeApi>,
    resolver: Arc<dyn ExecutionResolver>,
    visibility: Arc<dyn VisibilityApi>,
    interceptors: Arc<EdgeInterceptors>,
    long_polls: LongPollGate,
    router: Arc<dyn EdgeRouter>,
}
```

This structure is important because it shows the service is composed from **capabilities**, not hardcoded topology.

### Start flow

For `StartWorkflowExecution`:

1. run interceptors,
2. compute route by workflow identity,
3. if remote, forward,
4. if local, translate request,
5. call runtime,
6. translate response.

This is deliberately small and linear.

### Signal flow

For `SignalWorkflowExecution`:

1. run interceptors,
2. resolve current run key for `(namespace, workflow_id)` or exact run selection,
3. route by workflow identity,
4. translate signal request,
5. call runtime,
6. translate response.

The key point is that Edge resolves identity, but the runtime still performs the durable state transition.

### Poll flow

For `PollWorkflowTaskQueue`:

1. run interceptors with `is_long_poll = true`,
2. acquire `LongPollGate` permit,
3. route by task queue,
4. translate poll request,
5. call runtime broker-facing poll method,
6. await response or timeout,
7. shape poll response.

The gate happens **before** runtime/broker work.

That protects deeper layers from poll storms.

## The `poll` service role

The `poll` role is not a different correctness service. It is a different **traffic role**.

Its obligations are:

- admit or reject long polls quickly,
- create memory-only waiter pressure,
- surface overload clearly,
- and keep normal API traffic from being crowded out.

### Poll invariants

The `poll` role should preserve these invariants:

- a waiting poll should not allocate durable state,
- a waiting poll should not pin a DSQL session,
- a waiting poll should be cancellable by client disconnect,
- poll overload should be visible and backpressured at Edge,
- and a runtime/broker stall should not collapse the entire public API fleet.

### What the current `LongPollGate` gives us

The current code already encodes the first important step:

```rust
pub async fn acquire(&self) -> EdgeResult<LongPollPermit>
```

That is the right shape because it makes admission explicit and scoped.

### What is still missing

A production-quality `poll` role will need more than the current global semaphore.

Future work should add:

- per-namespace quotas,
- per-task-queue quotas,
- per-principal or per-source quotas,
- cancellation propagation,
- overload hints for workers,
- and separate metrics for admitted, rejected, timed-out, and completed polls.

## The `api` service role

The `api` role is the normal public surface.

It should include:

- workflow request/response calls,
- visibility read calls,
- operator/admin-facing public calls,
- health checks,
- HTTP proxy translation.

It should **not** carry long-poll-specific overload behavior except for method dispatching or forwarding to the `poll` role.

That separation lets deployments tune and scale `edge-api` and `edge-poll` differently without changing crate boundaries.

## HTTP proxy design

The current `HttpProxy` is intentionally narrow:

```rust
pub struct HttpProxy;
```

It only understands:

- `/api/v1/{service}/{method}` path parsing,
- route typing,
- generic JSON response shaping.

That is correct.

The proxy layer should remain transport-only.
It must not become a second workflow API implementation.

## Namespace handling

Namespace lookup is part of request interception because:

- authorization is often namespace-scoped,
- deleted namespaces should fail early,
- and individual handlers should not repeat this logic.

So the design keeps namespace resolution in `EdgeInterceptors`, not inside each service handler.

This also means the runtime does not need to carry “is this namespace deleted?” logic as part of its normal request path.

## Error model

The edge error model should convert internal failures into stable external failures with consistent transport semantics.

This is not just about convenience. It is how we prevent each endpoint from inventing slightly different failure behavior.

The design rule should be:

- internal components return rich internal errors,
- Edge maps them to external categories once,
- transport-specific shaping happens at the boundary.

## Deployment shape

One of the most important design choices is that the crate is **not** the deployment boundary.

A single `tokeira-edge` crate should support at least these two deployment shapes.

### Shape A: single edge process

Useful for development and local bring-up.

```text
client -> edge(all methods) -> runtime/projection
```

### Shape B: split production edge

Preferred for serious environments.

```text
client/sdk
   |
   +--> edge-api  --> runtime/projection
   |
   +--> edge-poll --> runtime/broker
```

That deployment split is where poll isolation becomes real.

## What stays in Edge vs what stays elsewhere

| Concern | Edge? | Why |
|--------|-------|-----|
| Request IDs | Yes | request-scoped concern |
| Authn/Authz | Yes | public boundary concern |
| Namespace resolution | Yes | request admission concern |
| Poll gating | Yes | protects deeper layers |
| HTTP proxy path parsing | Yes | transport-only concern |
| External-to-internal translation | Yes | API shell concern |
| Workflow history ordering | No | kernel/runtime concern |
| Durable task creation | No | runtime/storage concern |
| Request dedupe semantics | No | storage/kernel concern |
| Sticky correctness | No | runtime/broker concern |
| Visibility indexing | No | projection concern |
| DSQL session management | No | storage concern |
| Archival | No | separate service concern |

## Suggested public construction model

The crate should expose composition-oriented builders rather than giant constructors.

For example:

```rust
pub struct EdgeApiBuilder {
    pub workflow_runtime: Arc<dyn WorkflowRuntimeApi>,
    pub execution_resolver: Arc<dyn ExecutionResolver>,
    pub visibility: Arc<dyn VisibilityApi>,
    pub operator_api: Arc<dyn OperatorApi>,
    pub health: Arc<dyn HealthReporter>,
    pub interceptors: Arc<EdgeInterceptors>,
    pub router: Arc<dyn EdgeRouter>,
}

pub struct EdgePollBuilder {
    pub workflow_runtime: Arc<dyn WorkflowRuntimeApi>,
    pub interceptors: Arc<EdgeInterceptors>,
    pub router: Arc<dyn EdgeRouter>,
    pub long_polls: LongPollGate,
}
```

This is not currently implemented, but it would make the intended role split very obvious.

## Migration path from current crate

1. Keep the existing small service contracts.
2. Introduce an explicit role-oriented façade:
   - `EdgeApi`
   - `EdgePoll`
3. Keep `WorkflowService`, `OperatorService`, and `HealthService` as building blocks.
4. Move transport-server startup concerns into the app/binary layer.
5. Add per-role metrics, admission, and overload handling.
6. Add remote forwarding support to `EdgeRouter` users.

This is evolutionary, not a rewrite.

## What this enables

- independent scaling of poll and non-poll traffic,
- earlier overload detection for worker poll storms,
- a much clearer boundary between public API concerns and workflow semantics,
- easier local testing through in-memory trait implementations,
- simpler Codex contributions because contracts are narrow and role-specific,
- and deployment flexibility without splitting the crate itself.

## What this does **not** change

This design does **not** change:

- the runtime/kernal/storage ownership of correctness,
- the broker’s authority over task delivery,
- projection’s ownership of list/count/visibility read models,
- or the broader Tokeira principle that durable history is the source of truth.

It is strictly a clarification of the Edge boundary.

## Complexity assessment

This design adds one important conceptual distinction:

- **one crate**,
- **two deployment roles**.

That indirection is justified because poll traffic and normal API traffic are mechanically different, even though they share most of the same transport shell code.

The risk is that the split becomes over-engineered and starts duplicating handlers or introducing too many role-specific abstractions.

The safeguard is simple:

- keep handlers small,
- keep common interception shared,
- keep routing transport-only,
- keep long-poll admission explicit,
- and treat every proposed semantic change in Edge with suspicion.

If a change makes Edge “smarter” about workflows, it is probably in the wrong crate.

## Recommendation

Keep `tokeira-edge` as a **single compatibility crate**, but explicitly define it as supporting two production roles:

- **`edge-api`** for ordinary public API traffic,
- **`edge-poll`** for long-poll worker traffic.

Preserve the existing small trait surfaces, make role construction explicit, and continue pushing all durable semantics downward into runtime, kernel, storage, and projection.

That keeps Edge thin, scalable, and hard to misuse.
