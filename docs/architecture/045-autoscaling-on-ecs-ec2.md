# 045 Autoscaling on ECS on EC2 (private-only, no CloudWatch)

**Status:** revised draft  
**Decision direction:** preferred  
**Related docs:** [035-placement-and-membership](035-placement-and-membership.md), [040-delivery-broker](040-delivery-broker.md), [050-dsql-storage](050-dsql-storage.md), [060-connection-management](060-connection-management.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Intent

This note describes how **Tokeira** should scale when deployed on **Amazon ECS on EC2** with **private-only networking** and **no CloudWatch in the scaling decision loop**.

The target state is:

- all application traffic remains private,
- all control-plane API calls remain private,
- workflow execution capacity scales independently of edge and projection capacity,
- worker poll storms cannot starve normal API traffic,
- autoscaling decisions come from **Tokeira-native metrics in Alloy/Mimir**, not CloudWatch,
- DynamoDB remains limited to **DSQL connection-budget control** and is not expanded into the general placement/autoscaling control plane.

This note therefore makes one explicit architectural choice:

> **Tokeira uses a custom `tokeira-autoscaler` service that reads Mimir and writes scaling decisions directly to ECS and EC2 Auto Scaling APIs.**

## Autoscaling invariants

The autoscaler must preserve these invariants:

1. **Scaling decisions are advisory.** DSQL lease fencing remains the authoritative ownership mechanism. A scaling mistake cannot corrupt workflow state.
2. **Runtime scale-in must be placement-aware and drain-aware.** A runtime node must not be terminated intentionally while it owns bundles, unless the system has declared an emergency forced-failover path.
3. **Missing metrics must never trigger scale-in.** Absent or stale data is unknown, not zero.
4. **DSQL connection headroom is a hard scaling envelope.** Runtime scale-out must not proceed if projected connections would exceed the safe connection budget.
5. **Poll traffic and non-poll API traffic must remain separately scalable.** The edge-api / edge-poll split is a core design principle.
6. **Projection lag must not affect authoritative workflow correctness.** Projection is outside the correctness path; its scaling is independent.
7. **Autoscaler unavailability must not affect workflow correctness.** It only pauses automatic capacity changes. Existing runtime owners continue renewing leases, edges continue using cached routing.

## Why not use native ECS autoscaling?

AWS documents two native autoscaling paths that are relevant to ECS on EC2:

1. **ECS Service Auto Scaling** for ECS services, which uses **Application Auto Scaling** and CloudWatch metrics/alarms.[^ecs-service-autoscaling]
2. **ECS cluster auto scaling** for EC2 capacity providers, which uses the `CapacityProviderReservation` CloudWatch metric and target tracking on the backing Auto Scaling group.[^ecs-cluster-autoscaling]

Those are good default AWS mechanisms, but they are not the right fit for Tokeira because we want:

- scaling decisions based on **workflow-system pressure**, not generic CPU/utilization alone,
- the control loop driven by **Mimir** instead of CloudWatch,
- more control over **fast scale-out / slow scale-in**,
- direct awareness of **poll pressure**, **projection lag**, **lane pressure**, and **DSQL connection headroom**.

So the decision is:

- **Do not use ECS Service Auto Scaling for decision-making.**
- **Do not use ECS managed cluster auto scaling for the runtime plane.**
- **Do use ECS and EC2 Auto Scaling as the actuators.**

In other words:

- **Mimir decides**,
- **AWS APIs enact**.

## Control loops

Tokeira should have **two different scaling loops**, because ECS has two different scaling surfaces.

### Loop A: ECS service desired count

This loop is for **REPLICA** services. AWS documents that `REPLICA` scheduling places and maintains the desired number of tasks for a service, and `UpdateService` can change that desired count.[^ecs-replica][^ecs-update-service]

Use this loop for:

- `tokeira-edge-api`
- `tokeira-edge-poll`
- `tokeira-projection`
- `tokeira-controller`
- `tokeira-autoscaler`
- `tokeira-admin`

The autoscaler action is simply:

- read current pressure from Mimir,
- compute `desiredCount`,
- call `ecs:UpdateService`.

### Loop B: EC2 Auto Scaling group desired capacity (runtime scale-out)

This loop handles **runtime scale-out only**.

AWS documents that `DAEMON` scheduling places exactly one task on each eligible active container instance, and with `DAEMON` there is no desired count and no Service Auto Scaling policy.[^ecs-daemon]

That is the preferred first production profile for `tokeira-runtime`:

- one runtime task per host,
- one shard-owning process per host,
- predictable local CPU and memory envelopes,
- no multi-runtime colocation on the same instance unless we explicitly choose it later.

The runtime correctness model must not depend on DAEMON scheduling. DAEMON is the initial placement profile because it gives one runtime incarnation per EC2 host and simplifies capacity accounting. The lease and placement model must continue to work if runtime later becomes REPLICA with explicit placement constraints.

So the runtime scale-out lever is **not** ECS desired count. It is the backing Auto Scaling group's desired capacity. AWS documents `SetDesiredCapacity` as the API that changes the desired capacity an Auto Scaling group attempts to maintain.[^asg-set-desired]

The autoscaler scale-out action is:

- read runtime pressure from Mimir,
- verify DSQL connection headroom is sufficient (see [Connection-aware scaling envelope](#connection-aware-scaling-envelope)),
- verify pressure is broad saturation, not hot-bundle imbalance (see [Runtime scale-out decision](#runtime-scale-out-decision)),
- compute desired host count,
- call `autoscaling:SetDesiredCapacity` on the runtime ASG.

New instances launch with **instance scale-in protection enabled by default**. The autoscaler clears protection only after Tokeira has drained the node (see Loop C). AWS documents instance scale-in protection for Auto Scaling groups.[^asg-instance-protection]

ECS managed termination protection is not sufficient for the runtime design because AWS notes that DAEMON tasks are ignored for managed termination protection purposes.[^ecs-daemon] Tokeira must own scale-in protection explicitly through the ASG instance protection API.

### Loop C: runtime retirement (runtime scale-in)

Runtime scale-in is **not** a raw desired-capacity operation. For the runtime plane, scale-out and scale-in are asymmetric:

- **scale-out** increases the runtime ASG desired capacity,
- **scale-in** retires selected runtime nodes after Tokeira-level drain.

The autoscaler MUST NOT reduce the runtime ASG desired capacity and allow Auto Scaling to choose an arbitrary runtime instance while that instance may own bundles. AWS Auto Scaling termination policies are not Tokeira-aware.[^asg-termination-policies]

The safe scale-in protocol:

1. **Autoscaler decides** the runtime fleet has excess capacity based on fresh Mimir metrics and a fresh controller snapshot.
2. **Autoscaler asks the controller** for candidate nodes. The controller selects nodes with:
   - lowest owned-bundle count,
   - lowest runnable-lane pressure,
   - no active sweep or repair,
   - enough remaining AZ capacity after removal.
3. **Controller marks selected nodes `DRAINING`:**
   - no new bundle placement,
   - no new queue-home assignment,
   - no new actors except takeover/repair.
4. **Runtime voluntarily releases or transfers work:**
   - stops acquiring new bundles,
   - renews current leases only until transfer completes,
   - moves queue-home broker ownership,
   - flushes projection/dispatch handoff,
   - rejects or forwards owner-targeted traffic with stale-owner metadata.
5. **Autoscaler sets the ECS container instance to `DRAINING`.** AWS documents that when an instance is set to DRAINING, ECS prevents new tasks from being scheduled there.[^ecs-container-instance-draining]
6. **Wait until the runtime heartbeat reports safe to terminate:**
   - `bundle_count == 0`,
   - `active_run_actor_count == 0` (or below a forced threshold),
   - open internal requests drained.
7. **Autoscaler clears instance scale-in protection** for the selected EC2 instance.
8. **Autoscaler terminates the instance** using `TerminateInstanceInAutoScalingGroup` with `ShouldDecrementDesiredCapacity=true`. AWS documents this API and its decrement parameter.[^asg-terminate-instance]

This gives Tokeira control over **which** runtime node leaves, rather than letting Auto Scaling choose arbitrarily.

#### Autoscaler vs controller responsibility split

The autoscaler and controller have complementary but distinct roles:

| Concern | Owner |
|---|---|
| Desired capacity envelopes | Autoscaler |
| AWS API calls (ECS, ASG) | Autoscaler |
| Cooldowns, min/max, rate limits | Autoscaler |
| Placement state and node drain state | Controller |
| Ownership movement decisions | Controller |
| Routing map publication | Controller |
| Safe scale-in candidate nomination | Controller |

The autoscaler does not decide which bundle moves. The controller does not change ASG size.

## Recommended ECS cluster and capacity-provider shape

Use **one ECS cluster per environment** and multiple EC2-backed capacity providers.

AWS documents that EC2-backed ECS capacity is managed through Auto Scaling group capacity providers.[^ecs-capacity-providers]

Recommended capacity providers:

- `cp-edge-api`
- `cp-edge-poll`
- `cp-runtime`
- `cp-projection`
- `cp-control`

This isolation is deliberate.

The two most important separations are:

1. **Edge poll traffic must not share hosts with runtime ownership by default.**
2. **Projection should scale independently and tolerate lag without affecting correctness.**

## Recommended service definitions

### `tokeira-edge-api`

**Scheduling:** `REPLICA`  
**Capacity provider:** `cp-edge-api`  
**Ingress:** private-only, internal L7 ingress  
**Purpose:** normal request/response APIs

Responsibilities:

- public Temporal-compatible API surface for non-poll methods,
- authn/authz,
- namespace resolution,
- request ID handling,
- routing to the correct runtime owner,
- read-path fanout to projection/visibility.

Scale this service from:

- in-flight non-poll RPCs,
- p95/p99 latency,
- request rejection rate,
- queue depth inside the edge process.

### `tokeira-edge-poll`

**Scheduling:** `REPLICA`  
**Capacity provider:** `cp-edge-poll`  
**Ingress:** private-only, separate target group / route from `edge-api`  
**Purpose:** long-poll worker traffic only

Responsibilities:

- `PollWorkflowTaskQueue`
- `PollActivityTaskQueue`
- memory-only waiter registration,
- long-poll admission control,
- handoff to the delivery broker.

This service exists specifically to stop worker pollers from overwhelming the general edge fleet.

Scale this service from:

- open long polls,
- admitted polls per second,
- rejected polls per second,
- broker handoff latency,
- process memory,
- executor saturation / event-loop lag.

### `tokeira-runtime`

**Scheduling:** `DAEMON`  
**Capacity provider:** `cp-runtime`  
**Ingress:** no external ingress; owner-targeted internal traffic only  
**Purpose:** shard ownership, lanes, workflow actors, transition commits

Responsibilities:

- acquire shard leases and epochs in DSQL,
- host lane-local workflow actors,
- commit authoritative transitions,
- publish dispatch and projection mutations,
- perform local sweep/repair.

Because this service should run exactly once per eligible runtime instance, `DAEMON` is the preferred first production profile.[^ecs-daemon]

Scale this plane by changing the **runtime Auto Scaling group desired capacity** for scale-out, and by using the **runtime retirement protocol** (Loop C) for scale-in.

### `tokeira-projection`

**Scheduling:** `REPLICA`  
**Capacity provider:** `cp-projection`  
**Ingress:** internal only  
**Purpose:** visibility and custom projection workers

Scale from:

- projection lag,
- oldest unapplied mutation age,
- batch apply latency,
- sink-specific backpressure.

### `tokeira-controller`

**Scheduling:** `REPLICA`  
**Capacity provider:** `cp-control`  
**Typical size:** 2 tasks  
**Purpose:** advisory placement and routing publication

Responsibilities:

- compute or republish desired shard ownership,
- publish shard-owner maps for edges,
- coordinate rebalance plans,
- avoid acting as a correctness authority.

The controller can be small because DSQL lease epochs remain authoritative.

### `tokeira-autoscaler`

**Scheduling:** `REPLICA`  
**Capacity provider:** `cp-control`  
**Typical size:** 2 tasks with one active leader  
**Purpose:** read Mimir, write scaling decisions

Responsibilities:

- query Mimir / Prometheus-compatible API,
- compute target desired counts and ASG sizes,
- apply decisions through ECS and EC2 Auto Scaling APIs,
- enforce hysteresis, cooldowns, and policy envelopes.

Use a **small DSQL leader lease** so that only one autoscaler task writes decisions at a time. This keeps DDB out of the general control plane while still giving high availability.

### `tokeira-admin`

**Scheduling:** `REPLICA` or on-demand task  
**Capacity provider:** `cp-control`  
**Purpose:** schema admin, repair, backfill, diagnostics

This does not need autoscaling in the first iteration.

## Private-only networking model

The private-only deployment should use:

- private subnets only for ECS tasks and EC2 instances,
- no internet gateway dependency for service control paths,
- VPC endpoints / PrivateLink where required,
- internal-only ingress for Edge.

AWS documents that ECS, ECR, EC2 Auto Scaling, Application Auto Scaling, AWS Cloud Map, and Aurora DSQL can all be reached privately through VPC endpoints / PrivateLink; gateway endpoints are available for S3 and DynamoDB.[^ecs-vpc-endpoints][^ecr-vpc-endpoints][^ec2-asg-vpce][^app-asg-vpce][^cloudmap-vpce][^s3-gateway][^dsql-privatelink]

### Required AWS private endpoints for the first Tokeira deployment

At minimum, a private-only ECS on EC2 Tokeira cluster should provision:

#### ECS control plane

AWS documents that private ECS control paths require three ECS interface endpoints and that if all are not configured, traffic can fall back to public endpoints.[^ecs-vpc-endpoints]

- `com.amazonaws.<region>.ecs`
- `com.amazonaws.<region>.ecs-agent`
- `com.amazonaws.<region>.ecs-telemetry`

#### ECR image pull path

AWS documents ECR interface endpoints for private API access, and recommends private connectivity via PrivateLink.[^ecr-vpc-endpoints]

- `com.amazonaws.<region>.ecr.api`
- `com.amazonaws.<region>.ecr.dkr`

#### S3 gateway endpoint

ECR image pulls also depend on S3-backed layer transfer paths. AWS recommends S3 gateway endpoints for private VPC service access, and ECS networking guidance lists S3 as a common endpoint in VPC-endpoint-based ECS deployments.[^ecs-vpc-best-practices][^s3-gateway]

- `com.amazonaws.<region>.s3` (gateway endpoint)

#### EC2 Auto Scaling endpoint

The custom autoscaler will directly change runtime host counts using EC2 Auto Scaling.[^asg-set-desired]

- `com.amazonaws.<region>.autoscaling`

#### Cloud Map endpoint (recommended)

If Tokeira uses Cloud Map API-based discovery for runtime nodes, AWS documents that Cloud Map supports interface endpoints and private DNS namespaces.[^cloudmap-vpce][^cloudmap-private-ns]

- `com.amazonaws.<region>.servicediscovery`

#### Aurora DSQL PrivateLink

Aurora DSQL now supports AWS PrivateLink and requires separate **management** and **connection** endpoint types for private management and PostgreSQL client connectivity.[^dsql-privatelink]

Provision the DSQL endpoints required by the chosen connectivity pattern before declaring the environment private-only.

### Optional endpoints

Add these only if you use the corresponding AWS features:

- **STS endpoint** (`com.amazonaws.<region>.sts`) — include if using AssumeRole, Pod Identity, or explicit STS calls from tasks.
- **KMS endpoint** (`com.amazonaws.<region>.kms`) — include if pulling encrypted secrets or decrypting config at runtime.
- **Secrets Manager endpoint** (`com.amazonaws.<region>.secretsmanager`) — include if task bootstrap reads secrets from Secrets Manager.
- **SSM endpoint** (`com.amazonaws.<region>.ssm`) — include if using SSM Parameter Store or ECS Exec.
- **CloudWatch Logs endpoint** (`com.amazonaws.<region>.logs`) — include only if using the `awslogs` log driver; otherwise use Alloy/log shipping path.
- **EC2 endpoint** (`com.amazonaws.<region>.ec2`) — include if the autoscaler or controller calls `DescribeInstances`, `DescribeNetworkInterfaces`, or related APIs directly.
- **Application Auto Scaling endpoint** — include if you later adopt scheduled actions or native scalable targets.

## Service discovery and routing

AWS recommends **Service Connect** for ECS service-to-service connectivity and discovery.[^ecs-service-connect][^ecs-networking-services]

Tokeira should use that recommendation selectively.

### Use Service Connect for generic service-to-service traffic

Recommended for:

- `edge-api` → `projection`
- `controller` → `projection`
- `autoscaler` → other control services
- `admin` → internal services

These are ordinary service-to-service flows that do not care which specific task instance is chosen.

### Do not rely on Service Connect alone for owner-targeted runtime routing

Tokeira edges often need to talk to the **specific runtime node that owns a shard**.

That is not a generic load-balanced service lookup problem. It is an **identity-aware endpoint selection** problem.

Cloud Map is not authoritative for ownership or routing. The controller publishes `node_id → endpoint` as part of the routing snapshot. Cloud Map / ECS metadata may be used to discover or validate the concrete endpoint, but the source of truth for `bundle_id → node_id → endpoint` is the controller's placement state.

For `tokeira-runtime`, use **ECS service discovery / Cloud Map** as discovery plumbing so each runtime task can be associated with a concrete, discoverable endpoint. AWS documents that ECS service discovery uses Cloud Map and that private DNS namespaces are visible only inside a specified VPC.[^ecs-service-discovery][^cloudmap-private-ns]

The recommended pattern is:

- runtime tasks register in a private Cloud Map namespace,
- the controller publishes the authoritative `node_id → endpoint` mapping as part of the routing snapshot,
- the edge routing cache maps `shard_id → node_id`,
- the edge then resolves that node to a concrete private endpoint.

This keeps runtime routing explicit and compatible with shard-owner semantics, and avoids a subtle future bug where DNS resolution and ownership routing become confused.

## Edge isolation: how Tokeira avoids poller overload

This is one of the primary reasons for the ECS service split.

Temporal can be heavily stressed by worker pollers because long-poll traffic shares too much of the general frontend path. Tokeira addresses this by design.

### Rule 1: split poll and non-poll fleets

`edge-api` and `edge-poll` are separate ECS services, with separate scaling loops and separate capacity providers.

That means a worker poll storm cannot directly consume:

- the CPU budget of non-poll APIs,
- the task count envelope of non-poll APIs,
- the memory reserved for non-poll request handling.

### Rule 2: long polls are memory-only waiters

A poll request admitted by `edge-poll` should not:

- hold a DSQL connection,
- hold a worker thread,
- allocate durable queue state.

It should hold only:

- a socket / HTTP2 stream,
- a lightweight waiter object,
- a deadline timer,
- a broker registration.

### Rule 3: enforce admission at the edge

`edge-poll` must enforce a `LongPollGate` with limits such as:

- global max open polls,
- per-namespace max open polls,
- per-task-queue max open polls,
- per-worker-identity or per-source limits.

If the gate is saturated, reject immediately with retryable overload semantics.

### Rule 4: route poll methods separately

If preserving a single Temporal-compatible endpoint is important, use an **internal Application Load Balancer** with gRPC routing rules. AWS documents that ALB can parse gRPC and route based on package, service, and method.[^alb-grpc-routing]

That makes the following split practical even on one private endpoint:

- `PollWorkflowTaskQueue` → `edge-poll`
- `PollActivityTaskQueue` → `edge-poll`
- all other methods → `edge-api`

If a split endpoint is acceptable initially, a simpler first deployment is:

- `edge-api.<private-zone>`
- `edge-poll.<private-zone>`

and configure workers explicitly to use the poll endpoint.

## Autoscaler design

`tokeira-autoscaler` should be a small control-plane service, not a huge platform.

### Inputs

- Mimir queries over private networking,
- optional controller snapshots,
- optional DSQL connection-headroom summaries,
- current ECS service state,
- current ASG state.

### Outputs

- `ecs:UpdateService` for REPLICA services,[^ecs-update-service]
- `autoscaling:SetDesiredCapacity` for runtime scale-out,[^asg-set-desired]
- `autoscaling:TerminateInstanceInAutoScalingGroup` for runtime scale-in.[^asg-terminate-instance]

### Metric freshness and degraded autoscaling

The autoscaler treats stale or missing metrics as **unknown**, not as zero.

Rules:

1. **Missing data is not zero.** An absent metric series must never be interpreted as "no load."
2. **No scale-in from stale metrics.** If the most recent Mimir sample for a scaling input is older than the staleness threshold, scale-in is blocked for that plane.
3. **Runtime scale-in requires a fresh controller snapshot.** The controller must have reported node placement state within the staleness window.
4. **Runtime scale-out requires fresh DSQL connection-headroom data** unless an emergency override is active.
5. **Scale-out may proceed from partial high-confidence overload signals.** If some metrics are missing but available signals clearly indicate overload, scale-out is allowed.
6. **If Mimir is unavailable, freeze desired capacity** except for explicit operator actions or emergency floor restoration.

| Condition | Scale out | Scale in |
|---|---|---|
| Mimir healthy, metrics fresh | allowed | allowed |
| Mimir unavailable | emergency/manual only | no |
| Metric series missing | maybe, with fallback | no |
| Controller snapshot stale | edge/projection only | no runtime scale-in |
| DSQL headroom unknown | constrained | no runtime scale-out beyond floor |
| AWS API throttled | backoff | backoff |

### Connection-aware scaling envelope

DSQL connection headroom is a **hard guardrail**, not merely a scaling input. See [060-connection-management](060-connection-management.md).

Runtime scale-out is allowed only if:

- `projected_runtime_connections_after_scale <= safe_connection_budget`
- `projected_new_connection_rate_after_scale <= safe_connection_rate`

The effective maximum runtime host count is:

```
effective_max_runtime_hosts = min(
    configured_max_runtime_hosts,
    floor(dsql_connection_budget / per_runtime_reserved_connections),
    floor(dsql_new_connection_rate_budget / per_runtime_startup_connection_rate)
)
```

Without this guardrail, the autoscaler can create a death spiral: runtime pressure rises → autoscaler adds hosts → new hosts open DSQL pools → connection pressure rises → commit latency worsens → autoscaler adds more hosts.

### AWS actuator reconciliation

The autoscaler should maintain desired state and reconcile on each loop, not fire-and-forget:

- `service_name → desired_count`
- `asg_name → desired_capacity`
- `instance_id → drain/terminate intent`

Reconciliation rules:

- do not issue `UpdateService` if `desiredCount` already matches the target,
- do not issue `SetDesiredCapacity` repeatedly for the same target,
- handle eventual consistency from `DescribeServices` / `DescribeAutoScalingGroups`,
- back off on AWS API throttling,
- record every scaling decision with input metrics and reason for auditability.

### Control-loop rules

Use three simple rules from day one:

1. **Scale out fast**
2. **Scale in slowly**
3. **Never scale from a single sample**

A reasonable first cut is:

- polling interval: 15–30 seconds,
- scale-out requires 1–2 consecutive bad samples,
- scale-in requires 5–10 consecutive good samples,
- enforce per-service min/max floors,
- enforce per-step maximum delta,
- respect deployment/change windows.

### Runtime scaling inputs

Scale the runtime Auto Scaling group from **Tokeira-native pressure**, not generic host CPU alone.

Recommended signals:

- runnable transitions queued per lane,
- hot shard imbalance,
- shard count per node,
- commit latency,
- serialization conflict rate,
- sweeper backlog,
- pending task-start count,
- DSQL active-connection headroom,
- DSQL new-connection-rate headroom.

### Runtime scale-out decision

Runtime scale-out is appropriate only when pressure is **broad enough** that new hosts can help. A single hot bundle or hot queue partition should first be handled by placement rebalance or partition split (see [037-dynamic-placement](037-dynamic-placement.md)).

The autoscaler should classify runtime pressure as:

- **broad saturation** — most nodes are under pressure, adding hosts helps,
- **hot-node imbalance** — a few nodes are overloaded, rebalance helps,
- **hot-bundle imbalance** — a few bundles are hot, partition split helps,
- **DSQL-bound** — connection or commit headroom is the bottleneck, adding hosts makes it worse,
- **admission-bound** — the system should shed load, not add capacity.

Only broad saturation with sufficient DSQL headroom should directly increase the runtime ASG size.

### Edge API scaling inputs

Recommended signals:

- in-flight non-poll RPCs,
- p95/p99 latency,
- reject rate,
- CPU only as a secondary guardrail.

### Edge poll scaling inputs

Recommended signals:

- open long polls,
- average waiter lifetime,
- admitted polls/sec,
- rejected polls/sec,
- broker handoff latency,
- memory pressure.

### Projection scaling inputs

Recommended signals:

- projection lag in seconds,
- oldest unapplied mutation age,
- sink apply latency,
- sink failure/retry rate.

## Operational guidance

### Default scheduling recommendations

| Service | ECS strategy | Scaling actuator | Default note |
|---|---|---|---|
| `tokeira-edge-api` | `REPLICA` | `ecs:UpdateService` | normal API traffic |
| `tokeira-edge-poll` | `REPLICA` | `ecs:UpdateService` | long-poll traffic only |
| `tokeira-runtime` | `DAEMON` | scale-out: `SetDesiredCapacity`; scale-in: Loop C retirement | one runtime per host |
| `tokeira-projection` | `REPLICA` | `ecs:UpdateService` | lag-tolerant plane |
| `tokeira-controller` | `REPLICA` | static or `ecs:UpdateService` | small fleet |
| `tokeira-autoscaler` | `REPLICA` | static | 2 tasks, one active leader |
| `tokeira-admin` | `REPLICA` or on-demand | static | rarely scaled |

### What we are explicitly not doing

- No CloudWatch metrics in the decision loop.
- No ECS managed cluster auto scaling for runtime.
- No Application Auto Scaling target tracking for the first iteration.
- No DDB-based placement or membership expansion beyond DSQL connection-budget control.

## Recommended first deployment shape

The initial ECS-on-EC2, private-only deployment should look like this:

- one ECS cluster per environment,
- private subnets only,
- capacity providers: `cp-edge-api`, `cp-edge-poll`, `cp-runtime`, `cp-projection`, `cp-control`,
- internal ingress for Edge, with split private DNS names (`edge-api.<private-zone>`, `edge-poll.<private-zone>`),
- `tokeira-runtime` as a DAEMON service with ASG instance scale-in protection enabled by default,
- `tokeira-autoscaler` as a small REPLICA service with DSQL leader lease,
- ECS / ECR / S3 / EC2 Auto Scaling / Cloud Map / DSQL private endpoints provisioned,
- Service Connect for generic service-to-service communication,
- controller-published endpoint registry for runtime node discovery (Cloud Map as discovery plumbing, not source of truth),
- Mimir as the source of scaling truth,
- direct ECS and EC2 Auto Scaling APIs only (no Application Auto Scaling in the first iteration).

## Bottom line

The scaling model for Tokeira on ECS on EC2 should be:

> **Custom autoscaler, private-only networking, isolated REPLICA services for edge/projection/control, DAEMON runtime as the first runtime placement profile, Mimir/controller/DSQL signals as scaling inputs, and AWS APIs used only as capacity actuators.**

Runtime scale-out is an ASG desired-capacity action. Runtime scale-in is a Tokeira node-retirement workflow.

That gives us:

- no CloudWatch dependency in the decision loop,
- no DDB expansion into general control-plane placement,
- strong isolation between poll and non-poll edge traffic,
- safe runtime scale-in that respects bundle ownership and drain state,
- DSQL connection headroom as a hard scaling envelope,
- a scaling model that matches Tokeira’s actual bottlenecks rather than generic ECS utilization.

## Resolved review questions

1. **DAEMON or REPLICA runtime first?** Start with DAEMON for the first ECS-on-EC2 profile. DAEMON is a placement profile, not a correctness dependency. Correctness comes from DSQL leases and epochs. DAEMON gives clean host/process accounting.
2. **Single Temporal-compatible endpoint immediately?** Start with split private names (`edge-api.<private-zone>`, `edge-poll.<private-zone>`). Move to a single ALB endpoint with gRPC method routing once the internals are stable.
3. **Cloud Map or controller endpoint registry?** Controller-published registry. Cloud Map is discovery plumbing. The controller publishes `node_id → endpoint` as part of the routing snapshot. Cloud Map / ECS metadata may be used to discover or validate the concrete endpoint.
4. **Direct ECS/ASG only, or Application Auto Scaling later?** Direct ECS and EC2 Auto Scaling APIs only for the first iteration. Keep Application Auto Scaling as a future optional integration for scheduled floors or non-runtime service envelopes.

## References

[^ecs-service-autoscaling]: AWS, "Automatically scale your Amazon ECS service" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-auto-scaling.html>
[^ecs-cluster-autoscaling]: AWS, "Automatically manage Amazon ECS capacity with cluster auto scaling" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/cluster-auto-scaling.html>
[^ecs-capacity-providers]: AWS, "Amazon ECS capacity providers for EC2 workloads" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/asg-capacity-providers.html>
[^ecs-daemon]: AWS, "Amazon ECS service deployment controllers and strategies" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_service-options.html>
[^ecs-replica]: AWS, "Amazon ECS service definition parameters" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service_definition_parameters.html>
[^ecs-update-service]: AWS, "UpdateService - Amazon Elastic Container Service" — <https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_UpdateService.html>
[^asg-set-desired]: AWS, "SetDesiredCapacity - Amazon EC2 Auto Scaling" — <https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_SetDesiredCapacity.html>
[^ecs-service-connect]: AWS, "Use Service Connect to connect Amazon ECS services" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-connect.html>
[^ecs-networking-services]: AWS, "Best practices for connecting Amazon ECS services in a VPC" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/networking-connecting-services.html>
[^ecs-vpc-endpoints]: AWS, "Amazon ECS interface VPC endpoints (AWS PrivateLink)" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/vpc-endpoints.html>
[^ecr-vpc-endpoints]: AWS, "Amazon ECR interface VPC endpoints (AWS PrivateLink)" — <https://docs.aws.amazon.com/AmazonECR/latest/userguide/vpc-endpoints.html>
[^s3-gateway]: AWS, "Gateway endpoints" — <https://docs.aws.amazon.com/vpc/latest/privatelink/gateway-endpoints.html>
[^ecs-vpc-best-practices]: AWS, "Best practices for connecting Amazon ECS to AWS services in a VPC" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/networking-connecting-vpc.html>
[^ec2-asg-vpce]: AWS, "Amazon EC2 Auto Scaling and interface VPC endpoints" — <https://docs.aws.amazon.com/autoscaling/ec2/userguide/ec2-auto-scaling-vpc-endpoints.html>
[^app-asg-vpce]: AWS, "Access Application Auto Scaling using interface VPC endpoints" — <https://docs.aws.amazon.com/autoscaling/application/userguide/application-auto-scaling-vpc-endpoints.html>
[^cloudmap-vpce]: AWS, "Access AWS Cloud Map using an interface endpoint (AWS PrivateLink)" — <https://docs.aws.amazon.com/cloud-map/latest/dg/vpc-interface-endpoints.html>
[^ecs-service-discovery]: AWS, "Use service discovery to connect Amazon ECS services with DNS names" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-discovery.html>
[^cloudmap-private-ns]: AWS, "CreatePrivateDnsNamespace - AWS Cloud Map" — <https://docs.aws.amazon.com/cloud-map/latest/api/API_CreatePrivateDnsNamespace.html>
[^alb-grpc-routing]: AWS, "Target groups for your Application Load Balancers" — <https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html>
[^dsql-privatelink]: AWS, "Managing and connecting to Amazon Aurora DSQL clusters using AWS PrivateLink" — <https://docs.aws.amazon.com/aurora-dsql/latest/userguide/privatelink-managing-clusters.html>
[^asg-instance-protection]: AWS, "Use instance scale-in protection" — <https://docs.aws.amazon.com/autoscaling/ec2/userguide/ec2-auto-scaling-instance-protection.html>
[^asg-termination-policies]: AWS, "Configure termination policies for Amazon EC2 Auto Scaling" — <https://docs.aws.amazon.com/autoscaling/ec2/userguide/ec2-auto-scaling-termination-policies.html>
[^asg-terminate-instance]: AWS, "TerminateInstanceInAutoScalingGroup - Amazon EC2 Auto Scaling" — <https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_TerminateInstanceInAutoScalingGroup.html>
[^ecs-container-instance-draining]: AWS, "Drain Amazon ECS container instances" — <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/container-instance-draining.html>
