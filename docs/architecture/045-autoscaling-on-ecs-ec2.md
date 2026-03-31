# 045 Autoscaling on ECS on EC2 (private-only, no CloudWatch)

**Status:** draft for architecture review  
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

### Loop B: EC2 Auto Scaling group desired capacity

This loop is for the **runtime fleet**.

AWS documents that `DAEMON` scheduling places exactly one task on each eligible active container instance, and with `DAEMON` there is no desired count and no Service Auto Scaling policy.[^ecs-daemon]

That is exactly what we want for `tokeira-runtime`:

- one runtime task per host,
- one shard-owning process per host,
- predictable local CPU and memory envelopes,
- no multi-runtime colocation on the same instance unless we explicitly choose it later.

So the runtime scaling lever is **not** ECS desired count. It is the backing Auto Scaling group's desired capacity. AWS documents `SetDesiredCapacity` as the API that changes the desired capacity an Auto Scaling group attempts to maintain.[^asg-set-desired]

The autoscaler action is therefore:

- read runtime pressure from Mimir,
- compute desired host count,
- call `autoscaling:SetDesiredCapacity` on the runtime ASG.

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

Because this service should run exactly once per eligible runtime instance, `DAEMON` is the cleanest ECS fit.[^ecs-daemon]

Scale this plane by changing the **runtime Auto Scaling group desired capacity**, not by changing ECS desired count.

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

- Systems Manager / ECS Exec endpoints,
- CloudWatch Logs endpoints,
- STS endpoints for explicit SDK flows that require them,
- Application Auto Scaling endpoint if you later adopt scheduled actions or native scalable targets.

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

For `tokeira-runtime`, use **ECS service discovery / Cloud Map** so each runtime task can be associated with a concrete, discoverable endpoint. AWS documents that ECS service discovery uses Cloud Map and that private DNS namespaces are visible only inside a specified VPC.[^ecs-service-discovery][^cloudmap-private-ns]

The recommended pattern is:

- runtime tasks register in a private Cloud Map namespace,
- the routing/control plane maps `node_id -> endpoint`,
- the edge routing cache maps `shard_id -> node_id`,
- the edge then resolves that node to a concrete private endpoint.

This keeps runtime routing explicit and compatible with shard-owner semantics.

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
- `autoscaling:SetDesiredCapacity` for runtime host groups.[^asg-set-desired]

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
| `tokeira-runtime` | `DAEMON` | `autoscaling:SetDesiredCapacity` on runtime ASG | one runtime per host |
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
- internal ingress for Edge,
- `tokeira-runtime` as a DAEMON service,
- `tokeira-autoscaler` as a small REPLICA service with DSQL leader lease,
- ECS / ECR / S3 / EC2 Auto Scaling / Cloud Map / DSQL private endpoints provisioned,
- Service Connect for generic service-to-service communication,
- Cloud Map / explicit endpoint registry for runtime node discovery,
- Mimir as the source of scaling truth.

## Bottom line

The scaling model for Tokeira on ECS on EC2 should be:

> **Custom autoscaler, private-only networking, REPLICA services for edge/projection/control, DAEMON runtime on dedicated hosts, and AWS APIs used only as actuators.**

That gives us:

- no CloudWatch dependency in the decision loop,
- no DDB expansion into general control-plane placement,
- strong isolation between poll and non-poll edge traffic,
- a scaling model that matches Tokeira’s actual bottlenecks rather than generic ECS utilization.

## Review questions

1. Do we want `tokeira-runtime` to start as `DAEMON`, or do we want a REPLICA runtime fleet first for denser packing experiments?
2. Do we want to preserve a single Temporal-compatible private endpoint immediately, or accept split private DNS names for `edge-api` and `edge-poll` in the first deployment?
3. Do we want Cloud Map to be the runtime endpoint source of truth, or do we want the controller to publish an explicit endpoint registry derived from ECS task metadata?
4. Should the autoscaler write directly to ECS and EC2 Auto Scaling only, or do we also want an optional Application Auto Scaling integration for scheduled actions later?

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
