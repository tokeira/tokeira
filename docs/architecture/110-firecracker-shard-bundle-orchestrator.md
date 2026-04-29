# 110 — Firecracker Shard-Bundle Orchestrator

Status: Draft for review  
Scope: Exploratory platform option  
Primary comparison: ECS on EC2 runtime services  
Position: Not the initial Tokeira substrate; candidate future execution substrate for isolated or high-locality bundle groups

## Problem

Tokeira's current platform direction is ECS on EC2:

- `tokeira-edge-api` and `tokeira-edge-poll` are horizontally scaled API/poll ingress services.
- `tokeira-runtime` owns lanes, run actors, bundle leases, and authoritative transition commits.
- `tokeira-projection` and `tokeira-archival` operate outside the correctness path.
- DSQL remains the authoritative workflow store.

This shape is straightforward, operationally familiar, and a good initial target. However, it may leave performance and isolation opportunities on the table for workloads that would benefit from stronger compute isolation, tighter bundle locality, and long-lived per-bundle execution environments.

The question explored here is:

> Could Tokeira orchestrate Firecracker microVMs as reusable execution environments for shard bundles or bundle ranges, and if so, what performance lift might be realistic compared with ECS on EC2?

## Short Answer

Firecracker should not replace ECS as the first Tokeira control-plane/runtime substrate.

A better interpretation is:

> ECS on EC2 remains the platform for core services. Firecracker becomes an optional, later execution substrate for selected bundle groups.

The strongest use case is a **Firecracker bundle slot**: a reusable microVM assigned to a bundle range for a lease period. The slot hosts a small `tokeira-bundle-runtime` process that can keep bundle-local caches, actor state, sticky delivery state, and DSQL clients warm.

The correctness rule remains unchanged:

> Firecracker placement is locality. DSQL bundle lease epochs are authority.

## Inspiration: Lambda's Firecracker Orchestration Pattern

The AWS Lambda Firecracker paper describes a multi-layer orchestration path:

```text
Frontend
  -> Worker Manager
  -> Placement Service
  -> Worker hosts
  -> Firecracker microVM slots
```

The frontend is a shared-nothing fleet. The Worker Manager is a high-volume, low-latency stateful router that sticky-routes invocations for a function to as few workers as possible. Workers expose reusable slots. When no slot exists, the Worker Manager asks Placement to create one. Placement optimizes slot placement across CPU, memory, network, and storage, usually under a 20 ms placement task, then leases the slot back to Worker Manager for autonomous routing. The Firecracker paper is also clear that Firecracker itself does not provide orchestration, packaging, management, or metadata services; those must be supplied by higher-level infrastructure.[^agache]

Tokeira can borrow the pattern, but not the exact semantics. Lambda slots are for functions. Tokeira slots would be for bundle ranges.

## Proposed Tokeira Mapping

```text
Tokeira Edge
  -> Bundle Manager
  -> Bundle Placement Service
  -> Firecracker Host Agents
  -> Bundle Slot microVMs
  -> DSQL fenced commits
```

| Lambda concept | Tokeira analogue | Meaning |
|---|---|---|
| Frontend | Tokeira Edge | API/poll ingress; shared-nothing; not authoritative |
| Worker Manager | Bundle Manager | Sticky router for bundle slots |
| Placement Service | Bundle Placement Service | Metrics-driven slot allocator |
| Worker host | Firecracker EC2 host | Runs host agent and microVMs |
| Slot | Bundle slot | Reusable microVM for a bundle range |
| Function | Bundle group / queue partition / hot placement group | Unit of locality, not semantic authority |

## Non-Goals

This design should not initially attempt to:

- run every Tokeira service inside Firecracker;
- replace ECS service deployment for Edge, Projection, Autoscaler, Archival, or Orchestrator;
- move authority from DSQL into Firecracker;
- run one microVM per workflow;
- implement a production Firecracker control plane before the ECS baseline is proven.

## Platform Prerequisites

Firecracker requires KVM. Historically, that pushed EC2 deployments toward `.metal` instances, and the Firecracker getting-started docs still present `.metal` EC2 as the opinionated EC2 path.[^fc-start]

In 2026, AWS added nested virtualization support for selected non-bare-metal EC2 instance families. The current EC2 docs say nested virtualization is supported on C8i, M8i, and R8i instance types, and KVM is one of the supported L1 hypervisors.[^ec2-nested]

For Tokeira this means there are two realistic EC2 deployment profiles:

1. **Bare-metal Firecracker fleet**
   - Most proven Firecracker-on-EC2 route.
   - Larger instance shapes and coarser capacity increments.
   - Cleaner KVM model.

2. **Nested-virtualization Firecracker fleet**
   - More flexible instance sizing.
   - Newer operational surface.
   - Requires benchmarking; nested virtualization overhead must be measured for Tokeira's workload.

## Core Design Rule

A Firecracker slot is not a shard owner by itself.

It is a leased execution environment that may acquire and use a DSQL bundle lease.

```text
Placement lease = route to this microVM for locality
DSQL lease      = permission to commit authoritative state
```

The placement lease may be stale. The DSQL epoch fence may not.

## Bundle Slot Granularity

There are three possible slot granularities.

### Option A — One slot per bundle

Strong locality and simple ownership. Too many microVMs if bundle count is large.

Best for very hot bundles.

### Option B — One slot per bundle range

A microVM owns a small contiguous or hashed set of bundles. This is the best default. It amortizes VM/runtime cost while keeping migration and hot-splitting possible.

### Option C — One slot per queue partition

Useful if worker queue pressure dominates. Less natural for execution-scoped authority unless queue-home and execution-home are well aligned.

Recommendation:

> Default to bundle ranges, then split hot bundles into dedicated slots when metrics justify it.

## Bundle Slot Contents

A bundle slot microVM would run:

```text
tokeira-bundle-runtime
  - lane executor(s)
  - run actor cache
  - bundle-local timer wake handling
  - broker-facing local delivery endpoint
  - DSQL commit client
  - projection/event publisher client
  - metrics/logs/traces side channel
```

It should not include:

- the global placement controller;
- the Edge API;
- global visibility query service;
- DSQL authority beyond the current fenced bundle lease.

## Main Components

### 1. Tokeira Edge

Edge remains shared-nothing.

Responsibilities:

- authenticate/authorize requests;
- handle API and poll admission;
- resolve execution-home or queue-home routing;
- forward to Bundle Manager or normal ECS runtime;
- retry on stale routing responses.

Edge should not know Firecracker details. It should only understand execution cells, bundle routing, request deadlines, and admission policy.

### 2. Bundle Manager

The Bundle Manager is the Tokeira analogue of Lambda's Worker Manager.

Responsibilities:

- maintain a route cache from bundle range to slot;
- sticky-route work to warm slots;
- enforce per-bundle concurrency gates;
- detect stale or unhealthy slots;
- ask Placement for a new slot on miss;
- fall back to ECS runtime if Firecracker slot placement fails or is disabled.

Example route record:

```rust
struct BundleRoute {
    cell_id: CellId,
    bundle_range: BundleRange,
    host_id: HostId,
    slot_id: SlotId,
    placement_epoch: u64,
    valid_until: Instant,
    dsql_owner_epoch_hint: u64,
}
```

The `dsql_owner_epoch_hint` is only a hint. The authoritative check happens at DSQL commit time.

### 3. Bundle Placement Service

The Placement Service chooses where slots should live.

Inputs:

- host CPU/memory/network pressure;
- microVM count and density;
- slot boot/restore latency;
- DSQL commit latency and OCC conflict pressure by cell;
- bundle heat;
- queue partition pressure;
- sticky hit/miss rate;
- host/AZ failure domains;
- image/runtime version constraints.

Outputs:

- create slot;
- reuse warm slot;
- drain slot;
- split hot bundle range;
- merge cold bundle ranges;
- terminate idle or redundant slot.

The service should be replicated and deterministic where possible. It should not require leadership election for correctness. Placement is advisory; DSQL fencing is authority.

### 4. Firecracker Host Agent

This is the largest new implementation surface.

One host agent runs per Firecracker host.

Responsibilities:

- Firecracker process lifecycle;
- jailer setup;
- kernel/rootfs/snapshot cache;
- TAP/vsock networking;
- cgroup and resource accounting;
- microVM health checks;
- metrics/log extraction;
- forced cleanup;
- image/runtime rollout;
- slot admission;
- local warm-pool management.

The Lambda paper explicitly notes that Firecracker is not an orchestration layer; higher-level orchestration must provide packaging, metadata, and management.[^agache]

### 5. Bundle Slot microVM

The slot lifecycle:

```text
Empty
  -> Booting
  -> Warm
  -> Leased(bundle_range)
  -> Busy
  -> Idle
  -> Draining
  -> Dead
```

Slots should be long-lived enough to amortize boot and cache warmup, but disposable enough to make failure recovery simple.

## Control Plane and Data Plane

### Control Plane

```text
metrics + host health
  -> Placement Service
  -> Host Agent create/lease slot
  -> Bundle Manager route cache
```

### Data Plane

```text
Edge
  -> Bundle Manager
  -> Bundle Slot microVM
  -> DSQL fenced commit
  -> response
```

If the slot is stale:

```text
microVM returns NotBundleOwner / stale epoch
  -> Bundle Manager invalidates route
  -> Placement creates or selects new slot
  -> retry within deadline
```

## Connection Points With Existing Tokeira Architecture

### Edge

Edge forwards to Bundle Manager for Firecracker-managed bundle ranges. Existing Edge admission remains the first line of defense against poll floods.

### Runtime Lanes

The bundle slot contains lane executors. This preserves the existing lane/run-actor model, but moves some lanes into a microVM boundary.

### DSQL Storage

DSQL remains the authoritative store. Every commit checks bundle lease epoch.

### Delivery Broker

There are two choices:

1. Broker remains outside microVM and dispatches to slots.
2. Broker has a local slot endpoint for a bundle range.

The second is more interesting because it can keep sticky/live-ready state near the bundle runtime.

### Projection Plane

Projection remains outside the microVM. Slots emit committed projection mutations after successful DSQL transition commits.

### Observability

The host agent exports host/microVM metrics. The bundle runtime exports Tokeira metrics. Logs should flow to stdout or vsock/log agent and then into Alloy. The Tokeira app-facing triad remains unchanged: tracing JSON logs, Prometheus metrics, OTLP traces.

### Orchestration Framework / `tkr`

A future Firecracker platform should be another deployment/platform implementation, not a rewrite of `tkr`.

Candidate provider components:

```text
tokeira-firecracker
  - HostAgentService
  - BundleManagerService
  - PlacementService
  - FirecrackerHostResource
  - MicroVmImageResource
  - SnapshotResource
```

## ECS on EC2 Baseline

The ECS baseline is:

```text
ECS service: tokeira-runtime
  - normal Linux process/container
  - one or more lanes per task
  - placement via ECS/ASG/capacity provider
  - isolation via container boundary
  - service lifecycle handled by ECS
```

Advantages:

- simpler operational model;
- native ECS scheduling and health management;
- straightforward logging and metrics collection;
- no custom microVM host agent;
- easy to iterate while Tokeira semantics are still evolving.

Weaknesses:

- weaker tenant/workload isolation than microVMs;
- less control over per-bundle locality;
- cache locality is bound to ECS task/process lifecycle;
- noisy workload effects are mitigated by process/container controls rather than VM boundary;
- hot-bundle specialization is harder.

## Theoretical Performance Improvements

Firecracker is not automatically faster than ECS containers. In fact, for cold starts, containers may start faster and have lower runtime overhead. Firecracker's value is stronger isolation, controlled reuse, and slot locality. Firecracker's official site says it can initiate user-space/application code in as little as 125 ms and create up to 150 microVMs/sec/host, and its specification says VMM thread overhead is no more than about 5 MiB, excluding guest memory and some configuration-specific overhead.[^fc-site][^fc-spec]

The performance thesis is therefore not:

> Firecracker is faster per syscall or faster per container.

It is:

> Bundle slots may reduce coordination, cold activation, cache misses, and cross-host routing for hot bundle ranges.

### Potential Lift Sources

| Source | ECS baseline | Firecracker bundle-slot path | Likely effect |
|---|---|---|---|
| Bundle-local actor cache | Runtime task cache; may shift as ECS tasks move | Slot dedicated to bundle range | Higher cache hit rate for hot bundles |
| Sticky delivery | Broker/runtime process-local | Slot-local broker endpoint possible | Better sticky hit rate if routing stable |
| DSQL clients | Runtime task-local pools | Slot-local warm clients | Fewer cold client/session paths |
| Runtime specialization | Generic runtime task | Slot can specialize for bundle/queue class | Lower dispatch and lookup overhead |
| Isolation | Container/process | MicroVM | Better fault/noisy-neighbor containment |
| Cold placement | ECS task already running | MicroVM boot/restore if no warm slot | Worse unless warm/snapshot pool exists |
| Operational overhead | ECS-managed | Tokeira-managed host agent | Higher complexity |

### A Simple Performance Model

Let:

```text
T_ecs = DSQL_commit + runtime_dispatch + broker_handoff + cache_miss + network_hops
T_fc  = DSQL_commit + slot_dispatch + slot_cache_miss + placement_overhead
```

Because DSQL commit remains in both paths, Firecracker can only improve the non-DSQL portion unless better locality also lowers DSQL conflict pressure.

The speedup is bounded by the fraction of time that is not DSQL commit:

```text
max_speedup ≈ 1 / (dsql_fraction + (1 - dsql_fraction) / local_speedup)
```

Example intuition:

| DSQL fraction of transition latency | Non-DSQL local speedup | Max theoretical speedup |
|---:|---:|---:|
| 70% | 2.0x | 1.18x |
| 50% | 2.0x | 1.33x |
| 40% | 3.0x | 1.67x |
| 25% | 3.0x | 2.0x |

So if DSQL dominates, Firecracker will not produce a large ST/s lift. If runtime/broker/cache overhead is significant, bundle slots could produce meaningful improvements.

### Estimated Lift Bands

These are architecture estimates, not benchmarks.

#### General workload, DSQL-bound

Expected lift vs ECS runtime:

```text
0.9x – 1.2x
```

Firecracker may be neutral or slightly worse due to orchestration overhead unless slot locality removes enough non-DSQL work.

#### Hot bundle / queue-partition workload

Expected lift:

```text
1.2x – 1.8x
```

Reason: stable slot locality improves actor-cache, sticky routing, and broker locality.

#### High-isolation multi-tenant workload

Performance lift may be modest:

```text
1.0x – 1.4x
```

But the operational/security value may be much higher than the raw throughput change.

#### Highly bursty cold workload

Expected lift:

```text
0.7x – 1.1x without warm pools
1.0x – 1.4x with good snapshot/warm pools
```

Firecracker slot creation must be amortized. Without warm or snapshot pools, ECS can be simpler and faster.

#### Pathological hot single workflow

Expected lift:

```text
~1.0x
```

A single workflow execution remains single-writer. Firecracker cannot parallelize the semantic object.

## What Must Be Measured

Before adopting this, Tokeira should benchmark:

- slot boot latency;
- snapshot restore latency;
- slot creation rate per host;
- host density;
- per-transition latency inside slot;
- DSQL commit latency from inside microVM;
- cross-host vs slot-local dispatch latency;
- cache hit rate;
- sticky hit rate;
- stale lease/retry rate;
- microVM CPU steal/noisy-neighbor behavior;
- host agent CPU/memory overhead;
- Alloy log/metric path overhead.

## Expected Failure Modes

### Slot stampede

Many hot bundles miss at once and trigger microVM creation storms.

Mitigation:

- placement admission;
- warm pools;
- per-host creation rate limits;
- fallback to ECS runtime.

### Stale route

Bundle Manager routes to an expired slot.

Mitigation:

- short placement leases;
- DSQL epoch fence;
- retry with route invalidation.

### Orphaned microVMs

Host agent dies or loses track of slot processes.

Mitigation:

- startup reconciliation;
- cgroup/jailer scans;
- forced cleanup;
- host drain protocol.

### Poor slot granularity

Too many slots increases overhead. Too few slots loses locality and isolation.

Mitigation:

- bundle-range default;
- adaptive hot split;
- cold merge.

### DSQL remains bottleneck

The expensive part is still commit latency/OCC pressure.

Mitigation:

- do not expect Firecracker to solve storage bottlenecks;
- combine with multi-DSQL-cell dynamic placement.

## Security Notes

Firecracker provides a strong isolation boundary through KVM and a minimal device model. The Firecracker paper describes each microVM as a sandbox with guest kernel and customer/user code isolated from other workloads. Firecracker also intentionally excludes broad device emulation and legacy hardware surfaces to reduce attack surface.[^agache]

For Tokeira, a production design should require:

- jailer or equivalent confinement;
- seccomp profile;
- cgroups;
- read-only rootfs where possible;
- image/kernel signing;
- snapshot hygiene;
- unique identity regeneration after snapshot restore;
- no long-lived secrets baked into snapshots.

The snapshot uniqueness issue is not theoretical. Serverless snapshot restoration has to handle regenerated randomness, UUIDs, secrets, and other uniqueness-sensitive state.[^snapshot-uniq]

## Recommended Adoption Path

### Phase 0 — ECS baseline

Keep ECS on EC2 as the default platform.

### Phase 1 — Host-agent experiment

Build a standalone `tokeira-firecracker-host-agent` and run one trivial bundle runtime inside a microVM.

Goals:

- boot microVM;
- connect to Tokeira internal network;
- emit logs/metrics;
- perform a no-op transition against test storage.

### Phase 2 — Bundle slot prototype

Introduce Bundle Manager and Placement Service for one opt-in queue or bundle range.

Goals:

- route work to slot;
- acquire DSQL bundle lease;
- commit transitions;
- fallback to ECS runtime.

### Phase 3 — Warm pool and snapshots

Add warm slots and snapshot restore.

Goals:

- reduce cold slot latency;
- measure density and restore overhead;
- validate snapshot uniqueness and secret hygiene.

### Phase 4 — Dynamic placement

Allow metrics-driven hot split/cold merge of bundle ranges.

Goals:

- improve hot-bundle locality;
- avoid placement thrash;
- prove DSQL epoch fencing under movement.

## Open Questions

1. Should Firecracker slots run full `tokeira-runtime` components or a smaller `tokeira-bundle-runtime` binary?
2. Should bundle slots own broker-local waiters, or should Bundle Manager keep all waiter state outside the VM?
3. Should a slot acquire one DSQL bundle lease or a range of leases?
4. Is nested virtualization performance good enough for Tokeira, or are `.metal` hosts required?
5. How should microVM networking be built: TAP, vsock, or a narrow host-agent proxy?
6. Should Firecracker be used only for isolation-sensitive tenants, or also for performance-sensitive hot bundles?
7. What is the fallback policy when no slot can be created within the request deadline?

## Recommendation

Firecracker should be treated as a later optional substrate for **bundle-local execution**, not as the initial substrate for core Tokeira services.

The architecture is attractive if Tokeira needs:

- stronger isolation per tenant/bundle group;
- better hot-bundle locality;
- independent failure containment;
- reusable bundle execution slots.

The architecture is unattractive if the goal is simply to make the first Tokeira runtime faster. ECS on EC2 is simpler, cheaper to operate initially, and sufficient to validate the kernel/storage/runtime architecture.

The strongest recommendation is:

> Build Tokeira on ECS first. Then add Firecracker as an opt-in execution substrate for selected bundle ranges, with DSQL bundle leases remaining the only commit authority.

## References

[^agache]: Alexandru Agache et al., “Firecracker: Lightweight Virtualization for Serverless Applications,” NSDI 2020. https://www.usenix.org/system/files/nsdi20-paper-agache.pdf

[^fc-start]: Firecracker Getting Started. https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md

[^ec2-nested]: Amazon EC2 nested virtualization documentation. https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/amazon-ec2-nested-virtualization.html

[^fc-containerd]: firecracker-containerd. https://github.com/firecracker-microvm/firecracker-containerd

[^fc-site]: Firecracker project site. https://firecracker-microvm.github.io/

[^fc-spec]: Firecracker Specification. https://github.com/firecracker-microvm/firecracker/blob/main/SPECIFICATION.md

[^snapshot-uniq]: Marc Brooker et al., “Restoring Uniqueness in MicroVM Snapshots.” https://arxiv.org/abs/2102.12892
