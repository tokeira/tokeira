# 130 Firecracker Worker Placement (Self-Managed Serverless Workers)

**Status:** proposal draft for architecture review
**Author:** owner proposal, captured and reviewed by Kiro (2026-07-06)
**Related docs:** [000-overview](000-overview.md), [035-placement-and-membership](035-placement-and-membership.md), [037-dynamic-placement](037-dynamic-placement.md), [045-autoscaling-on-ecs-ec2](045-autoscaling-on-ecs-ec2.md), [110-firecracker-shard-bundle-orchestrator](110-firecracker-shard-bundle-orchestrator.md), [110-firecracker-shard-bundle-orchestrator-revision](110-firecracker-shard-bundle-orchestrator-revision.md)

## Intent

This note proposes a **self-managed Firecracker compute layer for running Temporal Workers** —
user code that creates a client, polls a Task Queue, processes work, and drains — as leased microVM
slots. Modeled on the architecture of the Firecracker/Lambda paper: a central **placement service**,
per-host **microVM managers**, **leased slots**, health/load feedback, and a **separate autoscaler**
that asks for capacity but does not pick hosts.[^firecracker]

Firecracker intentionally provides no orchestration, packaging, scheduling, or fleet management — the
paper positions it as replacing QEMU, not Docker/Kubernetes-style control planes.[^firecracker] That
missing layer is exactly what this proposal defines.

> Autoscaling decides **how many** slots a `WorkerFleetVersion` needs.
> Placement decides **where** those slots land and **when they die**.
> Host managers **materialize** leases as supervised microVMs.
> Guest agents **start** Temporal Workers that poll with the correct deployment/build identity.

### Boundary: this is a product/operational plane, not the correctness core

The [overview](000-overview.md) is explicit that **Tokeira core runs workflow state machines, not
user code**. This layer is the opposite: it runs **untrusted customer Worker code**. That single fact
is the whole justification for microVMs here, and it is the decisive difference from
[110](110-firecracker-shard-bundle-orchestrator.md): the 110-revision argues that OS processes may
suffice for *tokeira's own trusted runtime bundles*, so Firecracker there is "isolation looking for a
requirement." For **user Workers the isolation requirement is real and primary** — this is precisely
the multi-tenant untrusted-code threat model Firecracker was built for.[^firecracker]

This plane sits alongside the operational services in the overview (controller/autoscaler, system
service, archival). It is **optional** (a self-hosted "serverless workers" offering) and never on the
workflow correctness hot path.

## Relationship to existing Tokeira systems (reviewer integration)

Two names in the raw proposal collide with existing subsystems. This doc disambiguates them, because
conflating them would create a second, parallel placement system — the exact mistake the
[110-revision](110-firecracker-shard-bundle-orchestrator-revision.md) warns against.

| Concern | Existing (runtime) | This doc (workers) |
|---|---|---|
| What is placed | Tokeira's **own** lease bundles / runtime nodes | Customer **Worker microVM slots** |
| Controller | `tokeira-controller` — bundle-lease ownership, queue-partition homing ([035](035-placement-and-membership.md), [037](037-dynamic-placement.md)) | **Worker Placement Controller** (new) |
| Autoscaler | `tokeira-autoscaler` — runtime capacity on ECS/EC2 ([045](045-autoscaling-on-ecs-ec2.md)) | **Worker Fleet Autoscaler** (new) |
| Placed object's authority | DSQL bundle-lease fence (execution-scoped) | DSQL slot-lease fence (fleet-version-scoped) |

These are **different objects and different controllers** — so, unlike the 110 shard-bundle case, a
separate placement service *is* warranted here. But it SHALL **reuse the DSQL lease/fencing pattern**
from [035](035-placement-and-membership.md) (fenced compare-and-swap on a lease row, epoch/fencing
token, "stale owner fails closed"), not invent a new coordination store. See
[Placement store and fencing](#placement-store-and-fencing).

**Overlap with `tokeira-odori`.** The odori project already builds Firecracker runner daemons
(`odori-runnerd` / `-vmm` / `-guestd`) — a host manager + guest agent + Firecracker lifecycle over a
vsock control channel. `tokeira-hostd` and `tokeira-guest-agent` below are architecturally the same
shape. The microVM-lifecycle machinery (jailer, cgroups, tap/vsock, snapshot cache, guest control
protocol) should be **shared, not forked**. Where that shared crate lives — tokeira core, odori, or a
neutral crate both depend on — is an [open question](#review-questions), constrained by odori's rule
that it never depends on an engine-internal crate.

## The demand source and the v1.31.0 scope boundary (reviewer ground-truth)

The raw proposal sources demand from Temporal's **Serverless Workers** / **Worker Controller
Instance** (WCI), which reacts to sync-match failures and backlog by invoking a compute provider.

Ground-truthed against `TEMPORAL_SERVER_COMPAT = 1.31.0`:

- **Worker Deployment Version is in-target.** `deployment_name` + `build_id` are a real v1.31.0
  concept (`proto/internal/.../deployment/v1/message.proto:14-21 @ v1.31.0`). So
  `WorkerFleetVersionId { namespace, deployment_name, build_id }` binds to the pinned contract.
- **"Serverless Workers" / WCI are NOT in-target.** `grep -i serverless` over the v1.31.0 server and
  protos returns nothing — these are Temporal Cloud concepts, not the pinned OSS surface. **Tokeira
  cannot rely on a WCI demand signal it does not implement.**

**Consequence:** the demand signal MUST be **tokeira-native** — derived from tokeira's own matching /
delivery-broker signals (sync-match miss, task-queue backlog growth for a bound
`deployment_name/build_id`; see [040-delivery-broker](040-delivery-broker.md)) — not from a Temporal
WCI callback. The placement/host/guest machinery below is independent of *how* demand is produced;
only the trigger wiring depends on this decision.

## Core architecture

```text
tokeira demand source (matching backlog / sync-match miss for a bound deployment version)
        |
        v
Worker Fleet Autoscaler        desired slots for WorkerFleetVersion
        |
        v
Worker Placement Controller    placement lease (host + slot + envelope + lifetime)
        |
        v
tokeira-hostd (per Firecracker host)
        |   Firecracker API / jailer / cgroups / tap / vsock / snapshots
        v
Firecracker microVM
        |   tokeira-guest-agent starts the Temporal Worker
        v
Temporal Worker polls the Task Queue
```

Temporal (and tokeira's own edge) never needs to know which host runs a Worker. The Worker's
execution model is unchanged: start, create a client, poll a Task Queue, process, drain. Tokeira
treats a bound-but-unserviced deployment version as a **demand signal**; its placement controller
owns the physical fleet.

## Slots and fleet versions

In the Firecracker paper each Lambda host offers **slots**: pre-loaded execution environments for a
single function, reused for serial invocations; when none is free the Worker Manager asks Placement
to create one, and Placement optimizes host choice across CPU/memory/network/storage under a
time-based lease.[^firecracker] The tokeira analogue:

```rust
struct WorkerSlot {
    slot_id: SlotId,
    fleet_version: WorkerFleetVersionId,

    host_id: HostId,
    microvm_id: MicroVmId,

    resource_envelope: ResourceEnvelope,
    worker_identity: TemporalWorkerIdentity,

    state: SlotState,
    lease: PlacementLease,
}

// Binds to the v1.31.0 Worker Deployment Version (deployment_name + build_id).
struct WorkerFleetVersionId {
    namespace: String,
    deployment_name: String,
    build_id: String,
}
```

The default isolation profile:

```text
one slot = one Firecracker microVM = one supervised Temporal Worker process
```

That Worker may carry many Workflow-Task and Activity slots internally. Stronger profiles are
selectable per fleet version — `microvm_per_worker` (default), `microvm_per_activity_slot`,
`microvm_per_tenant`, `microvm_per_workflow_run`. The placement controller is SDK-language-agnostic:
**it places resource envelopes, not runtimes.**

## Worker Placement Controller responsibilities

### 1. Host selection

```rust
struct PlacementRequest {
    fleet_version: WorkerFleetVersionId,
    task_queues: Vec<TaskQueueBinding>,
    count: u32,
    resource_class: ResourceClass,
    isolation_policy: IsolationPolicy,
    locality_policy: LocalityPolicy,
    lifecycle_policy: SlotLifecyclePolicy,
    reason: PlacementReason,
}

struct PlacementLease {
    lease_id: LeaseId,
    fencing_token: u64,          // fences split-brain: hostd rejects a stale token
    host_id: HostId,
    slot_id: SlotId,
    fleet_version: WorkerFleetVersionId,
    resource_envelope: ResourceEnvelope,
    expires_at: DateTime<Utc>,
}
```

Time-based leases keep routing sticky while preserving clear ownership, and (as in
[035](035-placement-and-membership.md)) prevent split-brain between the central controller and
host-local state.[^firecracker]

### 2. Capacity accounting

```rust
struct HostInventory {
    host_id: HostId,
    arch: CpuArch,
    numa_topology: Option<NumaTopology>,
    total: HostResources,
    reserved: HostResources,
    observed_pressure: HostPressure,
    slots: Vec<SlotSummary>,
    cached_artifacts: Vec<ArtifactDigest>,
    cached_snapshots: Vec<SnapshotRef>,
    fault_domain: FaultDomain,
    lifecycle: HostLifecycleState,
}
```

Memory is **mostly hard-accounted**; CPU, network, and disk are softer but need explicit overcommit
policy. The Firecracker paper's multi-tenancy analysis applies: idle slots mainly consume memory,
while initializing/busy slots also consume CPU, caches, network, and memory bandwidth;
oversubscription is a statistical bet on high-percentile vs mean use under a compliance
goal.[^firecracker] So the model is a **vector**, not `vcpu=2, mem=1024`:

```rust
struct ResourceEnvelope {
    memory_mib: u64,          // hard reservation
    vcpu_count: u8,           // guest-visible vCPUs
    cpu_min_millis: u64,      // scheduling guarantee
    cpu_burst_millis: u64,    // soft overcommit
    disk_mib: u64,
    disk_iops_limit: Option<u64>,
    net_mbps_limit: Option<u64>,
}
```

Firecracker's built-in network/storage rate limiters and dense packing support this directly.[^firecracker]

### 3. Anti-correlation

Placement keeps utilization even across CPU/memory/network/storage while minimizing correlated
allocation on a host.[^firecracker] Correlated load for Workers can come from the same tenant,
`WorkerFleetVersion`, task queue, customer project, workflow type, external dependency,
model/tool/runtime, availability zone, or artifact snapshot. First scoring model — **vector packing,
not scalar**:

```text
score(host) =
    headroom_score(host, request)
  + balance_score(host)
  + locality_score(host, artifact_or_snapshot)
  + anti_affinity_score(host, fleet_version, tenant, task_queue)
  + health_score(host)
  - fragmentation_penalty(host, request)
  - drain_penalty(host)
  - hot_resource_penalty(host)
```

A host with free memory but saturated network is a bad fit for network-heavy Activities.

### 4. Slot lifetime and drainage

Placement owns slot *lifetime*, not just allocation: limiting lifetime, terminating idle/redundant
slots, managing updates, and consuming load/health data.[^firecracker]

```text
autoscaler lowers desired count
  -> placement chooses specific slots to remove
  -> hostd asks the guest to stop polling
  -> guest drains in-flight work / flushes telemetry
  -> hostd terminates the microVM
  -> placement releases the lease
```

Graceful drain follows Temporal Worker shutdown shape: stop polling, wait for in-flight Tasks, run
shutdown hooks, exit before forced termination.[^worker-tuning]

### 5. Validation and binding

A Task Queue binds to a Worker Deployment Version only after a Worker with that version successfully
connects and polls; a failed first invocation can leave the binding unestablished.[^worker-versioning]
So a fleet version SHALL pass a mandatory validation slot before autoscaled placement is enabled:

```text
register WorkerFleetVersion
  -> placement creates one validation slot
  -> hostd boots/restores the microVM
  -> guest starts the Worker (deployment_name + build_id)
  -> Worker successfully polls the Task Queue
  -> mark version placement-ready
```

## `tokeira-hostd` — per-host manager

Each Firecracker host runs a privileged host manager — the analogue of Lambda's per-worker
MicroManager, which manages Firecracker processes, exposes slot management/locking to Placement, and
feeds monitoring/logging back to it.[^firecracker] It owns: `/dev/kvm` access; Firecracker process
lifecycle and jailer invocation; cgroup setup; tap/veth/vsock setup; rootfs/kernel and snapshot
caches; metrics/log collection; slot locking; the guest-agent control channel; and local GC.

```rust
#[async_trait]
trait HostManager {
    async fn reserve_slot(&self, lease: PlacementLease) -> Result<ReservationAck>;
    async fn materialize_slot(&self, slot: SlotSpec) -> Result<SlotStarted>;
    async fn restore_slot(&self, slot: SlotSpec, snapshot: SnapshotRef) -> Result<SlotStarted>;
    async fn request_drain(&self, slot_id: SlotId, deadline: DateTime<Utc>) -> Result<()>;
    async fn terminate_slot(&self, slot_id: SlotId, reason: TerminationReason) -> Result<()>;
    async fn describe_inventory(&self) -> Result<HostInventory>;
    async fn list_slots(&self) -> Result<Vec<SlotSummary>>;
}
```

Firecracker's control surface fits: a KVM-based VMM driven by a REST API (machine config, then
`InstanceStart` to power on and boot the guest), with rate limiters, a metadata service, and the
jailer for host-side isolation.[^firecracker]

> **Reviewer note (cost).** As the 110-revision observed for the shard-bundle host agent, a
> production-quality `tokeira-hostd` is a substantial systems effort (privileged, security-reviewed,
> Linux-kernel-adjacent) — comparable to a core crate, not an "experiment." The MVP should lean on
> the shared odori runner machinery rather than build a second host agent from scratch.

## `tokeira-guest-agent` — in-guest supervisor

```text
guest-agent
  -> reads boot metadata
  -> fetches Temporal credentials/config (per-tenant, scoped)
  -> starts the Worker process
  -> reports "polling ready"  (only after the Worker created a client AND began polling)
  -> reports slots / in-flight work
  -> handles drain request
  -> flushes logs/metrics/traces
  -> exits
```

Readiness is reported **only** after the Worker has actually created a client and started polling —
the only signal that matters for serverless-worker semantics. Control is **host-mediated** so the
microVM exposes no broad inbound management surface:

```text
guest-agent <-> hostd (vsock) <-> tokeira control plane
```

## Slot state machine

```text
REQUESTED -> LEASED -> RESERVED_ON_HOST -> (BOOTING | RESTORING)
  -> GUEST_BOOTED -> WORKER_STARTING -> POLLING_READY -> ACTIVE -> IDLE
  -> DRAIN_REQUESTED -> DRAINING -> TERMINATED

failure: BOOT_FAILED | WORKER_FAILED | POLL_TIMEOUT | HOST_LOST | LEASE_EXPIRED
```

Richer than the paper's `Init -> Idle -> Busy -> Dead`[^firecracker] because Temporal readiness,
worker identity, and graceful drain are first-class here.

## Warm pools and snapshots

Firecracker cold boot is fast (~125 ms with a minimal kernel), but Lambda still keeps a small
pre-booted pool because even 125 ms is too slow on a blocking scale-up path; Little's Law sizes it:
mean pool ≈ creation rate × creation latency (at 125 ms, one pooled microVM covers ~8
creations/sec).[^firecracker] Three startup tiers:

```text
Tier 0: existing idle slot for the same WorkerFleetVersion
Tier 1: restored snapshot for the same WorkerFleetVersion
Tier 2: cold boot from kernel/rootfs/artifact
```

**Do not start with aggressive snapshot cloning.** Restoring the same snapshot more than once can
duplicate identifiers, RNG seeds, entropy state, and cryptographic tokens; VMGenID reseeds the kernel
PRNG but application-level unique state still needs explicit handling.[^snapshot] The safe rule:

```text
Snapshot only:  kernel booted, guest agent loaded, runtime deps warmed,
                NO Temporal client, NO credentials, NO tenant token,
                NO workflow/activity state, NO externally visible connection.

After restore:  refresh entropy-sensitive state, fetch credentials,
                create the Temporal client, start the Worker, poll.
```

Snapshots are a **startup accelerator**, never a way to clone a live Worker. (The 110-revision reaches
the same "warm pools over snapshots" conclusion for the runtime case.)

## Placement versus autoscaling

Keep the two controllers separate — this prevents the failure mode where every component tries to
scale:

```rust
// Worker Fleet Autoscaler owns desired capacity.
struct DesiredFleetCapacity { fleet_version: WorkerFleetVersionId, desired_slots: u32, min_slots: u32, max_slots: u32 }

// Worker Placement Controller owns physical realization.
struct PlacementPlan { create: Vec<PlacementLease>, keep: Vec<SlotId>, drain: Vec<SlotId> }
```

Demand → desired slots (autoscaler) → host choice + lifetime (placement). This mirrors the
autoscaler/placement split tokeira already uses for its runtime ([045](045-autoscaling-on-ecs-ec2.md)
vs [035](035-placement-and-membership.md)/[037](037-dynamic-placement.md)).

## Host selection algorithm (MVP)

A deterministic, explainable scheduler. **Filter**, then **score**, then **commit under a fence**:

```text
Filter:  Ready; arch matches artifact/snapshot; /dev/kvm healthy; not draining;
         kernel/rootfs/snapshot compatible; enough hard memory + disk;
         tenant/security/fault-domain constraints satisfied.

Score:   prefer cached artifact/snapshot; prefer balanced residual CPU/mem/net/disk;
         spread same fleet/version/tenant across fault domains;
         avoid high pressure / recent failures / pending drain;
         avoid placements that create unusable fragments.

Commit:  select top-K -> compare-and-swap reservation in the placement store
         -> send lease to hostd -> hostd confirms with fencing token
         -> materialize -> wait for guest readiness / Temporal poll -> ACTIVE.
```

Lambda's placement optimization reportedly completed in <20 ms before asking a worker to create a
slot.[^firecracker] Not an MVP requirement, but a good target: keep global optimization lightweight;
push slow work to host managers.

## Placement store and fencing (reviewer integration)

The raw proposal says "compare-and-swap reservation in placement store" without naming the store.
**Recommendation: reuse the DSQL fenced-lease pattern from [035](035-placement-and-membership.md)** —
a `worker_slot_lease` table with `(slot_id PK, owner_host_id, fencing_token/epoch, lease_until)`,
CAS on reservation, epoch bump on owner change, "stale owner fails closed." This keeps a single
authoritative fencing mechanism across the codebase and avoids a second coordination substrate
(consistent with 035 removing DynamoDB from the placement path). Host inventory and health, by
contrast, are **advisory live state** (controller-held, heartbeat-fed) exactly as runtime membership
is in 035 — not authoritative, so a lost heartbeat is not a correctness event.

## Security and threat model (reviewer integration)

Because this plane runs **untrusted, multi-tenant customer code**, the threat model must be explicit
(the runtime planes never need this):

- **Isolation boundary is the microVM**, hardened with the jailer, cgroups, seccomp, and a minimal
  guest surface — the configuration Firecracker was designed for.[^firecracker]
- **No broad inbound management surface** in the guest: all control is host-mediated over vsock.
- **Credential isolation is paramount.** The guest fetches Temporal credentials *after* boot/restore,
  scoped to exactly one tenant/namespace/fleet version; credentials, tenant tokens, and client state
  are **never** captured in a snapshot (see the snapshot rule above). A slot must never be able to
  obtain another tenant's credentials.
- **Artifact provenance**: the rootfs/artifact for a fleet version must be integrity-checked
  (`ArtifactDigest`) before boot; a host caches artifacts by digest.
- **Blast-radius containment**: anti-affinity spreads a tenant/fleet across fault domains so a host
  compromise or failure cannot take down a whole tenant.

## Failure behavior

```text
Host stops heartbeating:  mark suspect; stop new placement; expire leases after TTL;
                          replacement demand returns to autoscaler/placement;
                          Temporal retries work per normal semantics.
Slot boots but never polls: terminate; penalize host/artifact combo; replace;
                            fail validation if on the validation path.
Guest crashes:            hostd reports terminal state; placement releases lease;
                          autoscaler decides replacement.
Drain timeout expires:    hostd kills the Firecracker process;
                          Temporal handles the abandoned Activity via timeout/retry.
```

Temporal's own failure model carries the recovery: Worker crash → Activity timeout/retry; tasks stay
queued when provider concurrency is exhausted.[^worker-tuning] Tokeira's job is to make failures
visible and replace capacity quickly.

## Rust-first API shape

```rust
let fleet = WorkerFleetVersion::new("prod", "ai-agent", build_id)
    .task_queue("ai-agent-tasks")
    .resource_class(ResourceClass::new()
        .memory_mib(2048).vcpu_count(2)
        .cpu_min_millis(500).cpu_burst_millis(2000)
        .net_mbps_limit(200))
    .isolation(IsolationPolicy::microvm_per_worker())
    .lifecycle(SlotLifecyclePolicy::run_drain_terminate()
        .idle_after(Duration::from_secs(60))
        .drain_timeout(Duration::from_secs(300)));

placement.register_fleet_version(fleet).await?;
placement.validate_fleet_version("prod", "ai-agent", build_id).await?;
```

```rust
#[async_trait]
trait PlacementController {
    async fn place(&self, request: PlacementRequest) -> Result<PlacementPlan>;
    async fn release(&self, slot_id: SlotId, reason: ReleaseReason) -> Result<()>;
    async fn drain_host(&self, host_id: HostId, reason: DrainReason) -> Result<()>;
}

#[async_trait]
trait HostManagerClient {
    async fn reserve_slot(&self, lease: PlacementLease) -> Result<()>;
    async fn start_slot(&self, spec: SlotSpec) -> Result<SlotStarted>;
    async fn drain_slot(&self, slot_id: SlotId, deadline: DateTime<Utc>) -> Result<()>;
    async fn terminate_slot(&self, slot_id: SlotId) -> Result<()>;
}
```

## MVP recommendation

Ship first:

```text
1. Worker Placement Controller with DSQL lease-based slot allocation (035 fencing pattern).
2. tokeira-hostd that can cold-boot Firecracker microVMs (built on shared odori runner machinery).
3. tokeira-guest-agent that starts a Temporal Worker and reports poll readiness.
4. Hard memory accounting; CPU soft allocation; basic net/disk limits.
5. Simple anti-affinity by fleet version, tenant, and fault domain.
6. Mandatory validation slot before activating a WorkerFleetVersion.
7. Drain/terminate lifecycle.
8. GC for leaked leases, dead hosts, and orphaned Firecracker processes.
```

Then add: warm pools sized by measured creation latency × arrival rate (9); pre-credential snapshot
restore (10); NUMA-aware placement (11); statistical overcommit on p95/p99 host pressure (12);
rebalancing and host evacuation (13).

> **Reviewer note (measure first).** Before committing to the full host agent, take the 110-revision's
> discipline: boot a microVM on a test host, run a Worker that polls a tokeira task queue, and measure
> boot latency, poll-ready latency, memory overhead, and network path. The perf profile here is
> **poll/network-bound**, not DSQL-bound (Workers don't commit to DSQL) — so the 110 Amdahl/DSQL
> analysis does *not* transfer; validate the real bottleneck before sizing pools.

## The product model in one paragraph

**Tokeira Worker Placement turns abstract `WorkerFleetVersion` capacity into leased Firecracker slots
on specific hosts. Host managers materialize those leases as supervised microVMs. Guest agents start
Temporal Workers that poll with the correct deployment/build identity. The Worker Fleet Autoscaler
decides how many slots should exist; Worker Placement decides where they live and when they die.**

## Review questions

1. **Placement store.** Adopt the DSQL fenced-lease pattern (035) for slot leases, or a separate
   store? (Recommendation: DSQL, one fencing mechanism.)
2. **Shared microVM machinery.** Where do `tokeira-hostd` / `tokeira-guest-agent` live so both this
   plane and `tokeira-odori` reuse one implementation, given odori must not depend on an
   engine-internal crate? A neutral shared crate?
3. **Demand source.** Since v1.31.0 has no Serverless-Worker/WCI signal, what is the exact tokeira
   -native trigger — matching sync-match miss, backlog threshold per bound deployment version, or an
   explicit operator/API signal? How does it bind to `deployment_name/build_id`?
4. **Product scope.** Is a self-hosted serverless-worker offering in scope for tokeira at all, or does
   it belong in a downstream product (odori-style) that consumes tokeira over the public API? This
   determines whether the plane lives in this repo.
5. **Isolation profiles.** Is `microvm_per_worker` the only MVP profile, with the stronger profiles
   (per-activity / per-tenant / per-run) deferred until a concrete requirement?
6. **Overcommit policy.** What compliance goal (target p99 slot-starvation rate) sizes the memory hard
   bound vs the CPU/net soft overcommit?

## References

[^firecracker]: Agache et al., *Firecracker: Lightweight Virtualization for Serverless Applications*, USENIX NSDI 2020: https://www.usenix.org/conference/nsdi20/presentation/agache
[^snapshot]: Firecracker snapshotting & VMGenID / uniqueness guidance: https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md
[^worker-versioning]: Temporal Worker Deployments / versioning (deployment name + build ID; task-queue binding on first successful poll): https://docs.temporal.io/worker-deployments
[^worker-tuning]: Temporal Worker performance / graceful shutdown & drain semantics: https://docs.temporal.io/develop/worker-performance
