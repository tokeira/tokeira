# 131 Firecracker Worker Placement — Implementation Approach, Sequencing, and ECS Enablement

**Status:** proposal draft for planning review
**Author:** Kiro (2026-07-06)
**Companion to:** [130-firecracker-worker-placement](130-firecracker-worker-placement.md) (the architecture)
**Related docs:** [045-autoscaling-on-ecs-ec2](045-autoscaling-on-ecs-ec2.md), [035-placement-and-membership](035-placement-and-membership.md), [037-dynamic-placement](037-dynamic-placement.md), `platforms/ecs`

## Purpose

[130](130-firecracker-worker-placement.md) captures the *architecture* of a self-managed Firecracker
compute layer for Temporal Workers. This note captures the *implementation approach and sequencing* to
get from today's codebase to that architecture, and a concrete path to enable Firecracker worker
invocation on the **ecs** platform.

This work is executed by the agents (Kiro, and occasionally Claude), not staffed as human engineering,
so the sizing below is **relative complexity and risk for dependency ordering**, not calendar effort.
Each phase assumes the `tokeira-odori` Firecracker runner machinery is reusable and the ecs platform is
*extended*, not rebuilt; the dominant uncertainties are called out per phase.

## Baseline: what exists today (verified 2026-07-06)

Grounded by reading the code, not assumption:

| Area | State | Where |
|---|---|---|
| Worker Deployment **management plane** (v2 RPCs: create/describe/delete/list deployments + versions, set-current, set-ramping, update-metadata) | **Implemented**, registry-backed, ground-truthed to `workerdeployment/*.go @ v1.31.0`, unit-tested | `tokeira-edge` (`runtime_adapter.rs`, `translate.rs`), `WorkerDeploymentRegistry`, `WorkerDeploymentRepository`; matrix `worker-deployments = Implemented` |
| Deprecated v1 deployment companions | Correctly return `UNIMPLEMENTED` (matches v1.31.0) | `tokeira-edge` |
| Versioning **behavior**: routing config (current/ramping/percentage), revision **fencing** at dispatch/start | Present (not just stored): `PendingActivity.dispatch_revision` stamped at dispatch, re-validated at start | `tokeira-storage` (`StoredRoutingConfig`), `tokeira-kernel`/`tokeira-runtime` |
| Full pinned-vs-auto-upgrade **dispatch routing** end-to-end | Pieces present; **not fully traced/verified**; no functional-corpus proof | — |
| **WCI / compute-config surface** (`SetWorkerDeploymentManager`, `Update/ValidateWorkerDeploymentVersionComputeConfig`, `ComputeConfig{provider,scaler}`, `manager_identity`) | **Plumbed but inert** — persisted + served over RPC, from the ahead-of-target `v1.62.11` proto; **no control loop enacts it**; outside the v1.31.0 behavioural claim | `tokeira-edge`, `tokeira-storage` (`ComputeScaler`, `ComputeProvider`) |
| Compute provider, Worker Placement Controller, `tokeira-hostd`, `tokeira-guest-agent`, Worker Fleet Autoscaler, tokeira-native demand source, slot-lease store, warm pools/snapshots | **Do not exist** | — (this proposal) |
| Reusable adjacent machinery | Firecracker runner daemons (`odori-runnerd`/`-vmm`/`-guestd` + guest agent + vsock) in `tokeira-odori`; DSQL fenced-lease pattern ([035](035-placement-and-membership.md)); Mimir-decides/AWS-enacts autoscaler ([045](045-autoscaling-on-ecs-ec2.md)) | `tokeira-odori`, `tokeira-storage`, `platforms/ecs` |

**Net:** the *control surface* (a client can register a deployment version, set a manager, attach a
compute config, and have workers bind to `deployment_name/build_id`) is done. The **actuation** —
turning a compute config into running, leased Firecracker Worker microVMs — is entirely unbuilt. That
actuation is this proposal.

## Implementation approach

Build the [130](130-firecracker-worker-placement.md) MVP in seven phases, each independently
testable, reusing the three adjacent systems above rather than forking them. All new controllers
reuse the **DSQL fenced-lease** pattern (one fencing mechanism) and the **Mimir-decides / AWS-enacts**
autoscaler shape.

### Phase 0 — Compute-provider seam + demand source + slot-lease store

Turn the inert compute-config surface into a live seam.

- Define a `ComputeProvider` trait (place/drain/terminate/inventory) that the Worker Placement
  Controller implements; the stored `ComputeConfig.provider`/`manager_identity` selects it. A
  deployment version whose manager names the tokeira provider becomes placement-eligible.
- Define the **tokeira-native demand signal** (per [130](130-firecracker-worker-placement.md) the
  v1.31.0 target has no Serverless/WCI callback): derive demand from the delivery broker / matching —
  sync-match miss + backlog growth for a bound `deployment_name/build_id` ([040](040-delivery-broker.md)).
- Add the **slot-lease store**: a DSQL `worker_slot_lease` table with fenced CAS
  (`slot_id`, `owner_host_id`, `fencing_token`, `lease_until`), mirroring `control.bundle_lease` (035).

*Complexity:* M. *Risk:* low–medium (the demand-signal precision is the open bit).

### Phase 1 — `tokeira-hostd` (per-host manager)

The per-Firecracker-host privileged manager: `/dev/kvm`, Firecracker process + jailer, cgroups,
tap/vsock, rootfs/kernel + snapshot caches, slot locking, guest control channel, local GC. Expose the
narrow `HostManager` RPC surface (reserve/materialize/restore/drain/terminate/describe).

**Reuse `tokeira-odori`'s `-vmm`/`-runnerd` machinery** rather than writing a second host agent.

*Complexity:* L (lower with high odori reuse). *Risk:* **high** — privileged, security-reviewed,
kernel-adjacent; the 110-revision correctly flags a production host agent as comparable to a core
crate. **The biggest single risk in this work.**

### Phase 2 — `tokeira-guest-agent`

In-guest supervisor: read boot metadata, fetch **per-tenant-scoped** Temporal credentials *after*
boot/restore, start the Worker, report poll-ready only once the Worker is actually polling, handle
drain, flush telemetry. Host-mediated over vsock (no broad inbound guest surface). **Reuse odori
`-guestd`.**

*Complexity:* M. *Risk:* medium (credential isolation is the sensitive part).

### Phase 3 — Worker Placement Controller

Lease-based slot allocation over the DSQL store (Phase 0): host selection (filter → vector-packing
score → fenced CAS commit), capacity accounting (`HostInventory`, `ResourceEnvelope`), anti-affinity
(fleet/tenant/fault-domain), slot lifetime + drainage, and the **mandatory validation slot** before a
fleet version is placement-enabled. Runs as a service (see ECS section). Distinct from
`tokeira-controller` (which places tokeira's own *bundles*, not Worker slots).

*Complexity:* L. *Risk:* medium.

### Phase 4 — Worker Fleet Autoscaler

Demand (Phase 0) → `DesiredFleetCapacity` (how many slots), never host choice. Reuse the
[045](045-autoscaling-on-ecs-ec2.md) autoscaler shape: Mimir-decides, scale-out-fast/scale-in-slow,
never-from-a-single-sample, DSQL leader lease for HA, capacity actuated via ASG `SetDesiredCapacity`
on the worker-host fleet.

*Complexity:* M (mostly pattern reuse). *Risk:* low.

### Phase 5 — ECS platform enablement

The concrete host fleet + service wiring on ecs (detailed below).

*Complexity:* M. *Risk:* medium (KVM instance type + privileged host task).

### Phase 6 — Optimizations & hardening (post-MVP)

Warm pools sized by measured creation latency × arrival rate (Little's Law), pre-credential snapshot
restore, NUMA-aware placement, statistical p95/p99 overcommit, rebalancing/evacuation, metrics
(`speculative_workflow_task`-style counters for slot commits/rollbacks), and the full security review.

*Complexity:* M–L (incremental). *Risk:* medium.

### Complexity & sequencing summary

| Phase | Scope | Complexity | Dominant risk |
|---|---|---|---|
| 0 | Compute seam + demand + slot-lease store | M | demand-signal precision |
| 1 | `tokeira-hostd` | L | **privileged host agent / security** |
| 2 | `tokeira-guest-agent` | M | credential isolation |
| 3 | Worker Placement Controller | L | scoring/lifecycle correctness |
| 4 | Worker Fleet Autoscaler | M | low (045 reuse) |
| 5 | ECS enablement | M | KVM hosts + privileged task |
| 6 | Optimizations & hardening (post-MVP) | M–L | ongoing |

Critical path: Phase 0 seeds every later phase; Phases 1 and 2 (the host/guest microVM machinery)
lean heavily on odori reuse and are the highest-risk work; Phases 3–4 depend on 0–2; Phase 5 wires the
result onto ecs. The MVP is Phases 0–5, with Phase 6 layered on incrementally afterward.

## ECS platform enablement (how worker invocation to Firecracker works on `ecs`)

### Worker hosts: KVM-capable EC2 (established by the odori work)

Firecracker requires `/dev/kvm`, so on the ecs platform Worker microVMs run **on a dedicated
ECS-on-EC2 host fleet of KVM-capable instances** — bare-metal (`*.metal`) or a nested-virt-capable
family (`c8id` nested virt was verified in the odori work, 2026-06-16 — **re-verify for the target
region and instance size before committing**). This is a *new capacity provider*, isolated from the
tokeira control/runtime fleets exactly as [045](045-autoscaling-on-ecs-ec2.md) isolates
edge/runtime/control.

### New pieces in `platforms/ecs`

The ecs platform already models per-service capacity providers, `EcsWorkload`s, Service Connect, and
`attribute:workload ==` placement constraints. Extending it:

1. **New capacity provider `cp-worker-host`** — an ASG of KVM-capable instances (metal/nested-virt),
   tagged `attribute:workload == worker-host`, scale-in-protected by default (drain before
   terminate, like `cp-runtime`). Its desired capacity is the **Worker Fleet Autoscaler's**
   `SetDesiredCapacity` lever.
2. **`tokeira-hostd` as a privileged DAEMON workload** on `cp-worker-host` (one per host, mirroring
   `tokeira-runtime`'s DAEMON profile): `privileged` + a `/dev/kvm` device mapping + host networking.
   *Alternative:* bake `hostd` into the AMI as a `systemd` unit outside ECS (cleaner isolation, but
   leaves the ECS deployment model — recommend the DAEMON task for consistency, accepting the
   privileged caveat, and flag the AMI option as a decision).
3. **`tokeira-worker-placement` REPLICA service** on `cp-control` (2 tasks, DSQL leader lease) — the
   Worker Placement Controller.
4. **`tokeira-worker-autoscaler` REPLICA service** on `cp-control` (2 tasks, DSQL leader lease) — the
   Worker Fleet Autoscaler. (Kept separate from the existing `tokeira-autoscaler`, which scales
   tokeira's *own* fleet.)
5. **`EcsConfig` additions** (serde `deny_unknown_fields`, so additive + validated): a
   `worker_hosts: CapacityProviderConfig` (KVM instance type, min/desired/max) and a `worker_fleet`
   section (default envelopes, isolation profile, warm-pool sizing, per-fleet limits).

### Wiring and control flow on ecs

```text
matching/broker backlog for a bound deployment_name/build_id   (tokeira-native demand)
  -> tokeira-worker-autoscaler (Mimir-decides): DesiredFleetCapacity
  -> tokeira-worker-placement (DSQL slot leases): host selection over cp-worker-host inventory
  -> tokeira-hostd on a cp-worker-host instance: Firecracker microVM (jailer/cgroups/vsock)
  -> tokeira-guest-agent: starts the Temporal Worker (deployment_name+build_id), reports poll-ready
  -> Worker polls the tokeira edge task queue over Service Connect / private DNS
```

- **Binding to the deployment version:** a `WorkerDeploymentVersion` whose stored `ComputeConfig` /
  `manager_identity` names the tokeira compute provider (the Phase-0 seam) is what makes it eligible
  for Firecracker placement — this is where the currently-inert compute-config surface finally does
  something.
- **Networking (private-only, per 045):** worker microVMs egress to the edge poll endpoint
  (`edge-poll.<private-zone>` / Service Connect) over the private VPC; `hostd`↔placement and
  `hostd`↔guest are host-mediated. New VPC endpoints only if the guest needs Secrets Manager/STS for
  credential fetch (prefer host-mediated credential delivery to avoid broadening the guest surface).
- **IAM:** `cp-worker-host` instance role gets only what `hostd` needs (ECR pull for rootfs/artifacts
  by digest, SSM for exec); per-tenant Temporal credentials are delivered host-mediated and scoped to
  one namespace/fleet — never a broad instance-role grant a guest could assume.
- **Autoscaler split (reuse 045 exactly):** worker-host **scale-out** = ASG `SetDesiredCapacity`;
  **scale-in** = the drain-aware retirement protocol (placement drains slots → set container instance
  `DRAINING` → clear scale-in protection → terminate), so a host is never killed with live Worker
  slots on it.

### What ecs does *not* change

The tokeira control/runtime/edge/projection/observability topology and their capacity providers are
untouched. This is purely additive: one new host-fleet capacity provider + two new small control
services + a config section. The correctness core is unaffected (Worker placement is an operational
plane, never on the workflow hot path — [130](130-firecracker-worker-placement.md)).

## Prerequisite decisions (carried from 130)

These gate the build and should be settled first:

1. **Product scope.** Does a self-hosted serverless-worker offering live in tokeira, or in a
   downstream product (odori-style) that consumes tokeira over the public API? tokeira core
   deliberately runs no user code; this plane runs untrusted user code. This decision determines
   whether Phases 1–5 land in *this* repo at all.
2. **Where `hostd`/`guest-agent` live.** A shared crate reused by both this plane and odori, given
   odori must not depend on an engine-internal crate. Drives the Phase 1–2 shape.
3. **Demand source.** The exact tokeira-native trigger (sync-match miss / backlog threshold per bound
   deployment version) — Phase 0.
4. **KVM instance strategy on ecs.** `*.metal` (safe, costlier) vs nested-virt family (`c8id`,
   re-verify) — Phase 5.
5. **`hostd` deployment form on ecs.** Privileged DAEMON task vs AMI-baked systemd unit — Phase 5.

## Verification & documentation (per AGENTS)

- Every new crate/service carries module + public-item docs and WHY comments on the non-obvious
  (lease fencing, credential isolation, snapshot hygiene).
- Slot-lease CAS gets a property test (two concurrent reservations → at most one wins), mirroring the
  state-CAS property.
- No live-AWS/live-cluster tests in the default suite; the live host path is feature/`#[ignore]`-gated
  (as the platform-eks live path is).
- `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`, `cargo test --workspace`,
  `cargo doc --workspace --no-deps` green per bar.

## Bottom line

The worker-deployment **control surface is done and unit-tested**; the **actuation is entirely
unbuilt** (Phases 0–5), dominated by the privileged `tokeira-hostd` host agent and gated by the
product-scope decision. On **ecs**, Firecracker worker invocation is *additive*: a KVM-capable
`cp-worker-host` ASG running `tokeira-hostd`, plus a Worker Placement Controller and Worker Fleet
Autoscaler as small control-plane services — reusing the existing 045 autoscaler discipline, the 035
DSQL fencing, and the odori Firecracker runner, with the inert `ComputeConfig`/`manager` surface
finally becoming the binding that makes a deployment version placement-eligible.
