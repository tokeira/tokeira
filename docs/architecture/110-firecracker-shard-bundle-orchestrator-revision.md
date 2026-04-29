# 110 — Firecracker Shard-Bundle Orchestrator — Revision Notes

Status: Revision of 110-firecracker-shard-bundle-orchestrator.md
Author: Kiro (review revision)
Position: Agrees with the conclusion; disagrees with the framing and scope of the exploration

## Summary of the Original

The original doc asks whether Firecracker microVMs could serve as reusable execution environments for shard bundles. It concludes correctly: build on ECS first, consider Firecracker later for isolation and hot-bundle locality. The Lambda Worker Manager analogy is well-chosen, the performance model is honest about DSQL-bound Amdahl limits, and the failure mode catalog is thorough.

## What I Would Change

### 1. The doc explores too much surface for a "not yet" decision

The original is ~2,500 words of detailed component design (Bundle Manager, Placement Service, Host Agent, slot lifecycle FSM, route records, control/data plane diagrams) for something explicitly positioned as "not the initial substrate." That level of detail creates gravitational pull toward building it.

If the decision is "ECS first, Firecracker later," the doc should be shorter and sharper. The detailed component design should wait until there's a concrete trigger — a measured workload where ECS isolation or locality is demonstrably insufficient.

**My preference:** cut the doc to problem statement, performance model, decision, and trigger criteria. Move the component design into a future "111-firecracker-bundle-slot-design.md" that gets written when Phase 1 is actually funded.

### 2. The performance model understates the DSQL dominance

The Amdahl table shows DSQL fractions of 25%–70%. Based on the DSQL storage design (050), connection management constraints (060), and the OCC retry characteristics documented in the temporal-dsql work, I'd expect DSQL commit to dominate at 60%–80% of transition latency for most workloads once the runtime is mature. The non-DSQL overhead that exists today (cache misses, broker handoff, cross-host routing) is partly an artifact of the runtime being young — it will shrink as the runtime matures, making DSQL an even larger fraction.

That pushes the realistic lift band for general workloads closer to 1.0x–1.1x, not 0.9x–1.2x. The honest conclusion is:

> For throughput, Firecracker is unlikely to help. The value proposition is isolation and fault containment, not speed.

The doc says this in places but the performance tables invite readers to hope for 1.8x on hot bundles. I'd be more conservative: 1.1x–1.3x for hot bundles with perfect slot locality, because the DSQL commit still dominates even when everything else is local.

### 3. The "Bundle Manager" is a second placement system

Tokeira already has a placement controller (035, 037) that manages bundle lease ownership, queue-partition homing, and edge route caches. The proposed Bundle Manager is a parallel sticky-routing layer with its own route cache, epoch hints, and placement requests.

Running two placement systems — one for ECS runtime nodes and one for Firecracker slots — creates a coordination surface that the doc doesn't address:

- Who decides whether a bundle range is Firecracker-managed or ECS-managed?
- What happens when the placement controller moves a bundle lease to a different ECS node while the Bundle Manager thinks a Firecracker slot owns it?
- How do the two route caches stay consistent?

**My preference:** if Firecracker is adopted, it should be a new execution substrate within the existing placement controller, not a parallel system. The controller already understands bundles, queue partitions, and node health. Adding "this bundle range is homed to a Firecracker slot on host X" is a placement decision, not a new service.

### 4. The Host Agent is the real cost, and it's underweighted

The doc lists the Host Agent responsibilities correctly (Firecracker lifecycle, jailer, networking, cgroups, health checks, warm pools, image rollout, metrics extraction, forced cleanup). This is a substantial systems engineering effort — comparable in complexity to the entire `tokeira-runtime` crate.

The adoption path puts it in Phase 1 as an "experiment," but a production-quality host agent is not an experiment. It's a multi-month project that requires deep Linux systems knowledge, security review, and operational tooling. The doc should be more explicit about this cost.

**My preference:** Phase 1 should be even more minimal than described. Don't build a host agent. Instead:

1. Take an existing Firecracker orchestrator (firecracker-containerd, or Flintlock, or a minimal Rust wrapper around the Firecracker API socket).
2. Boot a microVM with a static `tokeira-bundle-runtime` binary.
3. Measure: boot latency, DSQL commit latency from inside the VM, memory overhead, network path latency.
4. If the numbers are interesting, then design the host agent.

Building the host agent before having latency numbers is premature.

### 5. Snapshot restore deserves more skepticism

The doc mentions snapshot restore as a Phase 3 optimization and correctly cites the Brooker uniqueness paper. But it underweights the practical difficulty.

Tokeira's bundle runtime would hold:
- DSQL connection pools (with IAM auth tokens that expire)
- TLS sessions
- In-flight OCC retry state
- Bundle lease epoch state
- Monotonic clocks used for lease expiry checks

Restoring a snapshot means all of these are stale. The runtime must detect and recover every one of them. This is not just "regenerate UUIDs" — it's "re-establish every stateful external connection and re-validate every lease." At that point, the snapshot advantage over a clean cold boot shrinks significantly.

**My preference:** defer snapshots indefinitely. Warm pools of pre-booted but unleased VMs are simpler and avoid the snapshot state hygiene problem entirely. If boot latency is the concern, Firecracker's 125ms boot time with a minimal kernel is already fast enough for a warm pool approach.

### 6. Missing: the "just use processes" alternative

The doc frames the choice as ECS containers vs. Firecracker microVMs. But there's a middle ground that gets most of the locality benefits without the VM overhead:

**Dedicated OS processes per bundle range, managed by the runtime node itself.**

A runtime node could fork a child process for a hot bundle range, giving it:
- process-level isolation (separate address space, cgroups, seccomp)
- dedicated DSQL connection pool
- bundle-local actor cache
- independent failure containment (child crash doesn't take the parent)

This is weaker isolation than a microVM but stronger than in-process lanes, and it requires zero new infrastructure. The runtime already owns the bundle lease; it just needs to delegate execution to a child process instead of an in-process lane.

If the goal is locality and cache warmth, processes may be sufficient. If the goal is hard multi-tenant isolation (untrusted code execution), then Firecracker is justified — but Tokeira doesn't run user code, it runs workflow state machines. The isolation threat model should be stated explicitly.

### 7. The trigger criteria are missing

The doc says "build ECS first, add Firecracker later" but doesn't define what "later" means. Without explicit triggers, "later" becomes "never" or "when someone is excited about it."

**Proposed triggers for revisiting Firecracker:**

1. **Isolation trigger:** A customer or compliance requirement demands VM-level isolation between tenant bundle groups, and process/container isolation is formally insufficient.
2. **Locality trigger:** Production metrics show that bundle cache miss rate exceeds 30% due to ECS task churn, and the miss penalty is measurable in p99 transition latency.
3. **Noisy neighbor trigger:** Production metrics show that co-located bundle ranges on the same ECS task cause measurable latency interference that cgroup controls cannot mitigate.
4. **Density trigger:** The runtime needs to run more bundle ranges per host than ECS task placement allows, and Firecracker's lower per-VM overhead enables meaningfully higher density.

If none of these triggers fire, Firecracker stays on the shelf. That's a good outcome.

## What I Would Keep

- The core design rule: "Firecracker placement is locality. DSQL bundle lease epochs are authority." This is exactly right and should survive into any future design.
- The Amdahl-style performance model. Honest about the DSQL bound.
- The non-goals list. Clear about what Firecracker should not attempt.
- The failure mode catalog. Slot stampede, stale routes, orphaned VMs, granularity mismatch — all real.
- The security notes, especially snapshot uniqueness.
- The recommendation to keep Firecracker as an opt-in substrate, not a replacement.

## Revised Recommendation

1. **Do not build any Firecracker infrastructure until the ECS runtime is production-proven and bundle lease fencing is battle-tested.** The placement controller, bundle leases, and DSQL fencing are prerequisites. They don't exist yet.

2. **When the ECS runtime is stable, measure before designing.** Boot a Firecracker VM on a test host. Run a trivial DSQL commit loop inside it. Compare latency and throughput to the same loop in an ECS container. If the numbers aren't interesting, stop.

3. **If Firecracker is pursued, integrate it into the existing placement controller** rather than building a parallel Bundle Manager. The placement controller already understands bundles, queue partitions, and node health.

4. **Skip snapshots. Use warm pools.** Pre-boot unleased VMs. Assign them to bundle ranges on demand. Avoid the snapshot state hygiene problem entirely.

5. **Consider the process-per-bundle-range alternative first.** It's cheaper, simpler, and may be sufficient for locality and fault containment without any new infrastructure.

6. **Define explicit triggers** for when Firecracker becomes worth revisiting. Without triggers, the exploration is academic.

## Proposed Doc Structure

If this revision is accepted, I'd restructure 110 as:

```
110-firecracker-shard-bundle-orchestrator.md
  - Problem (keep, tighten)
  - Core design rule (keep)
  - Performance model (keep, adjust DSQL fraction estimates upward)
  - Alternative: process-per-bundle-range (new)
  - Decision: ECS first (keep)
  - Trigger criteria for revisiting (new)
  - References (keep)

111-firecracker-bundle-slot-design.md (future, written when triggered)
  - Component design (moved from 110)
  - Slot lifecycle
  - Host agent design
  - Warm pool design
  - Integration with placement controller
```

This keeps 110 as a decision record and defers the engineering design to when it's needed.
