# Placement / Membership Simulator

Discrete-event simulator that validates the safety invariants of Tokeira's
placement and membership design described in
[035-placement-and-membership](../../docs/architecture/035-placement-and-membership.md)
and the [shard-placement-membership spec](../../.kiro/specs/shard-placement-membership/).

This is not a mathematical proof. It is a deterministic, adversarial
simulator that repeatedly tries to falsify the design by injecting
faults — stale routing, concurrent commits, runtime crashes, lease
expiry, delayed snapshots, and drain races — then checks that the
safety invariants still hold.

## Verification modes

### 1. Seeded stress simulator

Randomised discrete-event simulation with configurable seeds, ops,
and time range. Exercises the full event space including concurrent
OCC commits, controller repair latency, two-phase drain, and stale
routing recovery. Each seed produces a deterministic event sequence
so failures are reproducible.

### 2. Bounded exhaustive checker

Enumerates all reachable states up to a configurable depth over a
tiny model (2 bundles, 2 runtimes, 1 workflow). Explores every
interleaving — closer to model checking than simulation. Catches
protocol-shape bugs that random scheduling might miss. For example,
`--buggy-start-routing` is caught at depth 1 (one step) because
the exhaustive checker immediately tries `StartWorkflow` and
detects that it routes to the wrong bundle.

## What is being simulated

The stress simulator models the four participants in Tokeira's placement
architecture:

### DSQL (the source of truth)

A simplified model of the `bundle_lease` table with epoch-fenced
ownership. The key property: a runtime may commit a workflow
transition only if it holds the current lease epoch for the bundle.
The model implements acquire, renew, relinquish, and OCC-style
commit with transaction-start epoch snapshots that are validated
at commit time — not at read time. This is the critical DSQL OCC
behavior that prevents stale owners from committing after an epoch
change.

### Runtimes (bundle owners)

Each runtime maintains local state about which bundles it believes
it owns. Runtimes acquire bundles from DSQL, renew leases
periodically, and relinquish bundles during drain. The simulator
injects faults that create stale local ownership:

- **Disabled renewals**: a runtime keeps serving but stops renewing
  its lease, so the lease expires while the runtime still believes
  it owns the bundle.
- **Crashes**: a runtime loses all local state instantly. Its leases
  expire naturally in DSQL.
- **Concurrent commits**: two runtimes attempt to commit for the
  same bundle simultaneously. The DSQL OCC model ensures at most
  one succeeds.

### Controllers (active-active observers)

The simulator does not model controller leader election because the
revised design uses active-active controllers. Instead, it models
the controller's core job: periodically reading DSQL lease state
and publishing routing snapshots to the edge. Snapshot delivery is
deliberately delayed and jittered to create stale routing at the
edge — the exact scenario that `NotShardOwner` recovery must handle.

Controllers also issue advisory placement directives: when a bundle
is unowned or expired, the controller tells a live runtime to
acquire it. DSQL decides actual ownership.

### Edge (routing cache)

The edge maintains a cached routing snapshot mapping bundles to
their owners. The simulator exercises the full stale-routing
recovery path:

1. Edge routes a request using its cached snapshot.
2. The target runtime rejects with `NotShardOwner` (wrong epoch or
   not alive).
3. Edge initiates repair — modeled with controller RPC latency
   (5–30ms jitter) before the DSQL lease lookup resolves.
4. Edge retries with the updated route.

## How this relates to 035-placement-and-membership

The architecture doc establishes five core principles. The simulator
validates each:

| Principle | How the simulator validates it |
|---|---|
| **DSQL owns truth** | Every commit attempt goes through the DSQL model's epoch fence. Stale owners are rejected. |
| **Membership is advisory** | Controllers observe DSQL and publish snapshots, but the edge can route with stale snapshots. Correctness comes from the DSQL fence, not from snapshot freshness. |
| **No coordination store on the hot path** | The edge routes from its local cache. DSQL is consulted only during commit (by the runtime) and during repair (by the edge after NotShardOwner). |
| **Routing granularity is finer than lease granularity** | Queue partitions and bundles are separate concepts. The simulator tracks both but only bundles carry authoritative ownership. |
| **The workflow run is the unit of correctness** | The DSQL commit fence operates per-bundle, and the simulator checks that no two runtimes can successfully commit for the same bundle at conflicting epochs. |

The simulator also validates the design thesis stated in the spec:

> DSQL owns truth. Runtimes own bundles only by DSQL epoch lease.
> Controllers observe actual ownership and compute desired movement.
> Edges consume routing hints. Every stale route is repaired by
> NotShardOwner.

## Invariants checked

Six invariants are checked after every event:

| Invariant | What it checks |
|---|---|
| **I1 — Single Owner** | At most one runtime owns a given bundle at any simulated instant. If two runtimes both have the same bundle in their `owned_bundles` map, I1 fails. |
| **I2 — Epoch Monotonicity** | Bundle epochs never decrease. Every acquire, renew, or relinquish must produce an epoch ≥ the previous epoch for that bundle. |
| **I3 — Fence Rejection** | A stale owner (wrong epoch) cannot successfully commit a workflow transition. If a commit succeeds but the committer is not the current DSQL owner, I3 fails. |
| **I4 — Edge Convergence** | After a `NotShardOwner` error and repair, the edge converges to the correct owner within a bounded number of retries (default: 3). Only checked when a live owner exists. |
| **I5 — Lease Expiry Takeover** | An expired lease can be taken over by another runtime. If a lease is expired but acquisition fails, I5 fails. |
| **I6 — Two-Phase Drain** | During drain, no bundle is relinquished before the routing snapshot has been updated to stop sending new work to the draining node. Validates the two-phase drain protocol from the spec. |
| **I6 — Two-Phase Drain** | During drain, no bundle is relinquished before the routing snapshot has been updated to stop sending new work to the draining node. This validates the two-phase drain protocol from the spec. |

## Fault injection

The simulator injects these faults to exercise edge cases:

| Fault | What it creates | Which invariant it stresses |
|---|---|---|
| **Delayed snapshot delivery** | Edge routes with stale ownership information | I3, I4 |
| **Concurrent commits** | Two runtimes attempt to commit for the same bundle simultaneously, with DSQL OCC resolving the race at commit time | I1, I3 |
| **Runtime crash** | Runtime loses all local state; leases expire naturally | I1, I5 |
| **Disabled renewals** | Runtime keeps serving but its lease expires, creating a stale local owner | I3 |
| **Lease expiry + takeover** | A new runtime acquires an expired lease while the old owner may still have local state | I1, I2, I5 |
| **Two-phase drain** | Routing snapshot update, then bundle relinquishment, with jittered timing | I6 |
| **Controller repair latency** | 5–30ms delay on edge repair, during which the edge has no valid route | I4 |

## OCC transaction model

The most important modeling detail is how DSQL OCC is represented.
Real DSQL uses Repeatable Read with optimistic concurrency control:
a transaction reads data, does work, and at commit time DSQL checks
whether any read data was modified by another transaction. If so,
the commit is rejected with SQLSTATE 40001.

The simulator models this by splitting commits into two phases:

1. **Begin transaction** (`begin_transaction`): snapshots the
   current epoch for the bundle. This represents the `SELECT epoch
   FROM bundle_lease WHERE bundle_id = $1 FOR UPDATE` at the start
   of a real DSQL transaction.

2. **Commit attempt** (`commit`): scheduled 1–5ms later. At commit
   time, the fence check compares the snapshotted epoch against the
   *current* epoch. If another runtime acquired the bundle (bumping
   the epoch) between begin and commit, the commit is rejected.

This two-phase model is what allows the simulator to detect
concurrent stale-owner races — the scenario identified as a TLA+
verification candidate in the spec's safety analysis.

## Usage

```bash
cd tools/placement-sim
cargo run --release

# Stress test
cargo run --release -- --seeds 500 --ops 3000 --time-ms 10000

# Single seed with full event trace
cargo run --release -- --seeds 1 --verbose --random-only

# Exhaustive checker only with deeper exploration
cargo run --release -- --exhaustive-only --exhaustive-depth 14

# Inject execution-home routing bug (caught at depth 1 by exhaustive checker)
cargo run --release -- --buggy-start-routing
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `--seeds N` | 250 | Number of random seeds to test |
| `--ops N` | 800 | Number of random events per seed |
| `--time-ms N` | 6000 | Simulation time range in milliseconds |
| `--verbose` | off | Print detailed event log (use with `--seeds 1`) |
| `--buggy-start-routing` | off | Route Start by queue-home instead of execution-home |
| `--random-only` | off | Skip the exhaustive checker, run only the stress simulator |
| `--exhaustive-only` | off | Skip the stress simulator, run only the exhaustive checker |
| `--exhaustive-depth N` | 12 | Maximum depth for the exhaustive checker |

### Reading the output

```
=== Aggregate Results (500 seeds) ===
Total commits:            16122    # successful workflow transitions
Total fence rejections:   1936     # stale owners caught by DSQL fence
Total concurrent races:   2401     # two commits in-flight for same bundle
Total edge repairs:       440394   # NotShardOwner recovery attempts
Total drain events:       19037    # two-phase drain protocol executions
Total lease takeovers:    54808    # expired leases acquired by new owners
```

A healthy run shows:
- **Fence rejections > 0**: the simulator is creating stale owners
  and DSQL is catching them.
- **Concurrent races > 0**: the OCC model is exercising simultaneous
  commits.
- **Edge repairs > 0**: stale routing is being repaired.
- **All invariants PASSED**: the design holds under adversarial
  scheduling.

## Limitations

This simulator does not model:

- **Network partitions** between runtimes and DSQL. Real DSQL
  connection failures would prevent both reads and commits, which
  is a different failure mode than stale local state.
- **Connection budget allocation**. The connection rate limiter and
  reservoir are liveness/performance concerns, not safety concerns.
- **Queue-home placement**. Queue partitions are tracked but not
  used for routing decisions in the simulator. The spec's
  execution-home routing is the correctness boundary; queue-home
  is an optimization.
- **Multi-cell placement**. The simulator models a single DSQL
  cluster. Multi-cell placement (per 037-dynamic-placement) is a
  future concern.
- **Real DSQL transaction isolation**. The simulator models OCC
  semantics faithfully, but it cannot prove that Aurora DSQL's
  actual implementation matches. Black-box integration tests
  against a live DSQL cluster are needed for that.
