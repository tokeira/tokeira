# Placement / Membership Simulator

Discrete-event simulator that falsifies the safety invariants of Tokeira's
placement and membership design
([035-placement-and-membership](../../../docs/architecture/035-placement-and-membership.md),
with the [shard-placement-membership spec](../../../.kiro/specs/shard-placement-membership/)).

Like [`tools/simulation/broker`](../broker/), this is not a proof. It is a
deterministic, adversarial simulator that repeatedly tries to break the
placement design's central correctness claim under injected faults, then checks
that the invariants still hold after every event.

It is built on the shared [`tools/simulation/engine`](../engine/) library — the
reusable event-queue, RNG, invariant-registry, fault-injection, bounded-exhaustive
enumerator, reporting, and CLI machinery. The placement model imports no Tokeira
server crate; it **re-models** the design as a pure deterministic state machine.
The fidelity risk — the model drifting from the real placement implementation as
it evolves — is accepted and managed by keeping the model's vocabulary aligned
with the design doc (bundle leases with fencing epochs, advisory edge routing,
execution-home vs queue-home, the OCC commit fence).

## The central correctness claim being falsified

Doc 035 establishes the thesis:

> DSQL owns truth. Runtime ownership is valid only by the current DSQL bundle
> lease epoch. Queue-home is advisory. Execution-home is the correctness boundary.

The model embodies this by splitting state into **authority** (the `Dsql`
struct: bundle-lease rows with fencing epochs, the workflow table, the durable
dedupe set) and **advisory belief** (the `Edge`'s cached routing snapshot and
each `Runtime`'s local ownership map). A stale edge route or a lapsed local
belief can only ever cause a fence miss and a retry — never a wrong write —
because every mutation is revalidated against the live DSQL epoch at commit time.

## Verification modes

### 1. Seeded stress simulator

Randomised discrete-event simulation over configurable seeds, ops, and time
range. Exercises the full event space — controller observation and snapshot
publication, lease acquire/renew/relinquish, the two-phase (begin/commit) data
plane, edge repair after `NotShardOwner`, and the two-phase drain — and injects
the fault set below. Safety invariants are checked after every event. Each seed
is reproducible.

### 2. Bounded exhaustive checker

Enumerates every interleaving of a tiny model (2 bundles, 2 runtimes, 1 workflow)
up to a depth bound — closer to model checking. It catches protocol-shape bugs
random scheduling can miss, and is where the injected bug surfaces at shallow
depth: `--bug=buggy-start-routing` is falsified at **depth 1**, because the very
first `StartWorkflow` resolves to the wrong bundle.

## What is being modelled

| Participant | Role in the model |
|---|---|
| **DSQL** (`model::Dsql`) | The source of truth: epoch-fenced bundle leases, the workflow table, durable request dedupe, and an OCC commit that snapshots the epoch at transaction begin and validates it at commit. |
| **Runtimes** (`model::Runtime`) | Bundle owners holding *advisory* local ownership belief. Faults make this belief stale (renewals suppressed, crash) — the case the fence must survive. |
| **Controllers** (`model::Controller`) | Active-active observers that read DSQL and publish routing snapshots to the edge with delivery latency and jitter (creating the stale routing `NotShardOwner` must repair). No leader election is modelled. |
| **Edge** (`model::Edge`) | The advisory routing cache. Routes optimistically, repairs one bundle at a time after a fence miss, and never regresses to an older epoch. |

### The OCC commit fence (the load-bearing detail)

Real DSQL uses optimistic concurrency control: a transaction reads at begin and
DSQL rejects the commit if the read data changed. The model splits a commit into
two events so this race is observable:

1. `RuntimeHandle` → **begin transaction**, snapshotting the bundle's epoch.
2. `CommitAttempt`, scheduled 1–5 ms later → the fence compares the snapshotted
   epoch against the *current* epoch. If another runtime acquired the bundle in
   between (bumping the epoch), the commit is fenced out.

This temporal split is what lets the simulator catch a stale owner that commits
after an epoch change — the scenario the spec flags for formal verification.

## Invariants

Safety (checked after every event; must hold under all schedules):

| Invariant | Property |
|---|---|
| **I1** | Execution-home boundary: every committed workflow lives on its canonical execution-home, and no commit resolves off it (the buggy-routing detector). |
| **I2** | Durable request dedupe: no request id applies more than once. |
| **I3** | Dedupe-set integrity: every started workflow's request is in the applied set, and the applied set coincides with the positive apply-count set. |
| **I4** | No future epoch at the edge: the advisory cache may lag but never names an epoch newer than DSQL has issued. |
| **I5** | No orphaned live lease: an unowned lease row never carries a future expiry. |
| **I6** | Two-phase drain ordering: a draining runtime withdraws from routing before it relinquishes any lease. |

## Fault → invariant map

The simulator injects these adversarial faults, each stressing the named
invariants (mirroring the `broker` README table):

| Fault | What it creates | Stresses |
|---|---|---|
| Renewal suppression | A runtime keeps its local belief while its lease lapses | I1, I4 (via fence misses) |
| Runtime crash + restart | Local state lost instantly; a fresh incarnation (new id) replaces it | I1, I5 |
| Graceful drain | Two-phase routing-withdraw-then-relinquish, jittered | I6 |
| Delayed snapshot delivery | Edge routes on a stale snapshot until repair | I1, I4 |
| Lease expiry + takeover | A new owner acquires an expired lease under the old owner's stale belief | I1, I5 |
| Controller repair latency | 5–30 ms window where the edge has no valid route | I4 |
| Concurrent commit race | Two in-flight commits for one bundle, resolved by the OCC fence | I1, I2 |

## Injectable known bug

Like the broker simulator's `--bug` defects, a deliberately-incorrect variant
proves the simulator has real falsifying power. Select it with `--bug`:

| `--bug` value | Defect | Caught by |
|---|---|---|
| `buggy-start-routing` | Routes `Start` by the advisory queue-home instead of the execution-home | exhaustive (I1, depth 1) + stress |

When `--bug` is set, a falsification is the **expected** outcome (reported as
"bug correctly falsified"), not a regression.

## Usage

```bash
cd tools/simulation/placement
cargo run --release

# Stress only, more seeds
cargo run --release -- --random-only --seeds 500 --ops 3000 --time-ms 10000

# Single seed with a full event trace
cargo run --release -- --random-only --seeds 1 --verbose

# Exhaustive only, deeper
cargo run --release -- --exhaustive-only --exhaustive-depth 14

# Demonstrate the simulator catching the planted bug
cargo run --release -- --bug=buggy-start-routing
```

### CLI flags

Shared with the simulator family (via `sim-engine`): `--seeds` (250), `--ops`
(800), `--time-ms` (6000), `--verbose`, `--exhaustive-depth` (12),
`--random-only`, `--exhaustive-only`. Placement-specific: `--bug=<name>`.

## What a healthy run looks like

A healthy run shows all of `I1`–`I6` **PASS** with non-zero signal activity:
`successful_mutations` and `signals_applied` dominating, `fence_rejections` and
`not_shard_owner` proving stale owners are being created and caught,
`edge_repairs` proving stale routes are repaired, and `crashes`/`drains`/
`renewal_suspensions` proving the fault paths fire. A run with zero fence
rejections is suspicious — it means the adversary never created a stale owner.

## Limitations

Mirroring the broker simulator, this does not model:

- **Real DSQL transaction-isolation fidelity** — the fence models OCC semantics
  faithfully but cannot prove Aurora DSQL's implementation matches; black-box
  tests against a live cluster are needed for that.
- **Network partitions** between runtimes, controllers, and DSQL.
- **Connection budget / reservoir** — a liveness/performance concern, not safety.
- **Multi-cell placement** (per 037-dynamic-placement) — a single cluster is modelled.

It re-models placement semantics for design and implementation confidence; it is
not the placement implementation and must be kept faithful to the design as it
evolves.
