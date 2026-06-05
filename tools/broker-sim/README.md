# Delivery Broker Simulator

Discrete-event simulator that falsifies the safety and liveness invariants of
Tokeira's delivery broker design
([040-delivery-broker](../../docs/architecture/040-delivery-broker.md), implemented
in [`crates/tokeira-runtime/src/broker.rs`](../../crates/tokeira-runtime/src/broker.rs))
and the [delivery-broker-simulator spec](../../.kiro/specs/delivery-broker-simulator/).

Like [`tools/placement-sim`](../placement-sim/), this is not a proof. It is a
deterministic, adversarial simulator that repeatedly tries to break the broker's
central correctness claim under injected faults, then checks that the invariants
still hold after every event.

It is built on the shared [`tools/sim-harness`](../sim-harness/) library — the
reusable event-queue, RNG, invariant-registry, fault-injection, bounded-exhaustive
enumerator, reporting, and CLI machinery generalised from `placement-sim`. The
broker model imports neither `tokeira-runtime` nor `tokio`; it **re-models** the
broker's semantics as a pure deterministic state machine. The fidelity risk —
the model drifting from `broker.rs` as the broker evolves — is accepted and
managed by keeping the model's vocabulary aligned with the code (`QueueKey`,
`sticky_ready`/`general_ready`, the `(run, logical_seq)` / `(run, activity_id,
attempt)` dedup keys, `denied_workers`, the reservation/commit split). Keep the
two aligned when the broker changes.

## The central correctness claim being falsified

Doc 040 ("Why this is safe" + "Sweeper contract") states that authoritative
pending-task state lives **with the run** (`workflow_hot.pending_wft`,
`activity_state`), never in a broker/queue row. If the broker process dies before
durable backlog is written, a sweeper reconstructs delivery candidates from
authoritative state. The broker is therefore a **disposable delivery optimiser**:
it may duplicate, delay, expire, or redeliver, but it must never make workflow
state true by itself, and losing it must lose no work and complete nothing.

The model embodies this by holding `AuthoritativePendingState` (the per-run truth)
separately from `BrokerState` (everything a crash discards). A `BrokerCrash` event
drops the latter; the sweeper rebuilds the live tiers from the former.

## Verification modes

### 1. Seeded stress simulator

Randomised discrete-event simulation over configurable seeds, ops, and time
range. Exercises the full event space — three-tier delivery, reservation/commit,
sticky promotion, dedup, grace-scan backlog spill, denied workers, the sweeper,
the control loop — and injects the fault set below. Safety invariants are checked
after every event; liveness invariants at the run's quiescent point. Each seed is
reproducible.

### 2. Bounded exhaustive checker

Enumerates every interleaving of a tiny model (one run, one queue, two workers,
≤1 WFT) up to a depth bound — closer to model checking. Catches protocol-shape
bugs random scheduling misses, and is where an injected bug surfaces at shallow
depth with a shortest-path counterexample.

## Invariants

Safety (checked after every event; must hold under all schedules):

| Invariant | Property |
|---|---|
| **S1** | At most one in-flight workflow task per run (activities may run concurrently). |
| **S2** | No double-start: at most one live delivery per logical task. |
| **S3** | Reservation⇄commit coupling: a held token implies a committed start transaction. |
| **S4** | Stale completion rejection: a completed task is not also multiply live. |
| **S5** | Broker restart is disposable: a crash loses no authoritative pending task and completes nothing. |
| **S6** | Duplicate publication safety: a logical task appears at most once across ready tiers + backlog. |
| **S7** | Sticky safety: an expired sticky claim is promoted to general, never dropped. |

Liveness (checked at quiescence under healthy / bounded-adversary runs):

| Invariant | Property |
|---|---|
| **L1** | Eventual delivery / no loss: every authoritative pending task is eventually completed, including after a crash via the sweeper. |
| **L2** | Bounded poller memory: waiters per queue never exceed the cap; excess polls are rejected. |
| **L3** | Long polls resolve cleanly: no waiter remains parked at quiescence; polls never allocate a durable resource. |
| **L4** | Backlog fairness, no starvation: fairness applies only on durable backlog; the control loop never lets the backlog share starve fresh sync-matchable work. |

## Fault → invariant map

The simulator injects these adversarial faults, each stressing the named
invariants (mirroring the `placement-sim` README table):

| Fault | What it creates | Stresses |
|---|---|---|
| Broker crash | `BrokerState` discarded; sweeper must rebuild from authoritative state | S5, L1 |
| Delivery lease expiry | Redelivery; the prior delivery's completion becomes stale | S4, L1 |
| Worker crash | A worker's in-flight deliveries lapse like a lease | L1, S5 |
| Start-txn abort | Reservation does not commit; the reserved poller must be returned | S3 |
| Sticky-TTL expiry | A sticky claim must promote to general, not bind to the lost worker | S7 |
| Duplicate publish | The same logical task re-published; dedup must suppress it | S6, S2 |
| Poller storm | Polls beyond `max_waiters`; excess must be rejected, no durable loss | L2 |
| Partition backlog pressure | Backlog on one partition while pollers wait on another | L4, sync-match rate |
| Sustained backlog age | Drives the control loop to raise the backlog share | L4 |
| Worker denial | A worker barred from a queue must not receive its tasks | (delivery routing) |

## Injectable known bug

Like `placement-sim`'s `--buggy-start-routing`, a deliberately-incorrect broker
variant proves the simulator has real falsifying power. Select one with `--bug`:

| `--bug` value | Defect | Caught by |
|---|---|---|
| `token-before-commit` | Hands the worker a token before the start transaction commits | exhaustive (S3, shallow depth) + stress |
| `drop-expired-sticky` | Drops an expired sticky claim instead of promoting it | exhaustive (loss) + stress |
| `no-dedup-on-republish` | Skips the dedup check on a re-published task | stress (S6 / S2) — the exhaustive model does not republish |

When `--bug` is set, a falsification is the **expected** outcome (reported as
"bug correctly falsified"), not a regression.

## Usage

```bash
cd tools/broker-sim
cargo run --release

# Stress only, more seeds
cargo run --release -- --random-only --seeds 500 --ops 600 --time-ms 8000

# Exhaustive only, deeper
cargo run --release -- --exhaustive-only --exhaustive-depth 16

# Demonstrate the simulator catching a planted bug
cargo run --release -- --bug=token-before-commit
```

### CLI flags

Shared with the simulator family (via `sim-harness`): `--seeds` (250), `--ops`
(800), `--time-ms` (6000), `--verbose`, `--exhaustive-depth` (12),
`--random-only`, `--exhaustive-only`. Broker-specific: `--bug=<name>`.

## What a healthy run looks like

A healthy run shows all of `S1`–`S7` and `L1`–`L4` **PASS** with non-zero signal
activity: `tier_a_inline` and `tier_b_live_ready` matches dominating,
`tier_c_backlog_spill` and `sweeper_rebuilds` exercised by the faults,
`sticky_promotions` and `redeliveries` occurring, and `duplicates_suppressed`
proving dedup is active. High sync-match rate, high poll-success rate, and a
non-starving backlog are the broker-health signals doc 040 cares about.

## Limitations

Mirroring `placement-sim`, this simulator does not model:

- **Real DSQL transaction-isolation fidelity** — the start transaction is modelled
  as a commit/abort coin flip, not Aurora DSQL OCC.
- **Network partitions** between the broker, runtime, and storage.
- **Multi-cell placement** — a single logical broker is modelled.

It re-models broker semantics for design and implementation confidence; it is not
the broker implementation (`crates/tokeira-runtime/src/broker.rs`) and must be
kept faithful to it as the broker evolves.
