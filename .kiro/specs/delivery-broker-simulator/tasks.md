# Implementation Plan: Delivery Broker Simulator and Shared Simulation Engine

## Overview

Build, in dependency order, the reusable simulation engine (`tools/simulation/engine`, library) and the
delivery-broker simulator (`tools/simulation/broker`, binary) on top of it, strictly per `design.md`.
The engine generalises the proven `tools/simulation/placement` mechanics (xorshift64 RNG, `BinaryHeap`
event queue with `(at_ms, seq)` ordering, per-event invariant checks, bounded-exhaustive
enumeration with visited-state pruning, aggregate reporting, CLI scaffolding) behind small traits
so the broker model — and the future admission-control / connection-management simulators — drive
it without engine changes. The broker model is a pure deterministic re-model of
`crates/tokeira-runtime/src/broker.rs` (it imports neither `tokeira-runtime` nor `tokio`) and
falsifies the central correctness claim: authoritative pending state lives with the run, the broker
is a disposable optimiser. Both crates are `publish = false`, depend on no live AWS/Docker/network,
and are deterministic from a seed. All eleven invariants (S1–S7, L1–L4) are implemented and checked
in both verification modes.

## Tasks

- [x] 1. Engine: deterministic core (event queue + RNG)
  - [x] 1.1 Scaffold the `tools/simulation/engine` library crate
    - Create `tools/simulation/engine/Cargo.toml` (`publish = false`, `edition = "2024"`, no `tokio`, no `proptest` in non-dev deps, no Tokeira-crate dependency) and `src/lib.rs` exposing the `rng`, `event`, `invariant`, `fault`, `enumerate`, `report`, `cli` modules.
    - _Requirements: 7.1, 7.2, 34.1, 34.2, 34.3, 34.5_
  - [x] 1.2 Implement `Rng` in `tools/simulation/engine/src/rng.rs`
    - Port `placement-sim`'s `XorShift64` exactly (`new(seed.max(1))`, `next_u64`, `range(start, end_exclusive)` asserting `start < end`); add `bool_with_percent`.
    - _Requirements: 1.3, 1.4, 6.6, 34.5_
  - [x] 1.3 Implement `EventQueue<E>`, `Scheduled<E>`, and `SimCtx<'a, E>` in `tools/simulation/engine/src/event.rs`
    - `Scheduled { at_ms, seq, event }` ordered by `(at_ms asc, seq asc)` via a reverse-ordered `BinaryHeap` and a monotonic `seq` tie-breaker; `SimCtx` exposes `now_ms`, `rng()`, and `schedule(delay_ms, event)` enqueuing at `now_ms + delay_ms`; no wall-clock or I/O.
    - _Requirements: 1.1, 1.2, 1.5, 1.6, 1.7_
  - [x] 1.4 Unit tests for RNG determinism and event ordering
    - Same seed → identical `next_u64` stream; events with equal `at_ms` drain in `seq` order; `schedule` honors `now_ms + delay`.
    - _Requirements: 1.1, 1.2, 1.4_

- [x] 2. Engine: invariants, faults, reporting, CLI
  - [x] 2.1 Implement the invariant registry in `tools/simulation/engine/src/invariant.rs`
    - Define `InvariantClass {Safety, Liveness}`, `Invariant<M> { name, class, check: fn(&M) -> Option<String> }`, `InvariantRegistry<M>` with `register`, `check_safety` (after every event), `check_liveness` (at quiescent point), and per-name PASS/FAIL aggregation; define `Violation { invariant, reason }`.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_
  - [x] 2.2 Implement the fault framework in `tools/simulation/engine/src/fault.rs`
    - Define `Fault<E> { name, schedule: fn(&mut SimCtx<'_, E>) }`, `FaultConfig` (enable/disable by name), and `FaultInjector<E>` that schedules enabled faults using only the `Rng` and records per-fault injection counts.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_
  - [x] 2.3 Implement `SignalCounters` and `Report` in `tools/simulation/engine/src/report.rs`
    - `SignalCounters` is a `BTreeMap<&'static str, u64>` with `incr`/`add`/`get` (model-defined names only); `Report` aggregates signals summed across seeds, per-name invariant PASS/FAIL, `overall_passed()` (false if any safety FAIL), and retains the first failing seed + `Violation`; `print()` renders signals + per-invariant lines.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_
  - [x] 2.4 Implement the `StressModel` trait and `StressRunner` in `tools/simulation/engine/src/lib.rs` (or `stress.rs`)
    - `StressModel { type Event; bootstrap; handle; signals; is_quiescent }`; `StressRunner` drives one seed: bootstrap → drain `EventQueue` (stop past the time bound) → after each `handle` call `check_safety`, at the quiescent point / run end call `check_liveness`; produce a `SeedReport`; surface a `Failure` (seed, now_ms, violated invariant, recent-event tail) on violation.
    - _Requirements: 1.7, 2.2, 2.5, 8.1, 8.5, 30.2_
  - [x] 2.5 Implement the bounded-exhaustive enumerator in `tools/simulation/engine/src/enumerate.rs`
    - `ExhaustiveModel { Action; initial; actions; apply -> Result<(),String>; check -> Option<String> }`; `run_bounded_exhaustive<M>(max_depth)` doing DFS with `placement-sim`'s `best_remaining_by_state: HashMap<M, usize>` pruning (skip revisits with `remaining <= previous_best`), checking invariants on the initial state and after every transition, returning the shortest `Counterexample { depth, message, path }` or an `EnumReport { states_explored, transitions_tried }`.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_
  - [x] 2.6 Implement CLI scaffolding in `tools/simulation/engine/src/cli.rs`
    - `CliSpec { extra_flags }` and `parse(&CliSpec) -> CliArgs` handling `--seeds` (250), `--ops` (800), `--time-ms` (6000), `--verbose`, `--exhaustive-depth` (12), `--random-only`, `--exhaustive-only`, plus model-registered extra flags (incl. a buggy-mode flag); unknown flags panic with a message as `placement-sim` does.
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_
  - [x] 2.7 Unit tests for registry, enumerator, report, and CLI
    - Registry: a safety violation records FAIL and stops the seed; liveness evaluated at quiescence. Enumerator: visited-state pruning bounds exploration; a planted check failure returns the shortest path. Report: safety FAIL in any seed → `overall_passed() == false`; signals sum across seeds. CLI: shared + extra flags parse; `--random-only`/`--exhaustive-only` set the run toggles.
    - _Requirements: 2.3, 2.4, 4.3, 4.5, 5.3, 6.2, 6.5_

- [x] 3. Checkpoint — engine foundation
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p sim-engine`.
  - Ensure all tests pass; ask the user if questions arise.

- [x] 4. Engine reusability validation
  - [x] 4.1 Add a trivial second `StressModel` + `ExhaustiveModel` in `tools/simulation/engine` tests
    - In `#[cfg(test)]`, implement a few-line throwaway model (e.g. a bounded counter with one safety invariant and one fault) against both traits and run it through `StressRunner` and `run_bounded_exhaustive` to prove the engine API carries no broker/placement-specific assumptions — the lightweight stand-in for the future admission-control (055) and connection-management (060) consumers.
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [x] 5. Broker model: domain types and pure state
  - [x] 5.1 Scaffold the `tools/simulation/broker` binary crate
    - Create `tools/simulation/broker/Cargo.toml` (`publish = false`, `edition = "2024"`, depends on `sim-engine`; `proptest` as a dev-dependency only; no `tokeira-runtime`, no `tokio`) and `src/main.rs` plus the `model`, `events`, `invariants`, `faults`, `exhaustive`, `bug` modules.
    - _Requirements: 8.2, 34.1, 34.2, 34.3, 34.4, 35.2_
  - [x] 5.2 Define the re-modelled domain identifiers and keys in `tools/simulation/broker/src/model.rs`
    - Local newtypes (`NamespaceId`, `TaskQueueName`, `WorkerIdentity`, `RunKey`, `LogicalTaskSeq`, `ActivityId`, `Attempt`, `BuildId`, `DeploymentName`, `DeliveryId`, `Revision`), `TaskKind`, `QueueKey { namespace, task_queue, kind, deployment, build, partition }`, and `LogicalTaskId::{Wft(RunKey, LogicalTaskSeq), Activity(RunKey, ActivityId, Attempt)}`, matching `crates/tokeira-runtime/src/broker.rs` keying. Do not import `tokeira-runtime`.
    - _Requirements: 8.2, 8.3, 12.1, 14.1, 20.1_
  - [x] 5.3 Define `BrokerState` and `AuthoritativePendingState` in `tools/simulation/broker/src/model.rs`
    - `BrokerState` holds `sticky_ready`/`general_ready` (Tier B), `backlog` (Tier C, priority-ordered), `waiters`/`waiter_counts`, `enqueued` (dedup), `query_ready`, `denied_workers`, `inflight: HashMap<LogicalTaskId, Delivery>`, `budget: BudgetSplit`, and per-queue `quality`. `AuthoritativePendingState` holds `pending_wft` (≤1 per run), `pending_activities`, `started_count` (for S2), and `expired_sticky` (for S7) — held separately so discarding `BrokerState` never loses authoritative tasks.
    - _Requirements: 8.4, 9.x, 16.x, 17.2_
  - [x] 5.4 Define `BrokerCfg` and `WorkloadShape`
    - `BrokerCfg { grace_window_ms, max_waiters, partitions_per_queue, sticky_ttl_ms, budget_bands, lease_ms, quality_targets }` (existence/meaning fixed; values are defaults per Deferred Questions 3–6); `WorkloadShape` holds the run/queue/worker id pools the bootstrap samples from.
    - _Requirements: 13.1, 21.2, 25.2, 31.2, 22.5_

- [x] 6. Broker model: events and delivery semantics
  - [x] 6.1 Define `BrokerEvent` in `tools/simulation/broker/src/events.rs`
    - The full taxonomy from the design: `PublishWft`, `PublishActivity`, `PublishQuery`, `Poll`, `DirectClaim`, `ReserveAndStart`, `StartTxnCommit`, `CompleteTask`, `GraceScan`, `StickyTtlExpire`, `PollDeadline`, `ControlLoopTick`, and the fault events (`BrokerCrash`, `LeaseExpire`, `WorkerCrash`, `DenyWorker`, `PartitionBacklogPressure`, `SustainedBacklogAge`, `DuplicatePublish`).
    - _Requirements: 8.1, 9.x, 10.x, 13.x, 14.x, 15.x, 16.x, 18.x, 19.x, 20.x, 21.x_
  - [x] 6.2 Implement publish + dedup + tier placement (`handle` for `PublishWft`/`PublishActivity`)
    - Suppress an already-`enqueued` `LogicalTaskId` (clearing the key on grace-scan spill so it can be republished); place sticky-preferred tasks in `sticky_ready` else `general_ready`; on publish-with-waiter record Tier_A inline match, else Tier_B; record `published`/`published_with_waiter` for sync-match rate; record signals.
    - _Requirements: 9.1, 9.2, 9.4, 12.1, 12.2, 12.3, 12.4, 22.1_
  - [x] 6.3 Implement the poll tier-ladder and reservation/commit split
    - `Poll` resolves through denied-worker check → sticky-exact → general live/ready → backlog-offer (budget-permitting, fairness pick) → register memory-only waiter + schedule `PollDeadline`. A match emits `ReserveAndStart`; `StartTxnCommit { will_commit }` either delivers the token (`inflight[id]=Delivery{committed:true}`, `started_count++`) or returns the reserved poller to waiters and leaves the task deliverable. Record `tier_a/b/c`, `reservation_returns`, `reservation_aborts`, poll-success accounting.
    - _Requirements: 9.1, 9.3, 9.5, 10.1, 10.2, 10.3, 10.4, 10.5, 11.4, 14.2, 14.3, 15.1, 15.2, 15.3, 22.2_
  - [x] 6.4 Implement sticky promotion, grace scan, direct claim, and query path
    - `StickyTtlExpire` promotes a sticky task to general (records `sticky_promotions`); `GraceScan` spills aged live-ready tasks to backlog and clears their dedup key (records `tier_c_backlog_spill`); `DirectClaim` removes a run's task from the general tier only (never sticky), clears its dedup key, and prevents normal-poll re-delivery (records `direct_claims`); `PublishQuery`/query poll bypass dedup + backlog, prefer the matching sticky worker, allocate no durable resource (records `query_deliveries`/`poll_timeouts`).
    - _Requirements: 11.1, 11.2, 11.3, 11.5, 13.1, 13.2, 13.3, 13.4, 18.1, 18.2, 18.3, 18.4, 19.1, 19.2, 19.3, 19.4, 19.5_
  - [x] 6.5 Implement WFT-vs-AT separation, control loop, and quality accounting
    - Model the workflow-task and activity-task brokers as distinct instances (single-in-flight-WFT-per-run on WFT only; concurrent activities allowed; sticky/sweeper on WFT; a WFT-queue denial does not affect activity delivery); `ControlLoopTick` recomputes the `BudgetSplit` from backlog age bands (low age → sticky/live bias; high age → raised backlog share, never starving fresh sync-matchable work); maintain `QueueQuality` (sync-match rate, poll-success rate, schedule-to-start samples) per broker.
    - _Requirements: 17.1, 17.2, 17.3, 17.4, 17.5, 21.1, 21.2, 21.3, 21.4, 21.5, 22.3, 22.4, 22.5_
  - [x] 6.6 Implement the sweeper, crash, lease expiry, and partition pressure
    - `BrokerCrash` discards `BrokerState` (keeping `AuthoritativePendingState`); the sweeper reconstructs delivery candidates from authoritative pending WFTs/activities and expired sticky claims, republishing to live tiers or backlog and making expired sticky claims general-deliverable (records `sweeper_rebuilds`); `LeaseExpire` redelivers and marks prior-delivery completions stale (records `redeliveries`); `WorkerCrash` frees the worker's in-flight work; `PartitionBacklogPressure` builds backlog on one partition while pollers wait on another; `SustainedBacklogAge` drives the control loop.
    - _Requirements: 16.1, 16.2, 16.3, 16.4, 20.2, 20.3, 20.4_
  - [x] 6.7 Implement `bootstrap` (workload + fault schedule) and `StressModel` for `BrokerModel`
    - Seed the queue with the publish/poll workload and the enabled fault schedule (all via `SimCtx::rng`), register the broker's faults into the `FaultInjector`, expose `signals()`, and signal `is_quiescent()` when the queue is drained of workload and in-flight deliveries; implement `handle` dispatching to 6.2–6.6.
    - _Requirements: 3.1, 3.2, 8.1, 8.5, 32.1, 32.2_

- [x] 7. Broker model: invariants (S1–S7, L1–L4)
  - [x] 7.1 Implement the safety invariants in `tools/simulation/broker/src/invariants.rs`
    - Register `S1`–`S7` as `Invariant<BrokerModel>` (class Safety) with the design's Falsification_Conditions: S1 ≤1 in-flight WFT/run; S2 `started_count[id] ≤ 1`; S3 token only where `committed` and no stranded reservation; S4 stale delivery id cannot mutate authoritative state; S5 crash loses no authoritative task / marks nothing complete; S6 no second ready/backlog entry per id; S7 no sticky double-start and expired sticky becomes general.
    - _Requirements: 23, 24, 25, 26, 27, 28, 29_
  - [x] 7.2 Implement the liveness invariants in `tools/simulation/broker/src/invariants.rs`
    - Register `L1`–`L4` (class Liveness, evaluated at quiescence): L1 eventual delivery / no loss incl. post-crash via sweeper; L2 waiters ≤ `max_waiters`, excess rejected and counted, no durable loss; L3 every poll resolves work-or-timeout and allocates no durable resource; L4 fairness only on Tier C, FIFO within priority, no starvation, fresh sync-matchable work never blocked.
    - _Requirements: 30, 31, 32, 33_
  - [x] 7.3 Property test: tier-selection determinism and dedup (model-level, proptest)
    - In `tools/simulation/broker` `#[cfg(test)]` with workspace `proptest` (≥100 iterations): the pure tier-selection function is deterministic for a fixed state+poll, and republishing an enqueued id is suppressed. Tag `// Feature: delivery-broker-simulator, Property 2` and `... Property 6`.
    - _Requirements: 24, 28_
  - [x] 7.4 Property test: sticky promotion and reservation/commit coupling (model-level, proptest)
    - In `tools/simulation/broker` `#[cfg(test)]` with `proptest` (≥100 iterations): an expired sticky TTL always yields a general-deliverable task; a non-committing `StartTxnCommit` always returns the reserved poller and never delivers a token. Tag `// Feature: delivery-broker-simulator, Property 7` and `... Property 3`.
    - _Requirements: 25, 29_

- [x] 8. Verification modes wiring
  - [x] 8.1 Implement the bounded-exhaustive `BrokerActionModel` in `tools/simulation/broker/src/exhaustive.rs`
    - A tiny `Hash+Eq+Clone` model (1 run, 1 queue, 2 workers, ≤1 WFT + ≤1 activity) with `BrokerAction::{Publish, Reserve, Commit, CompleteCurrent, CompleteStale, Crash, LeaseExpire, StickyExpire, PollA, PollB}` implementing `ExhaustiveModel`; `check()` evaluates S1–S7 on each state so protocol-shape bugs surface at shallow depth.
    - _Requirements: 4.1, 4.2, 4.4, 31.1, 31.2, 31.3_
  - [x] 8.2 Wire `main.rs`: CLI, mode dispatch, and reporting
    - Parse args via `sim_engine::cli` (registering the buggy-mode flag); run `run_bounded_exhaustive` unless `--random-only`; run the seeded `StressRunner` over `--seeds` unless `--exhaustive-only`; aggregate into `sim_engine::Report`; print signals + per-invariant PASS/FAIL; exit non-zero on any failure; emit the `placement-sim`-style "buggy mode enabled but nothing failed" warning when applicable.
    - _Requirements: 6.3, 6.5, 30.1, 30.3, 30.4, 31.4, 33.1, 33.2, 33.3, 33.4, 32.1_

- [x] 9. Faults and injectable bug
  - [x] 9.1 Register the required fault set in `tools/simulation/broker/src/faults.rs`
    - Register, each as a named `Fault`: broker crash before backlog write (S5/L1); lease expiry + slow worker + stale old completion (S4/L1); worker crash (L1/S5); reservation-return / poller-went-away (S3); sticky-TTL expiry + promotion (S7); duplicate schedule + duplicate poll (S6/S2); poller storm beyond `max_waiters` (L2); hot-vs-cold partition pressure (L4); cross-partition backlog-with-waiters (L4 + sync-match rate); sustained backlog age (L4 + control loop); `StartTxnCommit` abort (S3/S2). All scheduled via `Rng`; injection counts recorded. (Implemented in `src/workload.rs` fault schedule + `src/events.rs` fault event variants rather than a standalone `faults.rs`; every named fault and its mapped invariant(s) are exercised.)
    - _Requirements: 34.1, 34.3, 34.4_
  - [x] 9.2 Document the fault→invariant map in `tools/simulation/broker/README.md`
    - Write the README mirroring `tools/simulation/placement/README.md`: purpose, the two verification modes, the fault→invariant table, the "what a healthy run looks like" signal/quality guidance, the re-modeling decision + fidelity-risk note (kept faithful to `crates/tokeira-runtime/src/broker.rs`), and the documented out-of-scope limitations (no real DSQL isolation, no network partitions, no multi-cell).
    - _Requirements: 34.2, 38 (design re-modeling note), 39.6, 41.4, 41.5_
  - [x] 9.3 Implement the injectable bug in `tools/simulation/broker/src/bug.rs`
    - `InjectedBug` selectable by the `--bug=<name>` flag with at least: `token-before-commit` (deliver token in `ReserveAndStart` → violates S3, enables S2), `drop-expired-sticky` (drop on `StickyTtlExpire` → violates S7), `no-dedup-on-republish` (skip the `enqueued` check → violates S6/S2). Thread the flag into both the stress model and `BrokerActionModel`.
    - _Requirements: 35.1, 35.2_
  - [x] 9.4 Test: injected bug is caught; clean config passes
    - For each injectable bug, assert `run_bounded_exhaustive` returns a `Counterexample` for the matching safety invariant with a shortest path; with the bug disabled, the same config reports all safety invariants PASS.
    - _Requirements: 35.3, 35.4_

- [x] 10. Broker-sim mode and determinism tests
  - [x] 10.1 Healthy-run and per-fault tests
    - A healthy-run seed (sufficient pollers, bounded faults) passes all S1–S7 and L1–L4; each fault run alone exercises its mapped invariant(s) without false failure; assert the registered fault set matches the README table.
    - _Requirements: 30.2, 34.1, 34.2, 24-33 coverage_
  - [x] 10.2 Determinism test
    - Stress mode run twice with identical flags (same seeds/ops/time/faults) produces byte-identical aggregate reports; the engine determinism property (same seed+model+fault-config → identical event sequence) holds for `BrokerModel`.
    - _Requirements: 30.4, 32.1, 32.2_

- [x] 11. Checkpoint — full simulator
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p sim-engine` and `cargo test -p broker-sim`.
  - Run `cargo run -p broker-sim --release` (default) and `cargo run -p broker-sim --release -- --bug=token-before-commit` to confirm the healthy run passes and the bug is caught.
  - Ensure all tests pass; ask the user if questions arise.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3"] },
    { "id": 2, "tasks": ["1.4", "2.1", "2.2", "2.3", "2.5", "2.6"] },
    { "id": 3, "tasks": ["2.4"] },
    { "id": 4, "tasks": ["2.7"] },
    { "id": 5, "tasks": ["3"] },
    { "id": 6, "tasks": ["4.1"] },
    { "id": 7, "tasks": ["5.1"] },
    { "id": 8, "tasks": ["5.2"] },
    { "id": 9, "tasks": ["5.3", "5.4"] },
    { "id": 10, "tasks": ["6.1"] },
    { "id": 11, "tasks": ["6.2", "6.3", "6.4", "6.5", "6.6"] },
    { "id": 12, "tasks": ["6.7"] },
    { "id": 13, "tasks": ["7.1", "7.2"] },
    { "id": 14, "tasks": ["7.3", "7.4", "8.1"] },
    { "id": 15, "tasks": ["8.2", "9.1", "9.3"] },
    { "id": 16, "tasks": ["9.2", "9.4", "10.1", "10.2"] },
    { "id": 17, "tasks": ["11"] }
  ]
}
```

## Notes

- Deliverable A (engine) is built and validated (tasks 1–4) before Deliverable B (broker model)
  so the abstraction boundary is proven by the trivial second model (4.1) before the broker leans
  on it — this is the cheap proxy for the future admission-control (055) and connection-management
  (060) consumers.
- The engine core uses no `proptest`; the broker-sim crate uses workspace `proptest` only as a
  dev-dependency for focused model-level property tests (7.3, 7.4), complementing the two
  simulation modes.
- The simulator re-models `crates/tokeira-runtime/src/broker.rs` and imports neither
  `tokeira-runtime` nor `tokio`; the README (9.2) records this re-modeling decision and the
  obligation to keep the model faithful as the broker evolves.
- `tools/simulation/placement` was left untouched by this spec; it has since been rebased onto `sim-engine` as a follow-on, tracked separately.
- The injectable bug (9.3) is the broker analog of `placement-sim`'s `--buggy-start-routing`: it
  proves the simulator has real falsifying power, caught by the exhaustive checker at shallow depth.
- No task depends on live AWS, Docker, or the network; both crates run in CI via `cargo test`.
