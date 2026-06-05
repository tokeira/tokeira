# Requirements Document

Delivery Broker Simulator and Shared Simulation Engine

## Introduction

Tokeira has exactly one discrete-event simulator today: `tools/simulation/placement`, which falsifies the safety invariants of the placement/membership design ([035-placement-and-membership](../../../docs/architecture/035-placement-and-membership.md)). It is a single self-contained binary that injects adversarial faults — stale routing, concurrent OCC commits, lease expiry, drain races — and checks that six named invariants still hold after every event. This spec extends that single tool into the first two members of a **family of service simulators** that build confidence in Tokeira's design and implementation choices.

This spec has **two deliverables**:

- **Deliverable A — Shared simulation engine.** The reusable mechanics that `placement-sim` invented inline (a deterministic seeded event-queue + RNG core, a per-event invariant-check loop, a named-invariant registry with PASS/FAIL aggregation, a fault-injection framework, a bounded-exhaustive state-space enumerator, aggregate reporting, and CLI scaffolding) are extracted into a reusable library. The engine is general enough to serve the future admission-control ([055-admission-control](../../../docs/architecture/055-admission-control.md)) and connection-management ([060-connection-management](../../../docs/architecture/060-connection-management.md)) simulators **without modification**, so later simulators are cheap to build and keep the invariant discipline uniform across the family.

- **Deliverable B — Delivery-broker simulator.** The first consumer of the engine. It models the real Tokeira delivery broker ([040-delivery-broker](../../../docs/architecture/040-delivery-broker.md), implemented in `crates/tokeira-runtime/src/broker.rs`) as a pure deterministic state machine and falsifies the broker's central correctness claim under adversarial schedules.

The delivery broker is built first because it is the highest-scale hot path, is actively being built, and its core correctness claim is safety-shaped and worth falsifying.

### The central correctness claim

Doc 040 ("Why this is safe" + "Sweeper contract") states that authoritative pending-task state lives **with the run** (`workflow_hot.pending_wft`, `activity_state`), never in any broker or queue row. If the broker process dies before durable backlog is written, a sweeper reconstructs delivery candidates from authoritative state. The broker is therefore a **delivery optimiser, not an authority**: it may duplicate, delay, expire, or redeliver tasks, but it must never make workflow state true by itself. The simulator exists to falsify that claim under adversarial scheduling.

### What this spec is NOT

This is a simulator plus an engine for design and implementation confidence. It is not the broker implementation (that is `crates/tokeira-runtime/src/broker.rs`). The simulator **re-models** broker semantics as a pure state machine; it does not import the async broker. This is the same modeling-vs-importing choice `placement-sim` made for DSQL and the runtime.

### Sources of truth

The model is grounded in three authoritative sources, cited by repo-relative path throughout this spec:

- **`docs/architecture/040-delivery-broker.md`** — the design contract: three-tier delivery, reservation-based sync matching, sticky-first/not-sticky-only, fairness-belongs-to-backlog, sweeper contract, long-polls-stay-out-of-storage.
- **`crates/tokeira-runtime/src/broker.rs`** — the actual implementation: `sticky_ready`/`general_ready` tiers per `QueueKey`, dedup keys `(RunKey, LogicalTaskSeq)` and `(RunKey, activity_id, attempt)`, `ReservedPoller` with `deliver()`/`return_reserved_poller`, the grace-scanner spill to durable backlog, the `denied_workers` set, and memory-only pollers.
- **`tools/simulation/placement/`** (binary + `README.md`) — the established simulator **pattern** this work generalises: two verification modes, named invariants checked after every event, a fault→invariant table, an injectable known bug caught by the exhaustive checker, and an aggregate report describing what a healthy run looks like.

This spec deliberately does NOT cite Temporal v1.31.0 server source: this is internal Tokeira design validation, not public-API-conformance behaviour. It is unrelated to the worker-deployments / api-conformance work.

### Three-tier model mapping (doc 040 ⇄ broker.rs)

The requirements use this mapping explicitly so the model is unambiguous:

- **Tier A — inline start** = a synchronous `ReservedPoller` match at publish time: a compatible poller is already waiting when the task is created, so the start transaction runs in one logical flow and the token is returned to that poller.
- **Tier B — live-ready** = the in-memory `sticky_ready` / `general_ready` queues during the short grace window, before durable backlog is written.
- **Tier C — durable backlog** = the grace-scanner spill to storage for tasks that outlive the grace window or occur under pressure / shard unload.

## Glossary

- **Engine**: The reusable simulation library (Deliverable A). Provides the deterministic event-queue + seeded RNG core, the invariant registry and per-event check hook, the fault-injection framework, the bounded-exhaustive enumerator, aggregate reporting, and CLI scaffolding. Contains no broker-specific or placement-specific logic.
- **Broker_Model**: The pure deterministic re-model of Tokeira's delivery broker semantics (Deliverable B), built on the Engine. Models tiers, sticky promotion, dedup, reservations, the grace scanner, denied workers, the authoritative per-run pending state, and the sweeper. Does NOT import `tokeira-runtime`.
- **Simulator**: The delivery-broker tool as a whole — the Broker_Model driven by the Engine in either verification mode.
- **Stress_Mode**: The seeded stress verification mode. Randomised discrete-event simulation over configurable seeds, op counts, and a simulated time range. Deterministic and reproducible per seed.
- **Exhaustive_Mode**: The bounded-exhaustive verification mode. Enumerates all reachable interleavings up to a configurable depth over a tiny model, closer to model checking than simulation.
- **Event_Queue**: The deterministic priority queue of scheduled events ordered by simulated timestamp (and a deterministic tie-breaker), driving the state machine forward. No real wall-clock time.
- **Seeded_RNG**: The deterministic pseudo-random number generator seeded per run, used for all randomised choices so that one seed produces exactly one event sequence.
- **Invariant**: A named correctness property registered with the Engine and evaluated against model state. Classified as safety (must hold under all adversarial schedules) or liveness (holds under healthy / bounded-adversary conditions).
- **Invariant_Registry**: The Engine component holding registered invariants by name, evaluating them via the per-event check hook, and aggregating PASS/FAIL per invariant across a run.
- **Falsification_Condition**: The measurable predicate whose truth means a given invariant has been violated. Each invariant requirement states one.
- **Fault_Injector**: The Engine framework that introduces adversarial events (crashes, expiries, races, duplicates, storms) according to the active fault configuration.
- **Reporter**: The Engine component that aggregates signal counts and per-invariant PASS/FAIL across seeds into a single report.
- **Delivery_Broker**: The Tokeira subsystem that handles worker polling, sync matching, sticky routing, and durable backlog without being a source of truth (doc 040). The thing being modelled.
- **QueueKey**: The broker's keying unit for ready tasks and waiters (`crates/tokeira-runtime/src/broker.rs`). The simulator's queue-family identity for delivery decisions.
- **Tier_A_Inline**: Synchronous `ReservedPoller` match at publish time (see mapping above).
- **Tier_B_Live_Ready**: The in-memory `sticky_ready` / `general_ready` queues during the grace window.
- **Tier_C_Durable_Backlog**: The grace-scanner spill to durable storage.
- **Sticky_Tier**: The `sticky_ready` queue for tasks whose run has a preferred worker; only the matching worker may take a sticky task.
- **Sticky_Promotion**: Moving a sticky task to the general tier when its sticky TTL expires before the preferred worker polls, making it general-deliverable to any poller.
- **Sweeper**: The recovery mechanism that, after broker restart or shard failover, reconstructs delivery candidates (pending WFTs, dispatchable activity attempts, expired sticky claims) from authoritative per-run state and republishes them (doc 040 "Sweeper contract").
- **Query_Task**: A read-only task delivered without deduplication and without backlog participation, preferring a matching sticky worker (`crates/tokeira-runtime/src/broker.rs` `publish_query_task` / `poll_query_task`).
- **Direct_Claim**: Pulling a specific run's task out of the general tier by RunKey for eager/direct dispatch rather than the normal poll path (`try_claim_workflow_task`); never claims from the sticky tier.
- **Queue_Partition**: One partition of a logically-named task queue. Each partition has its own waiters and ready tiers; backlog on one partition while pollers wait on another is the doc-040 sync-match-collapse condition.
- **Sync_Match_Rate**: The fraction of published tasks that found a waiting poller at publish time (the broker emits `record_sync_match` / `record_non_sync_match`); a primary doc-040 health indicator.
- **Poll_Success_Rate**: The fraction of resolved polls that received work rather than timing out; a doc-040 health indicator.
- **Schedule_To_Start**: The simulated-time latency from task publish to successful start; a doc-040 health indicator.
- **Control_Loop**: The broker's weighted-budget policy that shifts service share across sticky, live-ready, and backlog offers by backlog age (doc 040 "Suggested broker control loop"); a delivery-shaping effect that carries no correctness weight.
- **ReservedPoller**: A parked poll pulled off the wait queue for a synchronous (inline) match. If the producer cannot deliver, it must hand the reservation back (`return_reserved_poller`); `deliver()` returns false if the poller already went away so the caller re-routes rather than loses the task.
- **Reservation**: The claim a `ReservedPoller` represents — a brokered hold that becomes a real delivery only after the authoritative start transaction commits.
- **Start_Task_Transaction**: The authoritative transaction that appends `WorkflowTaskStarted` / `ActivityTaskStarted` and makes a delivery real. The broker brokers a reservation; the start transaction is what makes it true.
- **Delivery_Lease**: The bounded ownership a worker holds over a delivered task between start and completion. On expiry the task may be redelivered, and a later completion under the old lease is stale.
- **Logical_Task_Identity**: The dedup key for a task: `(RunKey, LogicalTaskSeq)` for workflow tasks, `(RunKey, activity_id, attempt)` for activity tasks.
- **Denied_Workers**: The set `(NamespaceId, TaskQueueName, WorkerIdentity)` of workers that may not currently receive a task on a queue (version/build compatibility, shutdown).
- **Authoritative_Pending_State**: The per-run truth (`workflow_hot.pending_wft`, `activity_state`) that the broker is an optimiser over. The model holds this separately from broker queue state.
- **Healthy_Run**: A run configured with sufficient pollers and bounded faults under which liveness invariants are expected to hold, used to validate the "what a healthy run looks like" signal counts.
- **Injectable_Bug**: A deliberately incorrect Broker_Model variant, selectable by CLI flag, that violates a named safety invariant so the verification modes can demonstrate real falsifying power.

## Requirements

---

## Deliverable A: Shared Simulation Engine

The Engine is the reusable substrate. It MUST contain no broker-specific or placement-specific logic so that the future admission-control (055) and connection-management (060) simulators can consume it unchanged.

### Requirement 1: Deterministic Event-Queue and Seeded RNG Core

**User Story:** As a simulator author, I want a deterministic event-queue and seeded RNG core, so that every simulation run is reproducible from a single seed and failures can be replayed exactly.

#### Acceptance Criteria

1. THE Engine SHALL provide an Event_Queue that orders scheduled events by simulated timestamp using a deterministic tie-breaker for events sharing a timestamp.
2. THE Engine SHALL advance simulated time only by draining the Event_Queue, without reading any wall-clock source.
3. THE Engine SHALL provide a Seeded_RNG initialised from a single seed value supplied at run start.
4. WHEN two runs are executed with the same seed, the same initial model, and the same fault configuration, THE Engine SHALL produce an identical ordered sequence of events.
5. WHERE a model schedules a future event, THE Engine SHALL accept a non-negative simulated-time delay and enqueue the event at the current simulated time plus that delay.
6. THE Engine SHALL expose the current simulated timestamp to the model during event handling.
7. THE Engine SHALL be free of `tokio`, real-time clocks, and real I/O in its event-driving core.

### Requirement 2: Generic Per-Event Invariant-Check Hook and Named-Invariant Registry

**User Story:** As a simulator author, I want a named-invariant registry with a per-event check hook, so that correctness properties are evaluated uniformly after every state change and reported per name.

#### Acceptance Criteria

1. THE Invariant_Registry SHALL accept invariants registered under a unique string name together with a classification of safety or liveness.
2. WHEN an event has been applied to the model, THE Engine SHALL evaluate every registered invariant whose evaluation conditions are met against the resulting model state.
3. IF a registered invariant's Falsification_Condition holds after an event, THEN THE Engine SHALL record that invariant as FAILED for the current run and retain the violating context for reporting.
4. THE Invariant_Registry SHALL aggregate, per invariant name, a PASS or FAIL result across all events in a run.
5. WHERE an invariant is classified as liveness, THE Engine SHALL allow that invariant to be evaluated at run completion or at a model-signalled quiescent point rather than after every event.
6. THE Invariant_Registry SHALL be parameterised over the model type so that any consuming simulator supplies its own state and its own invariants.

### Requirement 3: Fault-Injection Framework

**User Story:** As a simulator author, I want a reusable fault-injection framework, so that each simulator can declare its own adversarial faults and map them to the invariants they stress.

#### Acceptance Criteria

1. THE Fault_Injector SHALL allow a consuming simulator to register named faults that produce adversarial events.
2. WHILE Stress_Mode is active, THE Fault_Injector SHALL select and schedule faults using only the Seeded_RNG so that fault timing is reproducible per seed.
3. THE Fault_Injector SHALL accept a fault configuration that enables or disables individual faults for a run.
4. THE Engine SHALL record, per run, a count of how many times each named fault was injected.
5. THE Fault_Injector SHALL be parameterised over the model type so that fault definitions live in the consuming simulator, not in the Engine.

### Requirement 4: Bounded-Exhaustive State-Space Enumerator

**User Story:** As a simulator author, I want a bounded-exhaustive enumerator parameterised over a small model, so that protocol-shape bugs that random scheduling misses are caught by exhaustive interleaving exploration.

#### Acceptance Criteria

1. THE Engine SHALL provide an enumerator that explores reachable model states by applying every applicable transition at each step.
2. THE enumerator SHALL accept a configurable maximum depth and SHALL NOT expand states beyond that depth.
3. WHEN a registered safety invariant's Falsification_Condition holds at any enumerated state, THE enumerator SHALL report the violating state together with the shortest transition path that reaches it.
4. THE enumerator SHALL be parameterised over the model type, its transition set, and its invariants so that it carries no broker-specific or placement-specific logic.
5. WHERE two distinct transition orderings reach an equivalent model state, THE enumerator SHALL treat the state as already visited to bound exploration.

### Requirement 5: Aggregate Reporting

**User Story:** As a simulator author, I want reusable aggregate reporting, so that every simulator in the family presents signal counts and per-invariant PASS/FAIL the same way.

#### Acceptance Criteria

1. THE Reporter SHALL aggregate model-supplied named signal counters summed across all seeds in a run.
2. THE Reporter SHALL present a PASS or FAIL line per registered invariant name across the whole run.
3. IF any safety invariant is recorded as FAILED in any seed, THEN THE Reporter SHALL report the overall run as FAILED.
4. WHEN a seed produces an invariant failure, THE Reporter SHALL include the failing seed and the violating context in the report.
5. THE Reporter SHALL accept model-defined signal names so the Engine holds no broker-specific or placement-specific counter definitions.

### Requirement 6: CLI Scaffolding

**User Story:** As a simulator operator, I want reusable CLI scaffolding with a vocabulary matching `placement-sim`, so that every simulator in the family is invoked and tuned consistently.

#### Acceptance Criteria

1. THE Engine SHALL provide CLI scaffolding that parses, at minimum, the flags `--seeds`, `--ops`, `--time-ms`, `--verbose`, `--exhaustive-depth`, `--random-only`, and `--exhaustive-only`, matching the `placement-sim` vocabulary where sensible.
2. THE CLI scaffolding SHALL allow a consuming simulator to register additional simulator-specific flags, including a buggy-mode flag.
3. WHEN `--seeds N` is provided, THE Engine SHALL run Stress_Mode over N distinct deterministic seeds.
4. WHERE `--verbose` is set, THE Engine SHALL emit a per-event trace for the run.
5. WHERE `--random-only` is set, THE Engine SHALL run Stress_Mode and skip Exhaustive_Mode; WHERE `--exhaustive-only` is set, THE Engine SHALL run Exhaustive_Mode and skip Stress_Mode.
6. THE Engine SHALL NOT require `proptest` or any workspace property-testing dependency, supplying its own RNG and enumerator as `placement-sim` does.

### Requirement 7: Engine Reusability Boundary

**User Story:** As the owner of the simulator family, I want the Engine abstraction boundary validated against the next two consumers, so that admission-control (055) and connection-management (060) simulators can be built on it without modification.

#### Acceptance Criteria

1. THE Engine SHALL expose its event-queue core, invariant registry, fault framework, enumerator, reporter, and CLI scaffolding as a library API consumable by a separate simulator crate.
2. THE Engine SHALL NOT name, import, or depend on any delivery-broker, placement, admission-control, or connection-management type.
3. THE delivery-broker Simulator SHALL be the first consumer of the Engine and SHALL depend on the Engine as a library.
4. THE Engine API SHALL be general enough that the admission-control (055) and connection-management (060) simulators are realisable as additional consumers without changing the Engine library.

---

## Deliverable B: Delivery-Broker Model

This deliverable re-models the real Tokeira delivery broker as a pure deterministic state machine on top of the Engine. The model is grounded in `docs/architecture/040-delivery-broker.md` and `crates/tokeira-runtime/src/broker.rs`.

### Requirement 8: Pure Deterministic Re-Model of Broker Semantics

**User Story:** As a simulator author, I want the broker modelled as a pure deterministic state machine, so that the simulator can falsify broker semantics deterministically without importing the async production broker.

#### Acceptance Criteria

1. THE Broker_Model SHALL implement broker behaviour as a pure deterministic state machine driven by the Engine Event_Queue, with no `tokio`, no real time, and no real I/O.
2. THE Broker_Model SHALL re-model broker semantics rather than import `crates/tokeira-runtime/src/broker.rs`, mirroring the modeling choice `placement-sim` made for DSQL and the runtime.
3. THE Broker_Model SHALL key ready tasks and waiters by QueueKey, consistent with `crates/tokeira-runtime/src/broker.rs`.
4. THE Broker_Model SHALL hold Authoritative_Pending_State per run separately from broker queue state, so that broker state can be discarded without losing authoritative pending tasks.
5. WHEN the same seed, initial model, and fault configuration are supplied, THE Broker_Model SHALL produce an identical event sequence and identical final results.

### Requirement 9: Three-Tier Delivery Model

**User Story:** As a simulator author, I want the three delivery tiers modelled with the doc-040-to-code mapping, so that delivery-path decisions are exercised and reported unambiguously.

#### Acceptance Criteria

1. WHEN a task is published and a compatible poller is already waiting, THE Broker_Model SHALL deliver it via Tier_A_Inline by reserving that poller and running a Start_Task_Transaction.
2. WHEN a task is published and no compatible poller is waiting, THE Broker_Model SHALL place it in Tier_B_Live_Ready in either the Sticky_Tier or the general tier according to whether the run has a preferred worker.
3. WHILE a task remains in Tier_B_Live_Ready past the grace window, THE Broker_Model SHALL spill it to Tier_C_Durable_Backlog via the grace scanner.
4. THE Broker_Model SHALL record a named signal count for each of: Tier_A_Inline matches, Tier_B_Live_Ready hits, and Tier_C_Durable_Backlog spills.
5. WHERE a task is drained back from Tier_C_Durable_Backlog, THE Broker_Model SHALL make it deliverable again through the live tiers.

### Requirement 10: Reservation-Based Synchronous Matching

**User Story:** As a simulator author, I want reservation-based sync matching modelled per doc 040, so that the reservation-to-commit coupling and the poller-went-away race can be falsified.

#### Acceptance Criteria

1. WHEN the Broker_Model performs a synchronous match, THE Broker_Model SHALL pull a waiting poller off the wait queue as a ReservedPoller before attempting delivery.
2. THE Broker_Model SHALL deliver a token to a ReservedPoller only after the corresponding Start_Task_Transaction commits.
3. IF the Start_Task_Transaction does not commit, THEN THE Broker_Model SHALL return the ReservedPoller to the wait queue rather than stranding it or dropping the task.
4. IF the reserved poller has gone away when delivery is attempted, THEN delivery SHALL report failure and THE Broker_Model SHALL re-route the task instead of losing it.
5. THE Broker_Model SHALL record named signal counts for reservation returns and for reserved-poller-went-away re-routes.

### Requirement 11: Sticky Tier and Promotion

**User Story:** As a simulator author, I want the sticky tier and TTL promotion modelled, so that "sticky-first, not sticky-only" and "sticky expiration is a hint, not a permanent claim" can be falsified.

#### Acceptance Criteria

1. WHEN a published task's run has a preferred worker, THE Broker_Model SHALL place the task in the Sticky_Tier with a sticky TTL.
2. WHILE a task is in the Sticky_Tier, THE Broker_Model SHALL allow only the matching preferred worker to take it.
3. IF a sticky TTL expires before the preferred worker polls, THEN THE Broker_Model SHALL promote the task to the general tier where any compatible poller may take it.
4. THE Broker_Model SHALL apply delivery preference in the order sticky-exact-match, then general live waiter, then live-ready, then backlog, consistent with doc 040.
5. THE Broker_Model SHALL record a named signal count for sticky promotions.

### Requirement 12: Deduplication by Logical Task Identity

**User Story:** As a simulator author, I want deduplication by logical task identity modelled, so that duplicate publications from scanner sweeps and retry paths can be replayed safely without phantom work.

#### Acceptance Criteria

1. THE Broker_Model SHALL key workflow tasks by `(RunKey, LogicalTaskSeq)` and activity tasks by `(RunKey, activity_id, attempt)` for deduplication, consistent with `crates/tokeira-runtime/src/broker.rs`.
2. WHEN a task is published whose Logical_Task_Identity is already enqueued, THE Broker_Model SHALL suppress the duplicate without creating an additional ready or backlog entry.
3. WHEN a task leaves the broker to durable backlog via the grace scanner, THE Broker_Model SHALL clear its dedup key so the same logical task can be re-published later.
4. THE Broker_Model SHALL record a named signal count for suppressed duplicate publications.

### Requirement 13: Grace Scanner and Durable Backlog Spill

**User Story:** As a simulator author, I want the grace-scanner spill to durable backlog modelled, so that Tier C behaviour and the re-publish path are exercised.

#### Acceptance Criteria

1. WHILE a live-ready task's age exceeds the configured grace window, THE Broker_Model SHALL move it to Tier_C_Durable_Backlog and clear its dedup key.
2. WHEN a task is spilled to Tier_C_Durable_Backlog, THE Broker_Model SHALL persist enough identity to redeliver the same logical task later.
3. WHERE the modelled node is under pressure or its shard is being unloaded, THE Broker_Model SHALL allow live-ready tasks to spill to Tier_C_Durable_Backlog before the grace window elapses.
4. THE Broker_Model SHALL record a named signal count for grace-scanner spills.

### Requirement 14: Denied Workers

**User Story:** As a simulator author, I want the denied-workers dimension modelled, so that version/build compatibility changes to who may receive a task are exercised.

#### Acceptance Criteria

1. THE Broker_Model SHALL maintain a Denied_Workers set keyed by `(NamespaceId, TaskQueueName, WorkerIdentity)`.
2. IF a polling worker is in the Denied_Workers set for the polled queue, THEN THE Broker_Model SHALL NOT deliver a task to that worker on that queue.
3. WHEN a worker becomes denied, THE Broker_Model SHALL leave any task it would otherwise have taken deliverable to a non-denied worker.

### Requirement 15: Memory-Only Pollers

**User Story:** As a simulator author, I want pollers modelled as memory-only waiters, so that the "long polls stay out of storage" guarantee can be checked.

#### Acceptance Criteria

1. WHEN a worker long-polls, THE Broker_Model SHALL allocate only an in-memory waiter, a deadline, and a wake budget for that poll.
2. THE Broker_Model SHALL NOT allocate a durable backlog row or a DSQL connection for any long poll.
3. WHEN a long poll's deadline elapses without a match, THE Broker_Model SHALL resolve the poll as a timeout and release its waiter and budget.

### Requirement 16: Sweeper Reconstruction from Authoritative State

**User Story:** As a simulator author, I want the sweeper modelled per doc 040, so that recovery of authoritative pending work after broker loss can be checked.

#### Acceptance Criteria

1. WHEN the broker process is modelled as restarted or its shard fails over, THE Sweeper SHALL scan Authoritative_Pending_State for pending WFTs, dispatchable activity attempts, and expired sticky claims.
2. THE Sweeper SHALL republish reconstructed delivery candidates to the live tiers or to Tier_C_Durable_Backlog.
3. WHEN reconstructing an expired sticky claim, THE Sweeper SHALL make the task general-deliverable rather than re-binding it to the lost preferred worker.
4. THE Broker_Model SHALL record a named signal count for sweeper rebuilds.

### Requirement 17: Workflow-Task vs Activity-Task Separate Modelling

**User Story:** As a simulator author, I want workflow tasks and activity tasks modelled as separate brokers with distinct sensitivities, so that the doc-040 claim that the two tune separately can be exercised rather than assumed.

#### Acceptance Criteria

1. THE Broker_Model SHALL model the workflow-task broker and the activity-task broker as distinct broker instances, consistent with `InMemoryBroker` and `InMemoryActivityBroker` in `crates/tokeira-runtime/src/broker.rs`.
2. THE Broker_Model SHALL apply the single-in-flight-WFT-per-run constraint to the workflow-task broker only, and SHALL allow multiple concurrently started activity tasks per run on the activity-task broker.
3. THE Broker_Model SHALL model sticky routing and the sweeper's expired-sticky reconstruction on the workflow-task broker, reflecting that sticky execution is a workflow-task cache-locality optimisation (doc 040 "Workflow tasks vs activity tasks").
4. WHEN a worker is denied on a workflow-task queue, THE Broker_Model SHALL NOT let that denial affect activity-task delivery for the same worker, consistent with `denied_workflow_worker_does_not_affect_activity_delivery` in `crates/tokeira-runtime/src/broker.rs`.
5. THE Broker_Model SHALL record delivery signal counts separately for workflow tasks and activity tasks so their behaviour can be compared.

### Requirement 18: Query-Task Delivery Path

**User Story:** As a simulator author, I want the read-only query-task path modelled, so that its bypass of dedup and backlog and its sticky-preference behaviour are exercised alongside the durable paths.

#### Acceptance Criteria

1. THE Broker_Model SHALL model query-task delivery as a path that bypasses Logical_Task_Identity deduplication and never participates in Tier_C_Durable_Backlog, consistent with `query_tasks_bypass_dedup_and_all_deliver` in `crates/tokeira-runtime/src/broker.rs`.
2. WHEN a query task carries a preferred worker, THE Broker_Model SHALL prefer delivery to the matching sticky worker and SHALL keep a non-matching-sticky query queued for the matching worker rather than handing it to any poller, consistent with `query_poll_prefers_matching_sticky_worker`.
3. THE Broker_Model SHALL NOT allocate a durable resource for any query poll, consistent with the long-poll resource-isolation guarantee (Requirement 15).
4. THE Broker_Model SHALL record named signal counts for query deliveries and query poll timeouts.

### Requirement 19: Eager / Direct Claim Path

**User Story:** As a simulator author, I want the eager direct-claim path modelled, so that pulling a task out of the general tier for direct dispatch is exercised without double-starting or stranding it.

#### Acceptance Criteria

1. THE Broker_Model SHALL model a direct-claim operation that removes a specific run's task from the general tier by RunKey and clears its dedup key, consistent with `try_claim_workflow_task` in `crates/tokeira-runtime/src/broker.rs`.
2. THE Broker_Model SHALL NOT permit a direct claim to remove a task from the Sticky_Tier out from under its preferred worker.
3. IF a directly-claimed task is not subsequently started, THEN THE Broker_Model SHALL make it deliverable again rather than losing it.
4. THE Broker_Model SHALL ensure a directly-claimed task cannot also be delivered through the normal poll path, so the direct claim never produces a double-start (cross-checked by invariant `S2`).
5. THE Broker_Model SHALL record a named signal count for direct claims.

### Requirement 20: Partitioned-Queue Modelling and Sync-Match Collapse

**User Story:** As a simulator author, I want partitioned task queues modelled, so that the doc-040 anti-pattern where backlog drives sync-match rate toward zero on partitioned queues can be reproduced and guarded against.

#### Acceptance Criteria

1. THE Broker_Model SHALL allow a logical task queue to be modelled as multiple partitions, each with its own waiters and ready tiers.
2. WHILE a partition carries durable backlog AND compatible pollers are waiting on other partitions of the same logical queue, THE Broker_Model SHALL be able to represent the resulting cross-partition sync-match-collapse condition.
3. THE Broker_Model SHALL record per-logical-queue sync-match rate and backlog age so the collapse condition is observable in the report.
4. WHERE the simulator models the doc-040 broker control policy, THE Broker_Model SHALL be able to demonstrate that the policy keeps fresh sync-matchable work flowing rather than letting partition backlog collapse the sync-match rate (cross-checked by invariant `L4` and the new delivery-quality invariants).

### Requirement 21: Broker Control Loop and Weighted Service Budgets

**User Story:** As a simulator author, I want the broker's weighted-budget control loop modelled, so that the doc-040 policy of shifting service share across sticky, live-ready, and backlog offers by backlog age can be exercised and falsified.

#### Acceptance Criteria

1. THE Broker_Model SHALL model a control loop that allocates weighted service budgets across sticky offers, live-ready offers, and backlog offers, per doc 040 "Suggested broker control loop".
2. WHILE modelled backlog age is low, THE Broker_Model SHALL bias delivery toward sticky and live-ready offers; WHILE backlog age is high, THE Broker_Model SHALL raise the backlog offer share.
3. THE Broker_Model SHALL never let the backlog offer share reach a level that starves fresh sync-matchable work (cross-checked by invariant `L4`).
4. THE Broker_Model SHALL expose the active budget split as named signals so the control loop's behaviour is observable across a run.
5. THE Broker_Model SHALL keep the control loop a derived delivery-shaping effect only, placing no correctness weight on it (correctness rests on Authoritative_Pending_State and the Start_Task_Transaction, not on budget decisions).

### Requirement 22: Delivery-Quality Signals

**User Story:** As a simulator author, I want the broker's delivery-quality signals modelled and measured, so that the doc-040 health indicators (sync-match rate, poll success rate, schedule-to-start latency) can be reported and asserted, mirroring the signals the real broker already emits.

#### Acceptance Criteria

1. THE Broker_Model SHALL compute, per logical queue and aggregated, a sync-match rate defined as the fraction of published tasks that found a waiting poller at publish time, consistent with the `record_sync_match` / `record_non_sync_match` signals in `crates/tokeira-runtime/src/broker.rs`.
2. THE Broker_Model SHALL compute a poll-success rate defined as the fraction of resolved polls that received work rather than timing out.
3. THE Broker_Model SHALL compute a schedule-to-start latency distribution in simulated time from task publish to successful start.
4. THE Broker_Model SHALL expose sticky-tier and general-tier ready depths as named signals, consistent with the `set_queue_depth` emissions in `crates/tokeira-runtime/src/broker.rs`.
5. THE Reporter SHALL include these delivery-quality signals so a Healthy_Run can be characterised by high sync-match rate, high poll-success rate, and bounded schedule-to-start latency, per doc 040's worker-performance heuristics.

---

## Safety Invariants

Safety invariants MUST hold under all adversarial schedules. Each is registered with the Invariant_Registry under its `Sx` name, classified safety, evaluated after every event, and stated with a measurable Falsification_Condition. These mirror `placement-sim`'s I1–I6 discipline.

### Requirement 23: S1 — At Most One In-Flight Workflow Task Per Run

**User Story:** As a Tokeira architect, I want at most one in-flight workflow task per run guaranteed, so that workflow-task processing serialises per run even though activities may run concurrently.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S1` with the Invariant_Registry, classified as safety.
2. WHILE the simulation runs, THE Broker_Model SHALL hold at most one started-and-not-completed workflow task per RunKey.
3. IF two distinct workflow tasks for the same RunKey are simultaneously in the started-and-not-completed state, THEN invariant `S1` SHALL be recorded as FAILED (Falsification_Condition).
4. THE Broker_Model SHALL permit multiple concurrent started activity tasks for the same RunKey without affecting `S1`.

### Requirement 24: S2 — No Double-Start

**User Story:** As a Tokeira architect, I want no double-start of a logical task, so that a single logical task is never started successfully more than once.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S2` with the Invariant_Registry, classified as safety.
2. THE Broker_Model SHALL count successful start events per Logical_Task_Identity, where workflow tasks are keyed by `(RunKey, LogicalTaskSeq)` and activity tasks by `(RunKey, activity_id, attempt)`.
3. IF more than one successful `WorkflowTaskStarted` or `ActivityTaskStarted` is recorded for the same Logical_Task_Identity, THEN invariant `S2` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 25: S3 — Reservation⇄Commit Coupling

**User Story:** As a Tokeira architect, I want reservation and commit coupled, so that a worker receives a token only when the authoritative start transaction committed and reservations are never stranded.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S3` with the Invariant_Registry, classified as safety.
2. THE Broker_Model SHALL deliver a token to a worker only after the corresponding Start_Task_Transaction committed.
3. IF a token is held by a worker for a task whose Start_Task_Transaction did not commit, THEN invariant `S3` SHALL be recorded as FAILED (Falsification_Condition).
4. IF a ReservedPoller is neither delivered to nor returned to the wait queue after its reservation resolves, THEN invariant `S3` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 26: S4 — Stale Completion Rejection

**User Story:** As a Tokeira architect, I want stale completions rejected, so that only the current delivery may complete a task after lease expiry, redelivery, or broker restart.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S4` with the Invariant_Registry, classified as safety.
2. THE Broker_Model SHALL associate each delivery with a delivery/reservation identity and SHALL treat a completion as current only if it carries the identity of the current delivery.
3. IF a completion carrying a non-current delivery/reservation identity mutates Authoritative_Pending_State, THEN invariant `S4` SHALL be recorded as FAILED (Falsification_Condition).
4. WHEN a delivery lease expires, the task is redelivered, or the broker restarts, THE Broker_Model SHALL mark prior-delivery completions as stale.

### Requirement 27: S5 — Broker Restart Is Disposable

**User Story:** As a Tokeira architect, I want broker restart to be disposable, so that losing the in-memory broker marks no durable task complete and loses no authoritative pending task.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S5` with the Invariant_Registry, classified as safety.
2. WHEN the broker process is modelled as restarted, THE Broker_Model SHALL return in-flight in-memory deliveries to a deliverable state.
3. WHEN the broker process is modelled as restarted, THE Broker_Model SHALL treat pre-restart worker completions as stale per `S4`.
4. IF a broker restart marks any durable task complete or drops any task present in Authoritative_Pending_State from the set the Sweeper reconstructs, THEN invariant `S5` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 28: S6 — Duplicate Publication Safety

**User Story:** As a Tokeira architect, I want duplicate publication to be safe, so that scanner sweeps and retry paths can re-publish without creating duplicate durable work.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S6` with the Invariant_Registry, classified as safety.
2. WHEN the same Logical_Task_Identity is published more than once while still enqueued, THE Broker_Model SHALL suppress the duplicate.
3. IF a duplicate publication of an already-enqueued Logical_Task_Identity produces more than one durable backlog entry or more than one ready entry for that identity, THEN invariant `S6` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 29: S7 — Sticky Safety

**User Story:** As a Tokeira architect, I want sticky preference to be safe, so that stickiness never causes a duplicate start and an expired sticky claim never becomes a permanent claim.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `S7` with the Invariant_Registry, classified as safety.
2. IF sticky preference causes a second successful start for a Logical_Task_Identity that already started, THEN invariant `S7` SHALL be recorded as FAILED (Falsification_Condition).
3. IF a sticky claim whose TTL has expired remains takeable only by the original preferred worker and is never promoted to general-deliverable, THEN invariant `S7` SHALL be recorded as FAILED (Falsification_Condition).
4. WHEN a sticky TTL expires, THE Broker_Model SHALL make the task general-deliverable via Sticky_Promotion.

---

## Liveness / Quality Invariants

Liveness invariants hold under healthy or bounded-adversary conditions (sufficient pollers, healthy workers, bounded faults). Each is registered under its `Lx` name, classified liveness, and may be evaluated at run completion or at a model-signalled quiescent point (per Requirement 2.5). Each states a measurable Falsification_Condition.

### Requirement 30: L1 — Eventual Delivery / No Loss

**User Story:** As a Tokeira architect, I want eventual delivery with no loss, so that with healthy workers every scheduled task — and every authoritative pending task after a broker crash — is eventually delivered and completed.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `L1` with the Invariant_Registry, classified as liveness.
2. WHILE the run is configured as a Healthy_Run with sufficient pollers, THE Broker_Model SHALL eventually deliver and complete every scheduled task within the simulated time bound.
3. WHEN a broker crash occurs in a Healthy_Run, THE Broker_Model SHALL eventually deliver and complete every task present in Authoritative_Pending_State via Sweeper reconstruction.
4. IF, at the run's quiescent point in a Healthy_Run, a scheduled or authoritative-pending task is neither completed nor deliverable, THEN invariant `L1` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 31: L2 — Bounded Poller Memory

**User Story:** As a Tokeira architect, I want poller memory bounded, so that poller storms cannot create unbounded waiting pollers and no durable task is lost as a result.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `L2` with the Invariant_Registry, classified as liveness.
2. THE Broker_Model SHALL bound the number of concurrently waiting pollers per queue at a configured maximum.
3. WHEN a poll would exceed the maximum waiting pollers, THE Broker_Model SHALL reject the excess poll and record it in a named rejection count.
4. IF the count of concurrently waiting pollers exceeds the configured maximum, THEN invariant `L2` SHALL be recorded as FAILED (Falsification_Condition).
5. IF rejecting an excess poll causes a durable task to be lost, THEN invariant `L2` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 32: L3 — Long Polls Resolve Cleanly

**User Story:** As a Tokeira architect, I want long polls to resolve cleanly, so that a poll either receives work or times out and releases its budget without allocating a durable resource.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `L3` with the Invariant_Registry, classified as liveness.
2. WHEN a long poll resolves, THE Broker_Model SHALL resolve it as either a work delivery or a timeout, and SHALL release the poll's waiter and budget on resolution.
3. IF any long poll allocates a DSQL connection or a durable row at any point in its lifetime, THEN invariant `L3` SHALL be recorded as FAILED (Falsification_Condition).
4. IF a long poll neither receives work nor times out by its deadline, THEN invariant `L3` SHALL be recorded as FAILED (Falsification_Condition).

### Requirement 33: L4 — Backlog Fairness, No Starvation

**User Story:** As a Tokeira architect, I want backlog fairness without starvation, so that fairness applies only on the durable backlog path and fresh sync-matchable work is not blocked by backlog fairness machinery.

#### Acceptance Criteria

1. THE Broker_Model SHALL register invariant `L4` with the Invariant_Registry, classified as liveness.
2. THE Broker_Model SHALL apply fairness and priority only on the Tier_C_Durable_Backlog path, leaving Tier_A_Inline and Tier_B_Live_Ready free of backlog fairness machinery.
3. WHILE multiple Tier_C_Durable_Backlog items share a priority, THE Broker_Model SHALL dispatch them first-in-first-out within that priority band.
4. IF a Tier_C_Durable_Backlog item is never dispatched while items of equal-or-lower priority enqueued after it are dispatched, THEN invariant `L4` SHALL be recorded as FAILED (Falsification_Condition).
5. IF a hot queue or partition permanently prevents any colder queue's backlog from being dispatched, THEN invariant `L4` SHALL be recorded as FAILED (Falsification_Condition).
6. IF backlog fairness machinery blocks delivery of a fresh Tier_A_Inline or Tier_B_Live_Ready matchable task, THEN invariant `L4` SHALL be recorded as FAILED (Falsification_Condition).

---

## Fault Injection

The simulator MUST inject adversarial faults and document a fault→invariant map, mirroring the `placement-sim` README table. The faults below are the minimum required set.

### Requirement 34: Required Fault Set and Fault→Invariant Map

**User Story:** As a simulator operator, I want a documented set of adversarial faults mapped to the invariants they stress, so that I can confirm the simulator is actively trying to falsify each correctness claim.

#### Acceptance Criteria

1. THE Simulator SHALL register and be able to inject each of the following faults, each stressing the named invariants:
   - broker process crash before backlog write — stresses `S5`, `L1`;
   - delivery lease expiry with a slow worker producing redelivery and a stale old completion — stresses `S4`, `L1`;
   - worker crash — stresses `L1`, `S5`;
   - reservation return / poller-went-away race — stresses `S3`;
   - sticky-TTL expiry race and promotion — stresses `S7`;
   - duplicate schedule and duplicate poll — stresses `S6`, `S2`;
   - poller storm beyond max-waiting-pollers — stresses `L2`;
   - hot-partition pressure versus cold partitions — stresses `L4`;
   - cross-partition backlog-with-waiters (backlog on one partition while compatible pollers wait on another partition of the same logical queue) — stresses `L4` and the Sync_Match_Rate signal (Requirement 20);
   - sustained high backlog age driving the control loop's budget split — stresses `L4` and exercises Requirement 21;
   - Start_Task_Transaction commit failure/abort (OCC-style) at reservation time — stresses `S3`, `S2`.
2. THE Simulator SHALL document the fault→invariant map in the simulator README in a table mirroring the `placement-sim` README.
3. WHILE Stress_Mode is active with faults enabled, THE Simulator SHALL select fault timing using only the Seeded_RNG so injected faults are reproducible per seed.
4. THE Simulator SHALL record, per run, a count of how many times each named fault was injected.
5. THE Simulator SHALL name the following faults as future extensions connecting to the admission-control (055) and connection-management (060) simulators, and SHALL NOT require implementing them now: `RuntimeNotShardOwner`, `RoutingSnapshotUpdated`, `QueuePartitionReassigned` (the dynamic-placement-driven move of an in-scope Queue_Partition to a different owner), `ConnectionBudgetReduced`, `DsqlLatencySpike`.

### Requirement 35: Deliberately-Injectable Known Bug

**User Story:** As a simulator operator, I want a deliberately-injectable known bug caught by the exhaustive checker at shallow depth, so that the simulator demonstrates real falsifying power, like `placement-sim`'s `--buggy-start-routing`.

#### Acceptance Criteria

1. THE Simulator SHALL provide at least one Injectable_Bug selectable by a CLI flag.
2. THE Injectable_Bug SHALL be one of: handing the worker a token before the Start_Task_Transaction commits (violates `S3`), dropping an expired sticky claim instead of promoting it to general (violates `S7`), or failing to dedup a re-published logical task (violates `S6`/`S2`).
3. WHEN the Injectable_Bug is enabled, THE Exhaustive_Mode checker SHALL detect the corresponding safety-invariant violation and report the shortest transition path that reaches it.
4. WHEN the Injectable_Bug is disabled, THE Simulator SHALL report all safety invariants as PASS for an otherwise identical configuration.

---

## Verification Modes

The simulator MUST provide two verification modes, mirroring `placement-sim`.

### Requirement 36: Seeded Stress Simulator

**User Story:** As a simulator operator, I want a seeded stress simulator, so that I can exercise the full event space across many deterministic seeds and reproduce any failure.

#### Acceptance Criteria

1. THE Simulator SHALL provide Stress_Mode with configurable seed count, op count, and simulated time range via the `--seeds`, `--ops`, and `--time-ms` flags.
2. WHILE Stress_Mode runs a seed, THE Simulator SHALL evaluate every registered safety invariant after every event.
3. WHEN a seed produces an invariant failure, THE Simulator SHALL report the failing seed so the run can be reproduced.
4. WHEN Stress_Mode is run twice with identical flags, THE Simulator SHALL produce identical results.

### Requirement 37: Bounded-Exhaustive Checker

**User Story:** As a simulator operator, I want a bounded-exhaustive checker over a tiny model, so that protocol-shape bugs random scheduling misses are caught by exhaustive interleaving exploration.

#### Acceptance Criteria

1. THE Simulator SHALL provide Exhaustive_Mode that enumerates reachable interleavings of the Broker_Model up to the depth set by `--exhaustive-depth`.
2. THE Simulator SHALL run Exhaustive_Mode over a tiny fixed model small enough to bound the reachable state space.
3. WHEN a safety invariant is violated at an enumerated state, THE Simulator SHALL report the violating state and the shortest transition path reaching it.
4. WHERE `--exhaustive-only` is set, THE Simulator SHALL run only Exhaustive_Mode; WHERE `--random-only` is set, THE Simulator SHALL run only Stress_Mode.

### Requirement 38: Determinism and Reproducibility

**User Story:** As a simulator operator, I want determinism to be first-class, so that the same seed always yields the same event sequence and every failure is reproducible.

#### Acceptance Criteria

1. WHEN a seed, an initial model, and a fault configuration are fixed, THE Simulator SHALL produce the same ordered event sequence and the same result on every run.
2. THE Simulator SHALL drive all randomness from the Seeded_RNG and all time from the simulated Event_Queue, with no wall-clock or real-I/O dependence.
3. THE Simulator SHALL re-model broker semantics as a pure deterministic state machine rather than importing the async production broker, and the simulator README SHALL state this re-modeling decision and flag the fidelity risk that the model must be kept faithful to `crates/tokeira-runtime/src/broker.rs` as the broker evolves.

---

## Reporting

### Requirement 39: Aggregate Report with Healthy-Run Signals and Per-Invariant PASS/FAIL

**User Story:** As a simulator operator, I want an aggregate report of healthy-run signal counts and a clear PASS/FAIL per invariant, so that I can confirm the simulator is exercising the design and see at a glance whether it holds, mirroring `placement-sim`.

#### Acceptance Criteria

1. THE Simulator SHALL aggregate results across all seeds into a single report.
2. THE report SHALL include, at minimum, named signal counts for: Tier_A_Inline matches, Tier_B_Live_Ready hits, Tier_C_Durable_Backlog spills, redeliveries, reservation returns, reservation aborts, stale completions, poll timeouts, poll rejections, sticky promotions, direct claims, query deliveries, and Sweeper rebuilds.
3. THE report SHALL include the delivery-quality measures from Requirement 22 — Sync_Match_Rate, Poll_Success_Rate, and a Schedule_To_Start distribution — both aggregated and broken down by workflow-task vs activity-task broker, plus the active Control_Loop budget split from Requirement 21.
4. THE report SHALL present a PASS or FAIL line per registered invariant name (`S1`–`S7`, `L1`–`L4`).
5. IF any safety invariant is FAILED in any seed, THEN the report SHALL mark the overall run as FAILED.
6. THE simulator README SHALL describe what a Healthy_Run looks like in terms of the reported signal counts and the delivery-quality measures (high sync-match rate, high poll-success rate, bounded schedule-to-start, non-starving backlog), mirroring `placement-sim`'s "what a healthy run looks like" guidance.

---

## Constraints, Placement, and Scope

### Requirement 40: Placement and Tooling Constraints

**User Story:** As a Tokeira maintainer, I want the simulator and engine to live where `placement-sim` lives and depend on nothing live, so that they fit the established tooling pattern and run in CI without external services.

#### Acceptance Criteria

1. THE Engine and the Simulator SHALL live under `tools/` as standalone tool crates, as `tools/simulation/placement` does.
2. THE Engine and the Simulator crates SHALL set `publish = false`.
3. THE Engine SHALL be a library crate and the delivery-broker Simulator SHALL be a binary crate that consumes the Engine library.
4. THE Engine and the Simulator SHALL NOT depend on live AWS, Docker, or any network service.
5. THE Engine and the Simulator SHALL NOT require `proptest` or any workspace property-testing dependency, supplying their own RNG and enumerator as `placement-sim` does.
6. THE Simulator SHALL be deterministic and reproducible from a seed.

### Requirement 41: Scope and Non-Goals

**User Story:** As a Tokeira maintainer, I want scope and non-goals stated explicitly, so that the simulator is not mistaken for the broker implementation and the engine boundary stays correct.

#### Acceptance Criteria

1. THE Simulator SHALL model broker semantics for design and implementation confidence and SHALL NOT be or replace the broker implementation in `crates/tokeira-runtime/src/broker.rs`.
2. THE Simulator SHALL NOT import the async production broker.
3. THE Engine SHALL be general enough to serve the admission-control (055) and connection-management (060) simulators as future consumers, which are out of scope to implement in this spec.
4. THE Simulator SHALL document, as out-of-scope limitations mirroring `placement-sim`, that it does not model real DSQL transaction-isolation fidelity, network partitions, or multi-cell placement.
5. THE Simulator SHALL NOT cite Temporal v1.31.0 server source, since this is internal Tokeira design validation rather than public-API-conformance behaviour.

---

## Deferred Design-Phase Questions

The following are recorded for resolution in the design phase, not fixed here:

1. **Crate layout.** An external reviewer suggested a `crates/tokeira-simulation/` location; the user's decision is `tools/` (the `placement-sim` sibling location). Within `tools/`, the specific layout — one engine library crate plus one broker binary crate, versus a single crate with an engine module and a broker binary — is a design-phase decision.
2. **Sticky-vs-general internal structure.** Whether the model mirrors `broker.rs`'s separate `sticky_ready` / `general_ready` maps exactly or uses an equivalent unified structure with a sticky flag is a design-phase decision, provided the observable sticky/promotion semantics (Requirement 11) are preserved.
3. **Grace-window and max-waiting-poller defaults.** Concrete default values for the grace window (Requirement 13) and the maximum waiting pollers (Requirement 31) are design-phase decisions.
4. **Partition count and queue-family granularity.** How many partitions per logical queue the tiny exhaustive model and the stress model use (Requirement 20), and how QueueKey granularity maps onto partitions, are design-phase decisions.
5. **Control-loop budget weights and backlog-age bands.** The concrete weighted-budget split across sticky / live-ready / backlog offers and the backlog-age thresholds that shift it (Requirement 21) are design-phase decisions, provided the no-starvation property (`L4`) is preserved.
6. **Healthy-run delivery-quality thresholds.** The concrete sync-match-rate, poll-success-rate, and schedule-to-start bounds that characterise a Healthy_Run for reporting and any liveness assertions (Requirements 22, 39) are design-phase decisions, to be set against doc-040's worker-performance heuristics.
7. **WFT vs AT broker structure.** Whether the model realises the two brokers (Requirement 17) as two instances of one generic broker type or two distinct types is a design-phase decision, provided their distinct constraints (single-in-flight-WFT vs concurrent activities; sticky/sweeper on WFT) are preserved.
