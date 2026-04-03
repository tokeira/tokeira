# Requirements Document: Timer Scanner

## Introduction

This document captures the requirements for Feature 4 of the Tokeira runtime: Timer Scanner. This feature adds a background task that periodically discovers timers whose deadlines have passed and injects `Command::TimerDue` commands into the owning run's lane mailbox.

Timer scanning is non-authoritative. The scanner reads from the storage layer's `list_due_timers(now, limit)` API, which returns `DueTimer` entries (run_key, timer_id) from the independent timer bucket structure. The authoritative state transition happens when the kernel processes the `TimerDue` command — it emits a `TimerFired` history event, removes the timer from the open set, and schedules a workflow task. If the scanner fires a stale or duplicate `TimerDue` (timer already canceled or fired, run already closed), the kernel rejects it harmlessly via `Reject::UnknownTimer` or `Reject::RunClosed`.

This feature is structurally parallel to the Activity Timeout Scanner from Feature 3. Both are background tasks that periodically scan storage and inject commands through the lane. The key difference is that the timer scanner reads from the storage timer bucket (authoritative timer state managed by the kernel), whereas the activity timeout scanner reads from runtime-local tracking state.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing), which is already implemented.

The authoritative specifications are [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md) and [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md).

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one lane via `hash(run_key) mod lane_count`.
- **Timer_Scanner**: A background task in the Runtime that periodically calls `storage.list_due_timers(now, limit)` to discover timers whose `fire_at` has passed, and injects `Command::TimerDue` for each into the owning run's lane mailbox.
- **DueTimer**: A storage-layer struct containing `run_key` and `timer_id`, returned by `list_due_timers` for timers whose `fire_at` deadline is at or before the current time.
- **TimerDueRequest**: The kernel command payload containing `timer_id` and `fired_at`, submitted via `Command::TimerDue`.
- **Timer_Bucket**: The storage-layer structure that holds outstanding timer obligations keyed by `(run_key, timer_id)`. Managed authoritatively by the kernel via `TimerOp::Upsert` and `TimerOp::Delete` within committed transitions.
- **CancellationToken**: A cooperative shutdown signal (`tokio_util::sync::CancellationToken`) used to gracefully stop the Timer_Scanner background task when the Runtime shuts down.
- **Shard_Ownership**: The mechanism by which a runtime node claims exclusive responsibility for a set of runs. Timer scanning should eventually be scoped to owned shards (deferred to Feature 11: Sweeper and Recovery).

## Requirements

---

### Requirement 1: Background Timer Scanning

**User Story:** As a Tokeira developer, I want a background timer scanner, so that due timers are detected and delivered to their owning runs without external polling.

#### Acceptance Criteria

1. THE Runtime SHALL run a background task (Timer_Scanner) that periodically calls `list_due_timers(now, limit)` on the RunRepository to discover timers whose `fire_at` has passed.
2. WHEN `list_due_timers` returns one or more DueTimer entries, THE Timer_Scanner SHALL submit a `Command::TimerDue(TimerDueRequest { timer_id, fired_at })` for each entry to the owning run's lane via the same `submit` path used by other runtime commands.
3. THE Timer_Scanner SHALL use a configurable scan interval with a default suitable for sub-second timer resolution (e.g. 200ms).
4. THE Timer_Scanner SHALL use a configurable batch limit to bound the number of timers processed per scan cycle.
5. THE Timer_Scanner SHALL set the `fired_at` field in each `TimerDueRequest` to the wall-clock time at which the scan cycle observed the timer as due.

---

### Requirement 2: Timer Scanning Is Not Authoritative

**User Story:** As a Tokeira developer, I want timer scanning to be non-authoritative, so that duplicate or stale timer firings are harmless and the kernel remains the single source of truth.

#### Acceptance Criteria

1. THE Timer_Scanner SHALL NOT modify authoritative state directly; the authoritative transition happens when the Kernel processes the `TimerDue` command and commits the resulting `TimerFired` event and `TimerOp::Delete`.
2. WHEN a `TimerDue` command is delivered for a timer that has already been canceled or fired, THE Kernel SHALL reject it with `Reject::UnknownTimer`, and THE Runtime SHALL treat that rejection as a harmless no-op.
3. WHEN a `TimerDue` command is delivered for a run that is already closed, THE Kernel SHALL reject it with `Reject::RunClosed`, and THE Runtime SHALL treat that rejection as a harmless no-op.
4. WHEN a `TimerDue` command is delivered for a run that does not exist, THE Kernel SHALL reject it with `Reject::MissingRun`, and THE Runtime SHALL treat that rejection as a harmless no-op.

---

### Requirement 3: Timer Scanner Configuration

**User Story:** As a Tokeira developer, I want the timer scanner to be configurable, so that operators can tune scan frequency and batch size for their workload.

#### Acceptance Criteria

1. THE Timer_Scanner SHALL accept a configuration struct with at least `scan_interval` (duration between scan cycles) and `max_timers_per_scan` (maximum DueTimer entries processed per cycle).
2. THE `scan_interval` default SHALL be suitable for sub-second timer resolution (e.g. 200 milliseconds).
3. THE `max_timers_per_scan` default SHALL be a reasonable batch bound (e.g. 100).
4. THE Timer_Scanner SHALL respect the configured `max_timers_per_scan` by passing it as the `limit` parameter to `list_due_timers`.

---

### Requirement 4: Timer Scanner Lifecycle

**User Story:** As a Tokeira developer, I want the timer scanner to start when the runtime is created and stop when explicitly shut down, so that timer detection is active only while the runtime is serving.

#### Acceptance Criteria

1. WHEN the Runtime is created, THE Runtime SHALL spawn the Timer_Scanner as a background `tokio::spawn` task.
2. THE Runtime SHALL expose a cooperative `shutdown_timer_scanner` method that cancels the Timer_Scanner via a CancellationToken and awaits its completion. Shutdown is not automatic on drop — callers must invoke this method explicitly. A broader runtime lifecycle abstraction (e.g., a unified `shutdown` method covering all background tasks) is a future concern.
3. THE Timer_Scanner SHALL check the CancellationToken before each scan cycle and exit gracefully when cancellation is signaled.

---

### Requirement 5: Timer Scanner Error Resilience

**User Story:** As a Tokeira developer, I want the timer scanner to be resilient to transient errors, so that temporary storage failures do not crash the scanner or leave timers undelivered.

#### Acceptance Criteria

1. IF `list_due_timers` returns a transient error, THEN THE Timer_Scanner SHALL log the error at warn level and continue to the next scan cycle rather than crashing.
2. IF `submit` returns an error for a specific DueTimer (lane channel closed, OCC exhaustion), THEN THE Timer_Scanner SHALL log the error at warn level and continue processing remaining DueTimer entries in the current batch.
3. IF `submit` returns a kernel rejection (`Reject::UnknownTimer`, `Reject::RunClosed`, `Reject::MissingRun`), THEN THE Timer_Scanner SHALL log the rejection at debug level and continue processing remaining entries.

---

### Requirement 6: Timer Scanner Distributed Coordination (Deferred)

**User Story:** As a Tokeira developer, I want timer scanning to be scoped to owned shards in the future, so that multiple runtime nodes do not duplicate timer work.

#### Acceptance Criteria

1. THE Timer_Scanner SHALL be designed to support shard-scoped scanning, where only timer buckets for shards owned by the current runtime node are scanned.
2. WHEN shard ownership changes, THE Timer_Scanner SHALL stop scanning timers for relinquished shards and begin scanning for newly acquired shards.
3. WHILE shard ownership is not yet implemented (Feature 11), THE Timer_Scanner SHALL scan all timers regardless of shard assignment. This is safe because timer scanning is non-authoritative — duplicate `TimerDue` commands from multiple nodes are rejected harmlessly by the kernel.

