# Requirements Document: Durable Backlog Integration

## Introduction

This document captures the requirements for Feature 12 (Durable Backlog Integration) of the Tokeira runtime. The durable backlog is Tier C of the three-tier delivery model described in [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md). It provides persistent task storage for unmatched tasks that survive past the live-ready grace window, ensuring they are eventually delivered to workers without relying on in-memory state alone.

Currently, the `InMemoryBroker` (workflow tasks) and `InMemoryActivityBroker` (activity tasks) implement only Tier A (sync match) and Tier B (live-ready). Tasks sit in memory indefinitely with no mechanism to persist unmatched tasks to durable backlog or drain them back. This feature adds:

1. Timestamped entry tracking for live-ready tasks so the broker can determine when the grace window expires.
2. A background scanner that moves expired live-ready tasks to durable backlog via `RunRepository::persist_to_backlog`.
3. A background drain loop that retrieves persisted tasks via `RunRepository::drain_backlog` and matches them with waiting pollers.
4. Deduplication guards to prevent double dispatch of tasks that exist in both backlog and live-ready tiers.
5. Fairness policy: FIFO within a single priority band, with backlog delivery subordinate to fresh sync-matchable work.

The durable fact is that a run has a pending workflow task or an activity has a pending attempt — not that a backlog row exists. If the broker dies before durable backlog is written, the sweeper (Feature 11) reconstructs delivery candidates from authoritative state. Live-ready and backlog are optimizations, not correctness dependencies.

Depends on: Feature 1 (Lane OCC Retry), Feature 2 (Activity Pump), Feature 11 (Sweeper and Recovery).

The authoritative specifications are [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md), [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md), and [090-failover-and-recovery](../../../docs/architecture/090-failover-and-recovery.md).

## Glossary

- **Broker**: The in-memory workflow-task delivery subsystem (`InMemoryBroker`). Implements sticky, general, and live-ready tiers. Not authoritative — the Sweeper reconstructs its state from durable storage.
- **Activity_Broker**: The in-memory activity-task delivery subsystem (`InMemoryActivityBroker`). Implements a single ready tier. Not authoritative — the Sweeper reconstructs its state from durable storage.
- **Live_Ready_Tier**: The in-memory ready structure (Tier B) where tasks wait for near-future poller matches. Tasks enter this tier when published and no sync match is available.
- **Durable_Backlog**: Persistent task storage (Tier C) used when tasks survive past the live-ready grace window. Accessed via `RunRepository::persist_to_backlog` and `RunRepository::drain_backlog`.
- **Grace_Window**: The configurable duration a task may remain in the Live_Ready_Tier before being persisted to Durable_Backlog. Tuned to typical poller arrival latency.
- **Grace_Scanner**: A periodic background task that inspects the Live_Ready_Tier for tasks whose Grace_Window has expired and persists them to Durable_Backlog.
- **Drain_Loop**: A periodic background task that calls `drain_backlog` for queues with waiting pollers and attempts to match drained tasks.
- **BacklogEntry**: The storage-layer struct representing a persisted backlog task. The current shape (`run_key`, `queue`, `kind`, `insertion_seq`) is insufficient to reconstruct a dispatchable task on drain. This feature extends the storage type to carry a `BacklogPayload` enum with variant-specific fields: `Workflow { logical_seq }` or `Activity { activity_id, input, schedule_event_id, attempt }`. See the design document for the authoritative shape.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **Sync_Match**: Tier A delivery — a compatible poller is already waiting when a task is created, so the task is matched immediately without entering the Live_Ready_Tier.
- **Sweeper**: The one-time scan (Feature 11) that reconstructs volatile delivery state from authoritative durable state after shard acquisition. Republishes discovered tasks to the Broker and Activity_Broker.
- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Priority_Band**: A classification level for backlog task ordering. Initial implementation uses a single band (all tasks equal priority). Multi-band priority is deferred to Feature 15.

## Requirements

---

### Requirement 1: Live-Ready Entry Timestamp Tracking

**User Story:** As a Tokeira developer, I want the broker to record when each task enters the live-ready tier, so that the grace scanner can determine which tasks have exceeded the grace window.

#### Acceptance Criteria

1. WHEN a workflow task is published to the Broker and no Sync_Match occurs, THE Broker SHALL record the current timestamp alongside the task in the Live_Ready_Tier (sticky_ready or general_ready).
2. WHEN an activity task is published to the Activity_Broker and no Sync_Match occurs, THE Activity_Broker SHALL record the current timestamp alongside the task in the Live_Ready_Tier.
3. WHEN a sticky workflow task is promoted from the sticky tier to the general tier (due to sticky expiry or non-matching poller), THE Broker SHALL preserve the original entry timestamp from when the task first entered the Live_Ready_Tier.
4. THE entry timestamp SHALL be derived from a monotonic or wall-clock source consistent with the Grace_Window comparison logic.

---

### Requirement 2: Grace Window Configuration

**User Story:** As a Tokeira developer, I want the live-ready grace window to be configurable, so that operators can tune the threshold based on their poller arrival latency characteristics.

#### Acceptance Criteria

1. THE Runtime SHALL expose a configurable Grace_Window duration for the Broker and Activity_Broker.
2. THE Grace_Window SHALL have a default value suitable for typical poller arrival latency (on the order of seconds, not minutes).
3. THE Grace_Window configuration SHALL be settable independently for workflow tasks and activity tasks.
4. WHEN the Grace_Window is set to zero, THE Grace_Scanner SHALL persist tasks to Durable_Backlog on its next scan cycle without waiting.

---

### Requirement 3: Grace Scanner — Persist Expired Live-Ready Tasks

**User Story:** As a Tokeira developer, I want a background scanner to move expired live-ready tasks to durable backlog, so that unmatched tasks are persisted before the broker's in-memory state becomes a liability.

#### Acceptance Criteria

1. THE Runtime SHALL run a Grace_Scanner as a periodic background task that inspects the Live_Ready_Tier of both the Broker and the Activity_Broker.
2. WHEN the Grace_Scanner finds a task whose entry timestamp plus the configured Grace_Window is at or before the current time, THE Grace_Scanner SHALL remove the task from the Live_Ready_Tier.
3. WHEN the Grace_Scanner removes expired tasks from the Live_Ready_Tier, THE Grace_Scanner SHALL construct `BacklogEntry` values and call `RunRepository::persist_to_backlog` to persist them.
4. THE Grace_Scanner SHALL remove expired tasks from the Broker's deduplication set (`enqueued`) when they are moved to Durable_Backlog, so that the Sweeper or Drain_Loop can re-publish them without deduplication suppression.
5. THE Grace_Scanner SHALL batch multiple expired tasks into a single `persist_to_backlog` call when multiple tasks expire in the same scan cycle.
6. THE Grace_Scanner scan interval SHALL be configurable, with a default shorter than the Grace_Window to ensure timely persistence.
7. IF `persist_to_backlog` fails with a transient error, THEN THE Grace_Scanner SHALL retain the expired tasks in the Live_Ready_Tier and retry on the next scan cycle. Tasks are not lost because the Live_Ready_Tier still holds them.

---

### Requirement 4: Drain Loop — Retrieve and Match Backlog Tasks

**User Story:** As a Tokeira developer, I want a background drain loop to retrieve persisted backlog tasks and match them with waiting pollers, so that tasks in durable backlog are eventually delivered.

#### Acceptance Criteria

1. THE Runtime SHALL run a Drain_Loop as a periodic background task.
2. THE Drain_Loop SHALL identify queues that have waiting pollers (registered via long-poll) and call `RunRepository::drain_backlog` for those queues.
3. THE Drain_Loop SHALL NOT call `drain_backlog` for queues that have no waiting pollers, to avoid unnecessary storage reads.
4. WHEN `drain_backlog` returns tasks, THE Drain_Loop SHALL re-publish them to the Broker or Activity_Broker (depending on `BacklogTaskKind`) for matching with waiting pollers.
5. THE Drain_Loop drain interval SHALL be configurable, with a default suitable for balancing delivery latency against storage read frequency.
6. THE Drain_Loop batch limit (the `limit` parameter to `drain_backlog`) SHALL be configurable.
7. IF `drain_backlog` fails with a transient error, THEN THE Drain_Loop SHALL retry on the next cycle. Tasks remain safely persisted in Durable_Backlog.
8. WHEN a drained task is re-published to the Broker or Activity_Broker, THE task SHALL enter the Live_Ready_Tier and follow the normal matching and grace window lifecycle. If no poller matches within the Grace_Window, the task will be persisted to Durable_Backlog again.

---

### Requirement 5: Deduplication Across Tiers

**User Story:** As a Tokeira developer, I want the broker to prevent double dispatch of tasks that may exist in multiple tiers simultaneously, so that a single logical task is delivered to at most one worker.

#### Acceptance Criteria

1. WHEN a task is drained from Durable_Backlog and re-published to the Broker, THE Broker SHALL use the existing deduplication mechanism (`enqueued` set keyed on `(run_key, logical_seq)` for workflow tasks, `(run_key, activity_id, attempt)` for activity tasks) to suppress duplicates.
2. WHEN the Grace_Scanner persists a task to Durable_Backlog and removes it from the Live_Ready_Tier, THE Grace_Scanner SHALL also remove the task's deduplication key from the `enqueued` set, so that a subsequent drain or sweeper re-publish is not suppressed.
3. WHEN the Sweeper (Feature 11) republishes tasks after shard acquisition, THE Broker SHALL accept them through the normal publish path. If a task is already present in the Live_Ready_Tier (from a prior drain), the deduplication set SHALL suppress the duplicate.
4. THE Broker SHALL NOT dispatch the same logical task (same `run_key` and `logical_seq` for workflow, same `run_key`, `activity_id`, and `attempt` for activity) to more than one poller.

---

### Requirement 6: Backlog FIFO Ordering

**User Story:** As a Tokeira developer, I want backlog tasks to be delivered in FIFO order, so that tasks that have waited longest are delivered first.

#### Acceptance Criteria

1. THE Durable_Backlog SHALL maintain insertion order via the monotonic `insertion_seq` field on `BacklogEntry`.
2. WHEN `drain_backlog` returns tasks for a given QueueKey, THE tasks SHALL be ordered by ascending `insertion_seq` (oldest first).
3. THE Drain_Loop SHALL re-publish drained tasks to the Broker or Activity_Broker in the order returned by `drain_backlog`, preserving FIFO delivery semantics.
4. THE initial implementation SHALL use a single Priority_Band (all backlog tasks have equal priority). Multi-band priority ordering is deferred to Feature 15.

---

### Requirement 7: Backlog Delivery Subordinate to Fresh Work

**User Story:** As a Tokeira developer, I want fresh sync-matchable work to take precedence over backlog delivery, so that the fast path is not penalized by backlog fairness machinery.

#### Acceptance Criteria

1. THE Broker SHALL attempt Sync_Match (Tier A) and Live_Ready_Tier match (Tier B) before considering backlog-drained tasks for a given poller.
2. THE Drain_Loop SHALL NOT block or delay Sync_Match or Live_Ready_Tier matching. The Drain_Loop operates asynchronously and re-publishes drained tasks into the Live_Ready_Tier, where they compete with other live-ready tasks on equal footing.
3. WHEN a poller arrives and both fresh live-ready tasks and recently-drained backlog tasks are present in the Live_Ready_Tier, THE Broker SHALL serve them in the order they appear in the ready queue (FIFO within the tier). Drained tasks that were re-published earlier will naturally be ahead of tasks published later.
4. THE Broker SHALL NOT introduce additional fairness or priority logic on the Sync_Match or Live_Ready_Tier paths. Fairness applies only to the ordering within Durable_Backlog.

---

### Requirement 8: Sweeper Interaction with Backlog Lifecycle

**User Story:** As a Tokeira developer, I want tasks republished by the sweeper after shard acquisition to follow the normal grace window and backlog lifecycle, so that the recovery path does not bypass the tiered delivery model.

#### Acceptance Criteria

1. WHEN the Sweeper (Feature 11) republishes workflow or activity tasks to the Broker or Activity_Broker after shard acquisition, THE republished tasks SHALL enter the Live_Ready_Tier with a fresh entry timestamp.
2. WHEN a sweeper-republished task is not matched within the Grace_Window, THE Grace_Scanner SHALL persist it to Durable_Backlog following the same path as any other live-ready task.
3. THE Sweeper SHALL NOT write directly to Durable_Backlog. The Sweeper publishes to the Broker, and the Broker decides when to persist to backlog via the Grace_Scanner.
4. WHEN the Sweeper republishes a task that already exists in Durable_Backlog (from a previous runtime's Grace_Scanner), THE Drain_Loop will eventually drain the stale backlog entry. The Broker's deduplication set SHALL prevent double dispatch if the task has already been matched from the live-ready tier.

---

### Requirement 9: Both Brokers — Workflow and Activity

**User Story:** As a Tokeira developer, I want both the workflow broker and the activity broker to support durable backlog integration, so that all task types benefit from persistent fallback delivery.

#### Acceptance Criteria

1. THE Broker (workflow tasks) SHALL support entry timestamp tracking, grace window expiry, backlog persistence via the Grace_Scanner, and backlog drain via the Drain_Loop.
2. THE Activity_Broker (activity tasks) SHALL support entry timestamp tracking, grace window expiry, backlog persistence via the Grace_Scanner, and backlog drain via the Drain_Loop.
3. THE Grace_Scanner SHALL construct `BacklogEntry` values with `kind: BacklogTaskKind::Workflow` for workflow tasks and `kind: BacklogTaskKind::Activity { activity_id }` for activity tasks.
4. THE Drain_Loop SHALL route drained `BacklogEntry` values to the Broker or Activity_Broker based on the `kind` discriminant.

---

### Requirement 10: Broker Waiter Visibility for Drain Loop

**User Story:** As a Tokeira developer, I want the drain loop to know which queues have waiting pollers, so that it only drains backlog for queues where delivery is possible.

#### Acceptance Criteria

1. THE Broker SHALL expose a method to query which QueueKeys currently have at least one registered waiting poller.
2. THE Activity_Broker SHALL expose a method to query which QueueKeys currently have at least one registered waiting poller.
3. THE Drain_Loop SHALL use these methods to determine the set of queues to drain on each cycle.
4. WHEN no queues have waiting pollers, THE Drain_Loop SHALL skip the drain cycle entirely and wait for the next interval.

---

### Requirement 11: Graceful Shutdown of Background Tasks

**User Story:** As a Tokeira developer, I want the grace scanner and drain loop to shut down cleanly when the runtime stops or a shard is relinquished, so that no in-flight persistence or drain operations are lost.

#### Acceptance Criteria

1. WHEN the Runtime is shutting down, THE Grace_Scanner SHALL complete any in-progress `persist_to_backlog` call before stopping.
2. WHEN the Runtime is shutting down, THE Drain_Loop SHALL complete any in-progress `drain_backlog` call before stopping.
3. WHEN a shard is relinquished (Feature 11, Requirement 15), THE Runtime SHALL stop the Grace_Scanner and Drain_Loop for that shard's scope. Tasks remaining in the Live_Ready_Tier for that shard are not persisted — the sweeper on the new owner will reconstruct them from authoritative state.
4. THE Grace_Scanner and Drain_Loop SHALL respond to a cancellation signal (e.g., `CancellationToken` or `tokio::select!` on a shutdown channel) within one scan/drain interval.
