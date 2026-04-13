# Requirements Document: Broker Fairness and Delivery Metrics

## Introduction

This document captures the requirements for broker fairness, delivery-source budgeting, and the supporting delivery metrics infrastructure in the Tokeira runtime (`tokeira-runtime`).

Today the `InMemoryBroker` delivers tasks on a first-come-first-served basis with no weighting between delivery sources (sticky, live-ready, backlog) and no closed-loop feedback from delivery health metrics. The `poll_workflow_task` path contains explicit TODO markers for fairness budgets. This feature fills those gaps.

The broker currently operates a three-tier delivery model:

- **Tier A (Sync Match):** A newly published task is matched immediately with an already-waiting poller. The task never enters durable storage.
- **Tier B (Live-Ready):** The task sits in an in-memory `sticky_ready` or `general_ready` queue for a configurable grace window, waiting for a near-future poller.
- **Tier C (Durable Backlog):** After the grace window expires, the grace scanner persists the task to durable backlog via `persist_to_backlog`. The drain loop later retrieves backlog tasks for queues with waiting pollers.

The guiding architectural principle from [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md) is:

> **Fairness belongs to backlog.** The fast path (sync match, live-ready) should remain simple and cheap. Fairness machinery should apply only on the durable backlog tier, preventing starvation among persisted items without burdening the latency-sensitive sync-match and live-ready paths.

This means:
- The poll path retains its current priority order: sticky → live-ready → backlog. No weighted round-robin on the fast path.
- Backlog drain gets a fair-share budget that the control loop adjusts mechanically from live metrics.
- There are no per-namespace admission caps or operator-facing weight knobs. All tuning is derived from live mechanics, consistent with [015-configuration](../../../docs/architecture/015-configuration.md).

This feature adds backlog fairness budgets, a control loop that adjusts backlog drain share from live metrics, and the metrics infrastructure (schedule-to-start latency, sync match rate, poll success rate, backlog age) that feeds the control loop and operator observability.

The authoritative architecture reference is [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md).

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing), Feature 2 (Activity Pump), and Feature 12 (Durable Backlog Integration).

## Glossary

- **Broker**: The in-memory delivery subsystem (`InMemoryBroker`) that matches pending workflow tasks with waiting pollers. Not authoritative — the sweeper can reconstruct its state from durable storage.
- **Activity_Broker**: The in-memory delivery subsystem (`InMemoryActivityBroker`) that matches pending activity tasks with waiting activity pollers.
- **BrokerState**: The internal state of the `InMemoryBroker`, containing `sticky_ready`, `general_ready`, `enqueued` dedup set, `waiter_counts`, `query_ready`, and `query_waiter_counts`.
- **Sync_Match**: Matching a newly published task with an already-waiting poller at publication time, avoiding durable backlog entirely (Tier A).
- **Live_Ready**: Short-lived in-memory ready structure (Tier B) where tasks wait in `sticky_ready` or `general_ready` for near-future poller matches before falling back to durable backlog.
- **Durable_Backlog**: Persistent task storage (Tier C) used when tasks survive past the live-ready grace window. Managed by the grace scanner and drain loop in `backlog.rs`.
- **Grace_Scanner**: Background task (`scan_grace_once`) that periodically moves expired live-ready tasks to durable backlog via `persist_to_backlog`.
- **Drain_Loop**: Background task (`drain_once`) that periodically retrieves backlog tasks for queues with waiting pollers and republishes them to the broker.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **NamespaceId**: Unique identifier for a namespace, the first component of `QueueKey`.
- **Backlog_Drain_Share**: The fraction of poll responses that may be served from backlog-drained tasks during a control loop interval. Mechanically derived, not operator-configured.
- **Control_Loop**: A periodic background task that reads delivery metrics and adjusts the Backlog_Drain_Share.
- **Schedule_To_Start_Latency**: The elapsed wall-clock time between when a task is scheduled (published to the broker or persisted to backlog) and when the task is started (the start transaction commits).
- **Sync_Match_Rate**: The ratio of tasks matched synchronously at publication time (Tier A) to total tasks published.
- **Poll_Success_Rate**: The ratio of poll requests that return a task to total poll requests (including those that time out empty).
- **Backlog_Age**: The age of the oldest undelivered task in the durable backlog for a given queue.
- **Delivery_Metrics**: The set of metrics consumed by the Control_Loop: Schedule_To_Start_Latency, Sync_Match_Rate, Poll_Success_Rate, and Backlog_Age.
- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.

## Requirements

---

### Requirement 1: Poll Path Preserves Fast-Path Priority Order

**User Story:** As a Tokeira developer, I want the poll path to retain its current priority order (sticky → live-ready → backlog), so that the fast path remains simple and low-latency.

#### Acceptance Criteria

1. WHEN the Broker serves a poll request, THE Broker SHALL attempt delivery in this fixed order: (1) sticky-ready task matching the polling worker, (2) general live-ready task, (3) backlog-drained task.
2. THE Broker SHALL NOT apply weighted round-robin or probabilistic source selection on the poll path. The priority order is deterministic and unconditional.
3. WHEN a sticky-ready or live-ready task is available, THE Broker SHALL deliver it without consulting backlog state or fairness budgets.
4. THE Broker SHALL only serve a backlog-drained task when no sticky-ready or live-ready task is available for the polled QueueKey.
5. THE fast path (sticky and live-ready) SHALL NOT incur any per-poll cost from fairness accounting, backlog age checks, or budget evaluation.

---

### Requirement 2: Backlog Drain Fair-Share Budget

**User Story:** As a Tokeira developer, I want the backlog drain loop to have a fair-share budget, so that backlog delivery does not starve fresh sync-matchable work and old tasks are not starved indefinitely.

#### Acceptance Criteria

1. THE Drain_Loop SHALL maintain a Backlog_Drain_Share per QueueKey that bounds the fraction of poll responses served from backlog-drained tasks during a control loop interval.
2. WHEN the Backlog_Drain_Share for a QueueKey is exhausted in the current interval, THE Drain_Loop SHALL stop draining additional backlog tasks for that QueueKey until the next interval.
3. THE Backlog_Drain_Share SHALL be mechanically derived by the Control_Loop from live Delivery_Metrics. It SHALL NOT be operator-configured.
4. THE Drain_Loop SHALL deliver backlog tasks in FIFO order within the same QueueKey.
5. THE Drain_Loop SHALL NOT allow backlog delivery to drive the Sync_Match_Rate to zero. The Control_Loop SHALL reduce Backlog_Drain_Share when Sync_Match_Rate degrades, ensuring fresh sync-matchable work can still be delivered.
6. THE Drain_Loop SHALL use configurable batch limits per drain cycle, consistent with the existing `BacklogConfig`.

---

### Requirement 3: Backlog-Age-Aware Drain Adjustment

**User Story:** As a Tokeira developer, I want the control loop to increase backlog drain share when backlog age is high, so that old tasks are not starved indefinitely.

#### Acceptance Criteria

1. WHEN the Backlog_Age for a QueueKey is low (below a mechanically-derived threshold), THE Control_Loop SHALL set the Backlog_Drain_Share to a low value, heavily favoring the fast path.
2. WHEN the Backlog_Age for a QueueKey is high (above a mechanically-derived threshold), THE Control_Loop SHALL increase the Backlog_Drain_Share to accelerate backlog delivery.
3. WHILE the Backlog_Drain_Share is increased, THE Control_Loop SHALL preserve a minimum budget for the fast path so that fresh sync-matchable work is not starved.
4. THE Control_Loop SHALL derive the low and high Backlog_Age thresholds from observed Schedule_To_Start_Latency and Poll_Success_Rate, not from static configuration.

---

### Requirement 4: Broker Control Loop

**User Story:** As a Tokeira developer, I want the broker to run a control loop that adjusts backlog drain share from live metrics, so that the system adapts to changing load patterns without operator intervention.

#### Acceptance Criteria

1. THE Runtime SHALL run a background Control_Loop task that periodically evaluates Delivery_Metrics and adjusts the Backlog_Drain_Share per QueueKey.
2. THE Control_Loop SHALL read the following metrics each interval: Schedule_To_Start_Latency (p50 and p99), Sync_Match_Rate, Poll_Success_Rate, and Backlog_Age, broken down by QueueKey.
3. WHEN Schedule_To_Start_Latency increases, THE Control_Loop SHALL increase the Backlog_Drain_Share to accelerate delivery of queued work.
4. WHEN Sync_Match_Rate degrades, THE Control_Loop SHALL reduce the Backlog_Drain_Share to protect the fast path.
5. WHEN Poll_Success_Rate drops, THE Control_Loop SHALL adjust the Backlog_Drain_Share to favor sources with available tasks.
6. THE Control_Loop SHALL run on an adaptive interval that is mechanically derived from observed metric volatility. When metrics are volatile (large deltas between snapshots), the interval SHALL shorten (minimum 2 seconds) to react faster. When metrics are stable (small deltas), the interval SHALL lengthen (maximum 10 seconds) to reduce overhead. The initial interval SHALL be 5 seconds. The interval SHALL NOT be operator-configured.
7. THE Control_Loop SHALL expose the current Backlog_Drain_Share per QueueKey, the input Delivery_Metrics snapshot, and the timestamp of the last adjustment for observability.
8. THE Control_Loop SHALL use a CancellationToken for graceful shutdown, consistent with other background tasks in the runtime (grace scanner, drain loop, timer scanner).
9. THE Control_Loop SHALL bound adjustments per interval to prevent oscillation (e.g., maximum change of 10 percentage points per interval).

---

### Requirement 5: Schedule-to-Start Latency Recording

**User Story:** As a Tokeira developer, I want the runtime to track schedule-to-start latency, so that operators can monitor task delivery health and the control loop can react to delivery delays.

#### Acceptance Criteria

1. WHEN a workflow task is started (the `Command::WorkflowTaskStarted` transition commits successfully), THE Runtime SHALL compute the elapsed time between the task's scheduling timestamp and the current time, and record that duration as the Schedule_To_Start_Latency for the task.
2. WHEN an activity task is started (the activity-task-start transaction commits successfully), THE Runtime SHALL compute the elapsed time between the activity's scheduling timestamp and the current time, and record that duration as the Schedule_To_Start_Latency for the activity task.
3. THE Runtime SHALL maintain Schedule_To_Start_Latency as a histogram, broken down by QueueKey (namespace, task queue, task kind, deployment, build_id).
4. THE Runtime SHALL expose Schedule_To_Start_Latency histogram values (p50, p95, p99) for consumption by the Control_Loop and external observability systems.
5. THE scheduling timestamp for a workflow task SHALL be the wall-clock time when the task was published to the Broker. The broker's internal `TimestampedWorkflowTask::entered_at` captures this, but the current `poll_workflow_task` return type (`DispatchableWorkflowTask`) does not carry it. The broker SHALL be extended to return the `entered_at` alongside the task (e.g., as a tuple or a wrapper type) so the runtime can compute the latency at start time.
6. THE scheduling timestamp for an activity task SHALL be the wall-clock time when the activity task was published to the Activity_Broker.

---

### Requirement 6: Sync Match Rate Tracking

**User Story:** As a Tokeira developer, I want the runtime to track sync match rate, so that operators can diagnose delivery efficiency and the control loop can detect sync-match degradation.

#### Acceptance Criteria

1. WHEN a task is published to the Broker and a compatible poller is already waiting (the task is matched synchronously at publication time), THE Broker SHALL increment a sync-match counter for the task's QueueKey.
2. WHEN a task is published to the Broker and no compatible poller is waiting (the task enters the live-ready tier), THE Broker SHALL increment a non-sync-match counter for the task's QueueKey.
3. THE Runtime SHALL compute Sync_Match_Rate as `sync_match_count / total_publish_count` over a sliding window, broken down by QueueKey.
4. THE Runtime SHALL expose Sync_Match_Rate for consumption by the Control_Loop and external observability systems.
5. THE Activity_Broker SHALL track sync match rate using the same mechanism as the Broker.

---

### Requirement 7: Poll Success Rate Tracking

**User Story:** As a Tokeira developer, I want the runtime to track poll success rate, so that operators can diagnose whether pollers are receiving work efficiently.

#### Acceptance Criteria

1. WHEN a `poll_workflow_task` call returns a task to the poller, THE Runtime SHALL increment a poll-success counter for the polled QueueKey.
2. WHEN a `poll_workflow_task` call times out and returns None, THE Runtime SHALL increment a poll-timeout counter for the polled QueueKey.
3. THE Runtime SHALL compute Poll_Success_Rate as `poll_success_count / total_poll_count` over a sliding window, broken down by QueueKey.
4. THE Runtime SHALL expose Poll_Success_Rate for consumption by the Control_Loop and external observability systems.
5. WHEN a `poll_activity_task` call returns a task, THE Runtime SHALL increment a poll-success counter for the polled QueueKey.
6. WHEN a `poll_activity_task` call times out and returns None, THE Runtime SHALL increment a poll-timeout counter for the polled QueueKey.

---

### Requirement 8: Backlog Age Tracking

**User Story:** As a Tokeira developer, I want the runtime to track backlog age per queue, so that the control loop can detect backlog buildup and operators can monitor backlog health.

#### Acceptance Criteria

1. WHEN the Drain_Loop retrieves tasks from durable backlog, THE Runtime SHALL compute the age of each drained task as `now - entry.scheduled_at`, where `scheduled_at` is the wall-clock timestamp recorded on the `BacklogEntry` when the task was first published to the broker.
2. THE `BacklogEntry` type SHALL carry a `scheduled_at: OffsetDateTime` field that records the wall-clock time when the task was originally published to the broker (the `entered_at` from `TimestampedWorkflowTask` or `TimestampedActivityTask`). This field SHALL be populated by the grace scanner when persisting expired live-ready tasks to backlog, and by the sweeper when reconstructing backlog entries from authoritative state.
2. THE Runtime SHALL maintain the maximum Backlog_Age per QueueKey as a gauge that is updated each drain cycle.
3. WHEN no backlog tasks exist for a QueueKey (the drain returns empty), THE Runtime SHALL set the Backlog_Age gauge for that QueueKey to zero.
4. THE Runtime SHALL expose Backlog_Age per QueueKey for consumption by the Control_Loop and external observability systems.
5. THE Backlog_Age gauge SHALL reflect the age of the oldest undrained task, not the average age.

---

### Requirement 9: Delivery Metrics Snapshot for Observability

**User Story:** As a Tokeira developer, I want the broker to expose a snapshot of current delivery metrics and control loop state, so that operators can inspect the system's delivery health.

#### Acceptance Criteria

1. THE Runtime SHALL expose a method that returns a snapshot of current Delivery_Metrics: Schedule_To_Start_Latency percentiles, Sync_Match_Rate, Poll_Success_Rate, and Backlog_Age, broken down by QueueKey.
2. THE Runtime SHALL expose a method that returns the current Backlog_Drain_Share per QueueKey and the timestamp of the last Control_Loop adjustment.
3. THE exposed metrics SHALL be consistent with the values consumed by the Control_Loop (same data source, no separate collection path).

---

### Requirement 10: Broker Fairness Does Not Affect Correctness

**User Story:** As a Tokeira developer, I want broker fairness to be a purely ephemeral optimization, so that its loss on crash or restart does not affect workflow correctness.

#### Acceptance Criteria

1. THE Broker SHALL NOT persist Backlog_Drain_Share, Delivery_Metrics, or any fairness state to durable storage.
2. WHEN the runtime process restarts, THE Broker SHALL initialize with a default Backlog_Drain_Share and the Control_Loop SHALL converge to appropriate values within a small number of intervals from live Delivery_Metrics.
3. THE Broker fairness state SHALL NOT be required for workflow correctness; all correctness-critical state SHALL remain in committed transitions and durable storage, consistent with Requirement CC.1 from the master spec.

---

### Requirement 11: No Operator-Facing Fairness Configuration

**User Story:** As a Tokeira developer, I want all fairness parameters to be mechanically derived from live metrics, so that operators do not need to tune fairness knobs.

#### Acceptance Criteria

1. THE Runtime SHALL NOT expose per-namespace admission caps, source budget weights, backlog age thresholds, or control loop intervals as operator-configurable parameters.
2. ALL fairness parameters (Backlog_Drain_Share, drain thresholds, control loop interval, oscillation bounds) SHALL be derived from live Delivery_Metrics by the Control_Loop.
3. THE Runtime SHALL accept only the existing `BacklogConfig` for backlog-related settings (batch limits, grace window). Fairness tuning SHALL NOT require additional configuration structures.
4. THE system SHALL operate with reasonable fairness behavior out of the box, without any fairness-specific configuration.
