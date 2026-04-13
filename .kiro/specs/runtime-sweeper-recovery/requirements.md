# Requirements Document: Sweeper and Recovery

## Introduction

This document captures the requirements for Feature 11 (Sweeper and Recovery) of the Tokeira runtime. The sweeper is the mechanism that reconstructs volatile delivery state from authoritative durable state after shard acquisition or runtime restart. It ensures no dispatchable work is lost when the in-memory broker, tracking states, or live-ready structures are discarded.

The feature covers four areas:

1. Post-failover reconstruction of dispatchable workflow tasks, activity tasks, and due timers from authoritative storage.
2. Reconstruction of volatile timeout tracking state (activity, workflow, Nexus) from authoritative storage.
3. Shard lease acquisition, epoch fencing on commits, and periodic lease renewal.
4. The ordered operational recovery sequence that gates command admission on sweep completion.

Additionally, this feature scopes all existing background scanners (timer, workflow timeout, activity timeout, Nexus timeout) to owned shards, replacing the current global-scan behavior.

The authoritative specifications are [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md), [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md), and [090-failover-and-recovery](../../../docs/architecture/090-failover-and-recovery.md).

Depends on: Feature 1 (Lane OCC Retry and Mailbox Coalescing), Feature 2 (Activity Pump), Feature 3 (Activity Heartbeat and Timeouts), Feature 4 (Timer Scanner), Feature 5 (Workflow Timeouts), Feature 9 (Nexus Operation Dispatch).

## Glossary

- **Sweeper**: A one-time scan executed after shard acquisition that discovers all pending dispatchable work from authoritative durable state and republishes it to the in-memory Broker and Activity_Broker.
- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Shard_Epoch**: A monotonically increasing fencing token for shard ownership. Stale owners cannot commit transitions. Defined as `ShardEpoch(u64)` in `tokeira-types`.
- **Shard_Owner**: The runtime-local structure that tracks which shards the current node owns and their current epochs.
- **Lease_Renewer**: A periodic background task that renews the shard lease via `LeaseRepository::renew_bundle` to prevent lease expiry while the node is healthy.
- **Broker**: The in-memory workflow-task delivery subsystem (`InMemoryBroker`). Not authoritative — the Sweeper reconstructs its state from durable storage.
- **Activity_Broker**: The in-memory activity-task delivery subsystem (`InMemoryActivityBroker`). Not authoritative — the Sweeper reconstructs its state from durable storage.
- **Timer_Scanner**: Background task that scans `timer_bucket` for due timers and injects `TimerDue` commands into run actor mailboxes.
- **Workflow_Timeout_Scanner**: Background task that scans `WorkflowTimeoutTrackingState` for runs exceeding their configured execution or run timeout.
- **Activity_Timeout_Scanner**: Background task that scans `ActivityTrackingState` for activities exceeding their configured timeout thresholds.
- **Nexus_Timeout_Scanner**: Background task that scans `NexusTimeoutTrackingState` for Nexus operations exceeding their schedule-to-close timeout.
- **ActivityTrackingState**: Volatile in-memory state tracking started activities and their timeout-relevant timestamps. Must be reconstructed after restart.
- **WorkflowTimeoutTrackingState**: Volatile in-memory state tracking open runs with configured execution or run timeouts. Must be reconstructed after restart.
- **NexusTimeoutTrackingState**: Volatile in-memory state tracking pending Nexus operations with schedule-to-close timeouts. Must be reconstructed after restart.
- **Recovery_Sequence**: The ordered set of steps a runtime node follows when acquiring a shard: acquire lease, start control tasks, sweep, admit commands.
- **Sticky_Affinity**: A performance hint that routes workflow tasks back to the worker that last executed the run. Sticky claims have an expiry time.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **CommitResult**: The outcome of a storage commit — Applied, Conflict, or Duplicate.
- **LeaseOutcome**: The outcome of a lease acquire or renew — Acquired, Renewed, or Rejected.
- **ShardId**: Routing and placement key that partitions the run space for non-overlapping assignment to runtime nodes.
- **InMemoryStore**: The development/test storage backend. Currently lacks shard-to-run assignment; this feature must address that gap.

## Requirements

---

### Requirement 1: Shard Lease Acquisition

**User Story:** As a Tokeira developer, I want the runtime to acquire shard leases with epoch fencing, so that only one runtime node owns a shard at a time and stale owners are prevented from committing.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Runtime SHALL call `LeaseRepository::try_acquire_bundle` with the shard identifier and the node's owner identity.
2. WHEN `try_acquire_bundle` returns `LeaseOutcome::Acquired` with a new epoch, THE Shard_Owner SHALL record the shard identifier and epoch as owned by the current node.
3. WHEN `try_acquire_bundle` returns `LeaseOutcome::Rejected`, THE Runtime SHALL NOT proceed with the recovery sequence for that shard.
4. THE Runtime SHALL include the current Shard_Epoch in all transition commits for runs belonging to that shard. This requires extending `RunRepository::commit_transition` to accept a `ShardEpoch` parameter alongside the existing `(run_key, transition)` arguments.
5. WHEN a commit is attempted with a Shard_Epoch that does not match the storage-side current epoch, THE storage layer SHALL reject the commit with `CommitResult::Conflict`.
6. THE `InMemoryStore` SHALL enforce epoch fencing in `commit_transition` by comparing the provided epoch against the shard's current lease epoch in `bundle_leases`.

---

### Requirement 2: Shard Lease Renewal

**User Story:** As a Tokeira developer, I want the runtime to periodically renew shard leases, so that owned shards are not lost due to lease expiry while the node is healthy.

#### Acceptance Criteria

1. WHEN a shard is acquired, THE Runtime SHALL start a Lease_Renewer background task for that shard.
2. THE Lease_Renewer SHALL periodically call `LeaseRepository::renew_bundle` with the shard identifier, owner identity, and current epoch.
3. WHEN `renew_bundle` returns `LeaseOutcome::Renewed`, THE Lease_Renewer SHALL continue renewing at the configured interval.
4. IF `renew_bundle` returns `LeaseOutcome::Rejected`, THEN THE Runtime SHALL stop accepting new commands for runs in that shard and drain in-flight work.
5. IF `renew_bundle` fails with a transient error, THEN THE Lease_Renewer SHALL retry with bounded backoff before declaring the lease lost.
6. THE Lease_Renewer SHALL use a `DbClass::Control` permit for lease renewal operations.

---

### Requirement 3: Epoch Fencing on Task Tokens

**User Story:** As a Tokeira developer, I want task tokens to carry the current shard epoch, so that completions from workers holding tokens issued under a previous epoch are rejected.

#### Acceptance Criteria

1. WHEN a workflow task is started, THE Runtime SHALL set the `shard_epoch` field on the `WorkflowTaskToken` to the current epoch of the shard owning that run.
2. WHEN an activity task is started, THE Runtime SHALL set the `shard_epoch` field on the `ActivityTaskToken` to the current epoch of the shard owning that run.
3. WHEN a task completion or failure arrives with a `shard_epoch` that does not match the current epoch for the run's shard, THE Runtime SHALL reject the completion without mutating state.

---

### Requirement 4: Post-Failover Workflow Task Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to discover and republish pending workflow tasks after shard acquisition, so that scheduled-but-not-started workflow tasks are not lost when the broker's in-memory state is discarded.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan `workflow_hot` for runs in that shard that have a pending workflow task in the scheduled (not started) state.
2. WHEN a pending workflow task is discovered, THE Sweeper SHALL republish the task to the Broker using the run's QueueKey.
3. THE Sweeper SHALL use the existing `republish_queue` helper or an equivalent shard-scoped variant to batch-republish workflow tasks.
4. THE Sweeper SHALL use a `DbClass::Maintenance` permit for storage queries during the sweep.

---

### Requirement 5: Post-Failover Activity Task Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to discover and republish dispatchable activity tasks after shard acquisition, so that pending activities are not lost when the activity broker's in-memory state is discarded.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan `activity_state` for dispatchable activity attempts belonging to runs in that shard.
2. WHEN a dispatchable activity task is discovered, THE Sweeper SHALL republish the task to the Activity_Broker using the activity's QueueKey.
3. THE Sweeper SHALL use the existing `republish_activity_queue` helper or an equivalent shard-scoped variant to batch-republish activity tasks.
4. THE Sweeper SHALL use a `DbClass::Maintenance` permit for storage queries during the sweep.

---

### Requirement 6: Post-Failover Timer Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to discover due timers after shard acquisition, so that timers that fired while the shard was unowned are processed promptly.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan `timer_bucket` for due timers belonging to runs in that shard.
2. WHEN a due timer is discovered, THE Sweeper SHALL inject a `Command::TimerDue` into the appropriate run actor's lane mailbox.
3. WHEN a `TimerDue` command is delivered for a timer that has already been canceled or fired, THE Kernel SHALL reject it as a harmless no-op.
4. THE Sweeper SHALL use a `DbClass::Maintenance` permit for storage queries during the sweep.

---

### Requirement 7: Expired Sticky Claim Cleanup

**User Story:** As a Tokeira developer, I want the sweeper to identify and clear expired sticky claims, so that workflow tasks with stale sticky affinity are republished to the general task queue for any compatible worker.

#### Acceptance Criteria

1. THE Sweeper SHALL identify runs with sticky affinity where the sticky expiry timestamp has passed.
2. WHEN an expired sticky claim is found on a run with a pending workflow task in the scheduled state, THE Sweeper SHALL republish the task to the Broker without sticky preference.
3. THE Sweeper SHALL clear the expired sticky hint from the run's in-memory representation so subsequent dispatch does not attempt sticky routing to a stale worker.

---

### Requirement 8: Activity Timeout Tracking Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to reconstruct ActivityTrackingState after shard acquisition, so that the activity timeout scanner can detect timeouts for in-flight activities that were started before the failover.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan authoritative activity state for open activities belonging to runs in that shard.
2. FOR EACH open activity discovered, THE Sweeper SHALL insert an entry into ActivityTrackingState with the scheduling and start timestamps derived from authoritative state.
3. THE reconstructed ActivityTrackingState entries SHALL contain sufficient information for the Activity_Timeout_Scanner to evaluate schedule-to-close, schedule-to-start, start-to-close, and heartbeat timeouts.
4. THE durable `ActivityState` (in `tokeira-kernel`) SHALL be extended with `scheduled_at: OffsetDateTime` and `started_at: Option<OffsetDateTime>` fields so that the sweeper can reconstruct timeout tracking without replaying history. The kernel SHALL populate `scheduled_at` from the `happened_at` timestamp of the `ActivityTaskScheduled` history event. The runtime SHALL populate `started_at` via the existing activity-start OCC upsert (activity starts are runtime-side, not kernel events).
5. THE `ActivitySweepEntry` returned by `list_open_activities_for_shard` SHALL carry `original_scheduled_at` and `started_at` derived from the durable `ActivityState` fields added in criterion 4.
6. WHEN reconstructing `ActivityTrackingEntry` from a sweep entry, THE Sweeper SHALL set `last_heartbeat_at` to `None` and `cancel_requested` to `false`. These fields have no durable source and are best-effort on recovery. Heartbeat timeout evaluation falls back to `started_at` when `last_heartbeat_at` is `None`, which may produce false-positive heartbeat timeouts after failover if the activity had been heartbeating regularly before the failover but the elapsed time since `started_at` exceeds the heartbeat timeout. This is an accepted trade-off: the alternative (suppressing heartbeat timeout evaluation entirely after recovery) would leave genuinely unresponsive activities undetected. Cancellation state will be re-established when the worker next heartbeats.

---

### Requirement 9: Workflow Timeout Tracking Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to reconstruct WorkflowTimeoutTrackingState after shard acquisition, so that the workflow timeout scanner can detect execution and run timeouts for workflows that were running before the failover.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan `workflow_hot` for open runs in that shard that have a configured `workflow_execution_timeout` or `workflow_run_timeout`.
2. FOR EACH qualifying run discovered, THE Sweeper SHALL insert an entry into WorkflowTimeoutTrackingState with the run's timeout configuration and start timestamps.
3. THE reconstructed WorkflowTimeoutTrackingState entries SHALL contain sufficient information for the Workflow_Timeout_Scanner to evaluate execution and run timeouts.

---

### Requirement 10: Nexus Timeout Tracking Reconstruction

**User Story:** As a Tokeira developer, I want the sweeper to reconstruct NexusTimeoutTrackingState after shard acquisition, so that the Nexus timeout scanner can detect timed-out Nexus operations that were pending before the failover.

#### Acceptance Criteria

1. WHEN a runtime node acquires a shard, THE Sweeper SHALL scan authoritative state for pending Nexus operations with a configured `schedule_to_close_timeout` belonging to runs in that shard.
2. FOR EACH qualifying Nexus operation discovered, THE Sweeper SHALL insert an entry into NexusTimeoutTrackingState with the operation's timeout configuration and scheduled timestamp.
3. THE reconstructed NexusTimeoutTrackingState entries SHALL contain sufficient information for the Nexus_Timeout_Scanner to evaluate schedule-to-close timeouts.
4. THE durable `PendingNexusOperation` (in `tokeira-kernel`) SHALL be extended with `schedule_to_close_timeout: Option<Duration>` and `scheduled_at: OffsetDateTime` fields so that the sweeper can reconstruct Nexus timeout tracking without replaying history. The kernel SHALL populate `scheduled_at` from the `happened_at` timestamp of the `NexusOperationScheduled` history event, and `schedule_to_close_timeout` from the command parameters.
5. THE `NexusSweepEntry` returned by `list_pending_nexus_operations_for_shard` SHALL carry `schedule_to_close_timeout` and `scheduled_at` derived from the durable `PendingNexusOperation` fields added in criterion 4.

---

### Requirement 11: Operational Recovery Sequence

**User Story:** As a Tokeira developer, I want shard acquisition to follow a strict ordered sequence, so that no commands are admitted before the sweeper has finished reconstructing dispatchable work and the runtime is in a consistent state.

#### Acceptance Criteria

1. WHEN a shard is acquired, THE Runtime SHALL follow this ordered sequence: (1) acquire lease and epoch, (2) start Lease_Renewer, (3) execute the Sweeper to rebuild dispatchable work and tracking state, (4) transition the shard to Active, (5) start shard-scoped scanners (Timer_Scanner, timeout scanners) only after the shard is Active, (6) load run actors on demand as commands arrive.
2. THE Runtime SHALL NOT admit new commands for a shard until the Sweeper has completed its initial scan for that shard.
3. THE Runtime SHALL NOT eagerly rehydrate all run actors after shard acquisition; run actors SHALL be loaded on demand when a command or due timer targets them.
4. WHEN the Sweeper completes its initial scan, THE Runtime SHALL transition the shard to an active state that accepts commands.
5. Shard-scoped background scanners (Timer_Scanner, Workflow_Timeout_Scanner, Activity_Timeout_Scanner, Nexus_Timeout_Scanner) SHALL NOT begin scanning for a shard until that shard has reached the Active state. This prevents scanners from injecting commands into lanes before the sweep has reconstructed the volatile state they depend on.

---

### Requirement 12: Shard-Scoped Timer Scanning

**User Story:** As a Tokeira developer, I want the timer scanner to be scoped to owned shards, so that multiple runtime nodes do not duplicate timer work.

#### Acceptance Criteria

1. THE Timer_Scanner SHALL only scan timer buckets for runs belonging to shards owned by the current runtime node.
2. WHEN shard ownership changes (shard acquired or relinquished), THE Timer_Scanner SHALL adjust its scan scope to reflect the current set of owned shards.
3. THE Timer_Scanner SHALL accept a shard filter parameter in its storage query, or filter results client-side, to restrict scanning to owned shards.

---

### Requirement 13: Shard-Scoped Timeout Scanning

**User Story:** As a Tokeira developer, I want all timeout scanners (workflow, activity, Nexus) to be scoped to owned shards, so that timeout detection is partitioned across runtime nodes without duplication.

#### Acceptance Criteria

1. THE Workflow_Timeout_Scanner SHALL only evaluate timeout entries for runs belonging to shards owned by the current runtime node.
2. THE Activity_Timeout_Scanner SHALL only evaluate timeout entries for runs belonging to shards owned by the current runtime node.
3. THE Nexus_Timeout_Scanner SHALL only evaluate timeout entries for runs belonging to shards owned by the current runtime node.
4. WHEN a shard is relinquished, THE Runtime SHALL remove all tracking entries for runs in that shard from WorkflowTimeoutTrackingState, ActivityTrackingState, and NexusTimeoutTrackingState.

---

### Requirement 14: Shard Assignment for Runs in InMemoryStore

**User Story:** As a Tokeira developer, I want the InMemoryStore to support shard-to-run assignment, so that shard-scoped queries (list dispatchable tasks for a shard, list due timers for a shard) can be tested in the development store.

#### Acceptance Criteria

1. THE InMemoryStore SHALL maintain a mapping from RunKey to ShardId.
2. WHEN a run is created, THE InMemoryStore SHALL assign the run to a shard (deterministically derived from the RunKey or explicitly provided).
3. THE InMemoryStore SHALL support shard-filtered variants of `list_dispatchable_workflow_tasks`, `list_dispatchable_activity_tasks`, and `list_due_timers`.
4. THE InMemoryStore SHALL support a shard-filtered query to list open runs with timeout configuration for a given shard.
5. THE InMemoryStore SHALL support a shard-filtered query to list open activities for a given shard.
6. THE InMemoryStore SHALL support a shard-filtered query to list pending Nexus operations with timeouts for a given shard.

---

### Requirement 15: Shard Relinquish and Drain

**User Story:** As a Tokeira developer, I want the runtime to cleanly relinquish a shard when the lease is lost, so that in-flight work drains gracefully and no new commands are accepted for that shard.

#### Acceptance Criteria

1. WHEN a shard lease is lost (renewal rejected or expired), THE Runtime SHALL immediately stop accepting new commands for runs in that shard.
2. WHEN a shard lease is lost, THE Runtime SHALL cancel the Lease_Renewer, and stop the shard-scoped Sweeper if it is still running.
3. WHEN a shard lease is lost, THE Runtime SHALL allow in-flight commands that have already been accepted to complete or fail naturally.
4. WHEN a shard lease is lost, THE Runtime SHALL remove all tracking entries for runs in that shard from WorkflowTimeoutTrackingState, ActivityTrackingState, and NexusTimeoutTrackingState.
5. THE Runtime SHALL NOT attempt to commit transitions for runs in a relinquished shard; the epoch fence in storage provides the safety backstop.
