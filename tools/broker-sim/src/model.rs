//! Domain types and pure state for the delivery-broker model.
//!
//! These re-model the keying and structure of
//! `crates/tokeira-runtime/src/broker.rs` without importing it (the same
//! re-modeling choice `placement-sim` made for DSQL/runtime). The load-bearing
//! split is [`BrokerState`] (ephemeral — discarded on a broker crash) versus
//! [`AuthoritativePendingState`] (the per-run truth the broker is an optimiser
//! over, kept across a crash and reconstructed by the sweeper). That separation
//! is what lets the simulator falsify the broker's central correctness claim.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Namespace identity (modelled as a small integer).
pub type NamespaceId = u32;
/// Task-queue name (modelled as a small integer).
pub type TaskQueueName = u32;
/// Worker/poller identity.
pub type WorkerIdentity = u32;
/// Workflow run key.
pub type RunKey = u64;
/// Logical workflow-task sequence within a run.
pub type LogicalTaskSeq = u64;
/// Activity identifier within a run.
pub type ActivityId = u32;
/// Activity attempt number.
pub type Attempt = u32;
/// Build identifier for version compatibility.
pub type BuildId = u32;
/// Worker-deployment name for version compatibility.
pub type DeploymentName = u32;
/// One partition index of a logically-named task queue.
pub type PartitionIx = u32;

/// Identity of a single delivery/reservation, used to fence stale completions.
///
/// Each successful reservation+start mints a fresh `DeliveryId`; a completion is
/// only current if it carries the id of the delivery currently in flight. After
/// a lease expiry, redelivery, or broker crash, the prior delivery's id is no
/// longer current, so a late completion under it is rejected (invariant S4).
pub type DeliveryId = u64;

/// Workflow tasks and activity tasks are brokered separately (broker.rs splits
/// `InMemoryBroker` from `InMemoryActivityBroker`); query tasks are a distinct
/// read-only path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskKind {
    /// Workflow task — at most one in flight per run (invariant S1).
    Workflow,
    /// Activity task — many may run concurrently per run.
    Activity,
    /// Read-only query task — bypasses dedup and backlog.
    Query,
}

/// The broker's queue-family key: more than a queue name.
///
/// Mirrors `broker.rs` `QueueKey` — namespace, task-queue name, task kind, and
/// version-compatibility (deployment/build). `partition` additionally splits a
/// logical queue into partitions, so backlog on one partition while pollers wait
/// on another is representable (the sync-match-collapse condition).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueKey {
    /// Owning namespace.
    pub namespace: NamespaceId,
    /// Logical task-queue name.
    pub task_queue: TaskQueueName,
    /// Workflow / activity / query.
    pub kind: TaskKind,
    /// Optional deployment for version compatibility (None == unversioned).
    pub deployment: Option<DeploymentName>,
    /// Optional build id for version compatibility.
    pub build: Option<BuildId>,
    /// Partition of the logical queue.
    pub partition: PartitionIx,
}

/// The deduplication identity of a deliverable task.
///
/// Mirrors `broker.rs`: workflow tasks key by `(run, logical_seq)`, activity
/// tasks by `(run, activity_id, attempt)`. A new activity attempt is therefore a
/// *distinct* logical task, which is why a redelivered attempt is not a
/// double-start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogicalTaskId {
    /// A workflow task.
    Wft(RunKey, LogicalTaskSeq),
    /// An activity task attempt.
    Activity(RunKey, ActivityId, Attempt),
}

impl LogicalTaskId {
    /// The run this task belongs to.
    pub fn run(&self) -> RunKey {
        match self {
            LogicalTaskId::Wft(run, _) => *run,
            LogicalTaskId::Activity(run, _, _) => *run,
        }
    }

    /// True for workflow tasks (subject to the single-in-flight-per-run rule).
    pub fn is_wft(&self) -> bool {
        matches!(self, LogicalTaskId::Wft(_, _))
    }
}

/// A task sitting in a live-ready tier, awaiting a poll.
#[derive(Clone, Debug)]
pub struct ReadyTask {
    /// Dedup identity.
    pub id: LogicalTaskId,
    /// The queue family it is keyed under. Held for fidelity (the task is also
    /// stored under its queue key); not separately read by the invariants.
    #[allow(dead_code)]
    pub queue: QueueKey,
    /// Preferred worker for sticky tasks; `None` for general tasks.
    pub sticky_target: Option<WorkerIdentity>,
    /// Simulated time it entered the live-ready tier (for grace-scan aging).
    pub entered_at_ms: u64,
    /// Sticky TTL deadline; `None` for general tasks. Modelled structurally;
    /// expiry is driven by the scheduled `StickyTtlExpire` event.
    #[allow(dead_code)]
    pub sticky_deadline_ms: Option<u64>,
    /// Priority band for backlog fairness (lower = higher priority).
    pub priority: u8,
}

/// A durable backlog entry (Tier C). Carries enough to redeliver the task and
/// its enqueue order for FIFO-within-priority fairness.
#[derive(Clone, Debug)]
pub struct BacklogItem {
    /// Dedup identity.
    pub id: LogicalTaskId,
    /// Queue family. Held for fidelity (backlog is keyed by queue); not read
    /// separately by the invariants.
    #[allow(dead_code)]
    pub queue: QueueKey,
    /// Priority band (lower value = dispatched first).
    pub priority: u8,
    /// Monotonic enqueue order, for FIFO within a priority band.
    pub enqueue_seq: u64,
}

/// An in-memory waiting poller (long poll). Holds no durable resource.
#[derive(Clone, Debug)]
pub struct Waiter {
    /// Unique waiter id within the run.
    pub waiter_id: u64,
    /// The polling worker.
    pub worker: WorkerIdentity,
    /// Poll deadline; the poll resolves as a timeout if unmatched by then.
    /// Modelled structurally; the timeout is driven by the scheduled
    /// `PollDeadline` event.
    #[allow(dead_code)]
    pub deadline_ms: u64,
}

/// A delivery currently held by a worker between start and completion.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// The fencing identity for this delivery.
    pub delivery_id: DeliveryId,
    /// The worker holding the token.
    pub worker: WorkerIdentity,
    /// Lease expiry; after this, redelivery may occur and a late completion is
    /// stale. Modelled structurally (the `LeaseExpire` event carries the same
    /// deadline as a scheduled event); retained for fidelity and debugging.
    #[allow(dead_code)]
    pub lease_until_ms: u64,
    /// Whether the authoritative start transaction has committed. A token is
    /// only legitimately held when this is true (invariant S3).
    pub committed: bool,
}

/// The weighted service-budget split across delivery offers (control loop).
///
/// Percentages need not sum to 100; they are relative weights the control loop
/// shifts by backlog age. The invariant the model upholds is that the backlog
/// share never reaches a level that starves fresh sync-matchable work (L4).
#[derive(Clone, Copy, Debug)]
pub struct BudgetSplit {
    /// Weight given to sticky offers. Part of the modelled split; the
    /// no-starvation invariant (L4) compares backlog against live_ready, so the
    /// sticky weight is structural rather than asserted-on.
    #[allow(dead_code)]
    pub sticky: u8,
    /// Weight given to live-ready offers.
    pub live_ready: u8,
    /// Weight given to durable backlog offers.
    pub backlog: u8,
}

impl Default for BudgetSplit {
    fn default() -> Self {
        // Low-backlog default: bias toward sticky/live, minimal backlog share.
        BudgetSplit {
            sticky: 50,
            live_ready: 40,
            backlog: 10,
        }
    }
}

/// Per-queue delivery-quality accumulators (sync-match rate, poll success,
/// schedule-to-start). Aggregated into the report.
#[derive(Clone, Debug, Default)]
pub struct QueueQuality {
    /// Tasks published on this queue.
    pub published: u64,
    /// Of those, how many found a waiting poller at publish time (sync match).
    pub published_with_waiter: u64,
    /// Polls that resolved on this queue.
    pub polls_resolved: u64,
    /// Of those, how many received work (vs timed out).
    pub polls_with_work: u64,
    /// Sum of schedule-to-start latencies (sim ms), for a mean.
    pub sched_to_start_total_ms: u64,
    /// Count of schedule-to-start samples.
    pub sched_to_start_samples: u64,
}

/// The ephemeral broker state — everything a broker crash discards.
///
/// Discarding this (a `BrokerCrash` event) must lose no authoritative pending
/// task and mark nothing durable complete; the sweeper rebuilds the live tiers
/// from [`AuthoritativePendingState`]. That recoverability is invariant S5.
#[derive(Clone, Debug, Default)]
pub struct BrokerState {
    /// Tier B, sticky tier: tasks whose run prefers a specific worker.
    pub sticky_ready: BTreeMap<QueueKey, VecDeque<ReadyTask>>,
    /// Tier B, general tier: tasks any compatible poller may take.
    pub general_ready: BTreeMap<QueueKey, VecDeque<ReadyTask>>,
    /// Tier C: durable backlog, priority-ordered with FIFO within a band.
    pub backlog: BTreeMap<QueueKey, Vec<BacklogItem>>,
    /// In-memory waiters (long polls) per queue.
    pub waiters: BTreeMap<QueueKey, VecDeque<Waiter>>,
    /// Dedup set: a logical task already enqueued is not enqueued again.
    pub enqueued: BTreeSet<LogicalTaskId>,
    /// Query tasks per queue (bypass dedup + backlog).
    pub query_ready: BTreeMap<QueueKey, VecDeque<(WorkerIdentity, Option<WorkerIdentity>)>>,
    /// Workers barred from a queue (version/build/shutdown).
    pub denied_workers: BTreeSet<(NamespaceId, TaskQueueName, WorkerIdentity)>,
    /// Live deliveries keyed by logical task. Keyed by id so structurally one
    /// entry exists per id; the model additionally tracks live-delivery counts
    /// in the authoritative state to detect a buggy second start.
    pub inflight: BTreeMap<LogicalTaskId, Delivery>,
    /// The active control-loop budget split.
    pub budget: BudgetSplit,
    /// Per-queue delivery-quality accumulators.
    pub quality: BTreeMap<QueueKey, QueueQuality>,
}

/// The authoritative per-run truth the broker optimises over.
///
/// Held separately from [`BrokerState`] so a broker crash can drop the latter
/// without losing any of this. Analogous to `workflow_hot.pending_wft` and
/// `activity_state` in the real runtime.
#[derive(Clone, Debug, Default)]
pub struct AuthoritativePendingState {
    /// At most one pending workflow task per run (invariant S1). Value is the
    /// logical task and the time it was scheduled (for schedule-to-start).
    pub pending_wft: BTreeMap<RunKey, (LogicalTaskId, u64)>,
    /// Pending activity attempts keyed by `(run, activity_id)`.
    pub pending_activities: BTreeMap<(RunKey, ActivityId), (LogicalTaskId, u64)>,
    /// Count of currently-live (started, not completed/abandoned) deliveries per
    /// logical task. The no-double-start invariant (S2) is that every value
    /// stays `<= 1`; a correct broker never starts a second live delivery for a
    /// task while one is in flight.
    pub live_deliveries: BTreeMap<LogicalTaskId, u32>,
    /// Tasks that have been completed (terminal), to detect double-complete and
    /// confirm no durable loss.
    pub completed: BTreeSet<LogicalTaskId>,
    /// Sticky claims whose TTL expired and must be republished general (S7).
    pub expired_sticky: BTreeSet<RunKey>,
}

impl AuthoritativePendingState {
    /// The set of all currently-pending logical tasks (WFT + activity),
    /// independent of broker state. The sweeper republishes exactly these.
    pub fn all_pending(&self) -> Vec<(LogicalTaskId, u64)> {
        let mut out: Vec<(LogicalTaskId, u64)> = Vec::new();
        for (id, scheduled_at) in self.pending_wft.values() {
            out.push((*id, *scheduled_at));
        }
        for (id, scheduled_at) in self.pending_activities.values() {
            out.push((*id, *scheduled_at));
        }
        out
    }
}

/// Tunable parameters whose concrete values are design-phase decisions; the
/// model fixes their meaning, not their defaults.
#[derive(Clone, Copy, Debug)]
pub struct BrokerCfg {
    /// Live-ready age before the grace scanner spills a task to backlog.
    pub grace_window_ms: u64,
    /// Maximum concurrently-waiting pollers per queue (L2).
    pub max_waiters: usize,
    /// Number of partitions per logical queue (L4 / sync-match collapse).
    pub partitions_per_queue: u32,
    /// Sticky claim TTL before promotion to general (S7).
    pub sticky_ttl_ms: u64,
    /// Delivery lease before redelivery is permitted (S4).
    pub lease_ms: u64,
    /// Backlog age (ms) above which the control loop raises the backlog share.
    pub backlog_age_high_ms: u64,
}

impl Default for BrokerCfg {
    fn default() -> Self {
        BrokerCfg {
            grace_window_ms: 50,
            max_waiters: 64,
            partitions_per_queue: 4,
            sticky_ttl_ms: 30,
            lease_ms: 100,
            backlog_age_high_ms: 200,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_task_id_run_and_kind() {
        let wft = LogicalTaskId::Wft(7, 2);
        assert_eq!(wft.run(), 7);
        assert!(wft.is_wft());
        let act = LogicalTaskId::Activity(7, 1, 3);
        assert_eq!(act.run(), 7);
        assert!(!act.is_wft());
        // A new attempt is a distinct logical task (so redelivery is not a
        // double-start of the same id).
        assert_ne!(act, LogicalTaskId::Activity(7, 1, 4));
    }

    #[test]
    fn all_pending_collects_wft_and_activities() {
        let mut auth = AuthoritativePendingState::default();
        auth.pending_wft.insert(1, (LogicalTaskId::Wft(1, 0), 10));
        auth.pending_activities
            .insert((1, 5), (LogicalTaskId::Activity(1, 5, 1), 12));
        let pending = auth.all_pending();
        assert_eq!(pending.len(), 2);
    }
}
