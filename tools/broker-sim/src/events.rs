//! The broker simulation event taxonomy.
//!
//! Events split into workload (publishes, polls, claims), the reservation/commit
//! lifecycle (deliberately two-phase so the commit race is observable, mirroring
//! `placement-sim`'s begin/commit split), derived timers (grace scan, sticky
//! expiry, poll deadline, control-loop tick), and adversarial faults. The model
//! applies each in `handle` and schedules follow-ons through the harness context.

use crate::model::{
    ActivityId, Attempt, BuildId, DeliveryId, DeploymentName, LogicalTaskId, NamespaceId,
    PartitionIx, QueueKey, RunKey, TaskQueueName, WorkerIdentity,
};

/// The unit of work the broker delivers, with its routing key and sticky pref.
#[derive(Clone, Copy, Debug)]
pub struct BrokerEvent {
    /// What happens at this event's simulated time.
    pub kind: BrokerEventKind,
}

/// The discriminated event kinds. Grouped by role in the doc comments.
#[derive(Clone, Copy, Debug)]
pub enum BrokerEventKind {
    // ---- Workload ----
    /// Publish a workflow task for delivery (subject to dedup + tiering).
    PublishWft {
        id: LogicalTaskId,
        queue: QueueKey,
        sticky_target: Option<WorkerIdentity>,
        priority: u8,
    },
    /// Publish an activity task attempt.
    PublishActivity {
        id: LogicalTaskId,
        queue: QueueKey,
        priority: u8,
    },
    /// Publish a read-only query task (bypasses dedup + backlog).
    PublishQuery {
        queue: QueueKey,
        sticky_target: Option<WorkerIdentity>,
    },
    /// A worker long-polls a queue.
    Poll {
        queue: QueueKey,
        worker: WorkerIdentity,
        /// Poll retry attempt (carried for fidelity / future retry modelling).
        #[allow(dead_code)]
        attempt: u8,
    },
    /// Pull a specific run's task out of the general tier for eager dispatch.
    DirectClaim { queue: QueueKey, run_key: RunKey },

    // ---- Reservation / commit lifecycle (two-phase) ----
    /// A matched poll reserves the task and begins the start transaction.
    /// Reservations are begun inline from a match today; this explicit variant
    /// is retained for an external driver and is not currently scheduled.
    #[allow(dead_code)]
    ReserveAndStart {
        id: LogicalTaskId,
        queue: QueueKey,
        worker: WorkerIdentity,
        delivery_id: DeliveryId,
    },
    /// The start transaction resolves; `will_commit` decides commit vs abort.
    StartTxnCommit {
        id: LogicalTaskId,
        delivery_id: DeliveryId,
        will_commit: bool,
    },
    /// A worker completes a delivered task.
    CompleteTask {
        id: LogicalTaskId,
        delivery_id: DeliveryId,
    },

    // ---- Derived timers ----
    /// Grace scanner pass: spill aged live-ready tasks on a queue to backlog.
    GraceScan { queue: QueueKey },
    /// A sticky claim's TTL expired; promote it to general.
    StickyTtlExpire { id: LogicalTaskId, queue: QueueKey },
    /// A long poll's deadline elapsed.
    PollDeadline { queue: QueueKey, waiter_id: u64 },
    /// Control-loop tick: recompute the budget split from backlog age.
    ControlLoopTick,

    // ---- Faults ----
    /// The broker process restarts: discard BrokerState, keep authoritative.
    BrokerCrash,
    /// A delivery lease expires, enabling redelivery and staling old completion.
    LeaseExpire {
        id: LogicalTaskId,
        delivery_id: DeliveryId,
    },
    /// A worker crashes, freeing its in-flight work.
    WorkerCrash { worker: WorkerIdentity },
    /// A worker becomes denied on a (namespace, task_queue).
    DenyWorker {
        namespace: NamespaceId,
        task_queue: TaskQueueName,
        worker: WorkerIdentity,
    },
    /// Build backlog on one partition while pollers wait on another.
    PartitionBacklogPressure { queue: QueueKey },
    /// Drive sustained high backlog age to exercise the control loop.
    SustainedBacklogAge { queue: QueueKey },
    /// Re-publish an already-enqueued logical task (dedup stress).
    DuplicatePublish {
        id: LogicalTaskId,
        queue: QueueKey,
        priority: u8,
    },
}

/// Helper to build a workflow-task queue key on a chosen partition.
pub fn wft_queue(
    namespace: NamespaceId,
    task_queue: TaskQueueName,
    deployment: Option<DeploymentName>,
    build: Option<BuildId>,
    partition: PartitionIx,
) -> QueueKey {
    QueueKey {
        namespace,
        task_queue,
        kind: crate::model::TaskKind::Workflow,
        deployment,
        build,
        partition,
    }
}

/// Helper to build an activity-task queue key on a chosen partition.
pub fn activity_queue(
    namespace: NamespaceId,
    task_queue: TaskQueueName,
    partition: PartitionIx,
) -> QueueKey {
    QueueKey {
        namespace,
        task_queue,
        kind: crate::model::TaskKind::Activity,
        deployment: None,
        build: None,
        partition,
    }
}

/// Convenience constructors keep `bootstrap`/fault code readable.
impl BrokerEvent {
    /// Wrap a kind into an event.
    pub fn new(kind: BrokerEventKind) -> Self {
        BrokerEvent { kind }
    }
}

/// A workflow-task logical id.
pub fn wft_id(run: RunKey, seq: u64) -> LogicalTaskId {
    LogicalTaskId::Wft(run, seq)
}

/// An activity-task logical id.
pub fn act_id(run: RunKey, activity: ActivityId, attempt: Attempt) -> LogicalTaskId {
    LogicalTaskId::Activity(run, activity, attempt)
}
