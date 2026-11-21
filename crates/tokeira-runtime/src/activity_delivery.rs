//! Disposable activity delivery from committed state to queue-home admission.
//!
//! Publication only enqueues bounded work. Acknowledgements retain hints and
//! never acquire a lane, read inputs, or authorize an activity start.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_kernel::{ActivityState, WorkflowState, deadline_after};
use tokeira_storage::DispatchableActivityTask;
use tokeira_types::{ExecutionStatus, QueueKey, ShardId};

use crate::{
    RuntimeShutdownHandle,
    broker::InMemoryActivityBroker,
    discovery::{PlacementQueueHomes, QueueHomeProvider},
    shard::ShardAcquisition,
    workflow_delivery::sender::PublicationSource,
    workflow_offers::OfferSource,
};

mod sender;

/// Volatile routing and lifetime metadata, separate from persisted task encodings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivityQueueOffer {
    /// Exact committed incarnation and resolved queue coordinates, without inputs.
    pub task: DispatchableActivityTask,
    /// Execution-owner routing hint; authoritative start derives its own home.
    pub execution_home: ShardId,
    /// Original eligibility of this attempt, never its arrival time.
    pub dispatch_at: OffsetDateTime,
    /// Absolute retention deadline; duplicate admission never extends it.
    pub expires_at: OffsetDateTime,
}

impl ActivityQueueOffer {
    pub(crate) fn from_state(
        state: &WorkflowState,
        activity: &ActivityState,
        execution_home: ShardId,
        now: OffsetDateTime,
    ) -> Option<Self> {
        if state.status != ExecutionStatus::Running
            || activity.started_at.is_some()
            || activity.started_event_id.is_some()
            || activity.pause_info.is_some()
        {
            return None;
        }
        let dispatch_at = activity.current_attempt_scheduled_at?;
        let mut expires_at = deadline_after(now, time::Duration::seconds(5));
        for (anchor, timeout) in [
            (activity.scheduled_at, activity.schedule_to_close_timeout),
            (dispatch_at, activity.schedule_to_start_timeout),
        ] {
            if let Some(timeout) = timeout.filter(|timeout| !timeout.is_zero()) {
                expires_at = expires_at.min(deadline_after(anchor, timeout));
            }
        }
        if expires_at <= now || expires_at <= dispatch_at {
            return None;
        }
        Some(Self {
            task: crate::runtime::activity_dispatch_task(state.run_key, state, activity),
            execution_home,
            dispatch_at,
            expires_at,
        })
    }

    /// Conservative retained-memory accounting, including owned strings and index copies.
    pub(crate) fn retained_bytes(&self) -> usize {
        let queue = &self.task.queue;
        1024usize
            .saturating_add(self.task.activity_id.len().saturating_mul(3))
            .saturating_add(queue.task_queue.0.len().saturating_mul(3))
            .saturating_add(
                queue
                    .deployment
                    .as_ref()
                    .map_or(0, |v| v.0.len())
                    .saturating_mul(3),
            )
            .saturating_add(
                queue
                    .build_id
                    .as_ref()
                    .map_or(0, |v| v.0.len())
                    .saturating_mul(3),
            )
            .saturating_add(
                self.task
                    .priority
                    .as_ref()
                    .map_or(0, |p| p.fairness_key.len())
                    .saturating_mul(2),
            )
    }
}

/// Admission acknowledges only disposable retention in the receiver's acquisition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityOfferOutcome {
    /// Retained for a compatible poll.
    Admitted,
    /// Already retained, with its original deadline.
    Known,
    /// Wrong destination or no serving queue-home acquisition.
    NotHome,
    /// A count or byte bound refused acceleration.
    Full,
    /// The absolute hint deadline elapsed.
    Expired,
    /// Original attempt eligibility is still in the future.
    NotDue,
}

/// Finite transport failure categories; none changes durable activity state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ActivityOfferError {
    /// Trusted placement is unavailable.
    #[error("activity offer routing unavailable")]
    Routing,
    /// Signing or verification failed.
    #[error("activity offer authentication failed")]
    Authentication,
    /// Request, response or handler limit exceeded.
    #[error("activity offer capacity exceeded")]
    Capacity,
    /// Admission is unknown after a transport failure.
    #[error("activity offer transport unavailable")]
    Transport,
    /// The bounded attempt elapsed.
    #[error("activity offer deadline exceeded")]
    Deadline,
    /// Malformed request or reply.
    #[error("invalid activity offer")]
    Invalid,
    /// An older peer has no offer method; discovery remains sufficient.
    #[error("activity offer method unavailable")]
    Unimplemented,
}

/// Edge supplies trusted placement, pooled channels and the existing push purpose.
#[async_trait]
pub trait RemoteActivityOfferSink: Debug + Send + Sync {
    /// One bounded attempt. Refresh consults trusted placement, never a reply endpoint.
    async fn offer(
        &self,
        offer: &ActivityQueueOffer,
        destination: ShardId,
        refresh: bool,
    ) -> Result<ActivityOfferOutcome, ActivityOfferError>;
}

/// Delivery origin is diagnostic only; recovery and eligibility share a scheduling class.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ActivityDeliverySource {
    Publication,
    Eligibility,
    Recovery,
}
impl ActivityDeliverySource {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Publication => "publication",
            Self::Eligibility => "eligibility",
            Self::Recovery => "recovery",
        }
    }
    pub(crate) fn scheduling_class(self) -> PublicationSource {
        match self {
            Self::Publication => PublicationSource::Publication,
            _ => PublicationSource::Recovery,
        }
    }
}
impl From<PublicationSource> for ActivityDeliverySource {
    fn from(source: PublicationSource) -> Self {
        match source {
            PublicationSource::Publication => Self::Publication,
            PublicationSource::Recovery => Self::Recovery,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ActivityAdmissionSource {
    Origin(ActivityDeliverySource),
    Discovery,
    Pushed,
}
impl ActivityAdmissionSource {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Origin(source) => source.label(),
            Self::Discovery => "discovery",
            Self::Pushed => "pushed",
        }
    }
}
impl From<ActivityDeliverySource> for ActivityAdmissionSource {
    fn from(source: ActivityDeliverySource) -> Self {
        Self::Origin(source)
    }
}
impl From<OfferSource> for ActivityAdmissionSource {
    fn from(source: OfferSource) -> Self {
        match source {
            OfferSource::Local => Self::Origin(ActivityDeliverySource::Publication),
            OfferSource::Discovered => Self::Discovery,
            OfferSource::Pushed => Self::Pushed,
        }
    }
}

pub(crate) struct ActivityDelivery {
    pub(crate) broker: InMemoryActivityBroker,
    pub(crate) homes: Arc<PlacementQueueHomes>,
    sender: RwLock<Option<sender::ActivityOfferSender>>,
    pub(crate) shutdown: RuntimeShutdownHandle,
    pub(crate) clock: Arc<dyn Clock>,
}
impl Debug for ActivityDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivityDelivery").finish_non_exhaustive()
    }
}
impl ActivityDelivery {
    pub(crate) fn new(
        broker: InMemoryActivityBroker,
        homes: Arc<PlacementQueueHomes>,
        shutdown: RuntimeShutdownHandle,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            broker,
            homes,
            sender: RwLock::new(None),
            shutdown,
            clock,
        })
    }
    pub(crate) fn install_sender(self: &Arc<Self>, sink: Arc<dyn RemoteActivityOfferSink>) {
        let mut sender = self.sender.write().expect("activity sender lock poisoned");
        assert!(
            sender.is_none(),
            "activity sender installed once at startup"
        );
        *sender = Some(sender::ActivityOfferSender::spawn(self.clone(), sink));
    }
    pub(crate) fn destination(&self, queue: &QueueKey) -> ShardId {
        let (partitions, bundles) = *self
            .homes
            .counts
            .read()
            .expect("queue placement lock poisoned");
        tokeira_types::bundle_for_partition(
            tokeira_types::workflow_queue_partition(
                queue.namespace_id,
                &queue.task_queue.0,
                partitions,
            ),
            bundles,
        )
    }
    pub(crate) async fn receive(
        &self,
        offer: ActivityQueueOffer,
        destination: ShardId,
        source: impl Into<ActivityAdmissionSource>,
    ) -> ActivityOfferOutcome {
        let source = source.into();
        if self.destination(&offer.task.queue) != destination {
            return ActivityOfferOutcome::NotHome;
        }
        let Some(home) = self.homes.local_home(&offer.task.queue) else {
            return ActivityOfferOutcome::NotHome;
        };
        self.broker
            .admit_activity_offer(offer, Some(&home), source, self.clock.now(), None)
            .await
    }
    pub(crate) async fn publish(
        &self,
        offer: ActivityQueueOffer,
        acquisition: ShardAcquisition,
        source: ActivityDeliverySource,
    ) {
        if !acquisition.valid() {
            return;
        }
        let destination = self.destination(&offer.task.queue);
        if self.homes.local_home(&offer.task.queue).is_some() {
            self.receive(offer, destination, source).await;
        } else if let Some(sender) = self
            .sender
            .read()
            .expect("activity sender lock poisoned")
            .as_ref()
        {
            sender.try_enqueue(offer, acquisition, source, destination);
        }
    }
}
