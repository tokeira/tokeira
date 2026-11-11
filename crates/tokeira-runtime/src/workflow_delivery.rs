//! Disposable post-commit workflow delivery to the queue home's admission set.
//!
//! The receiver judges sticky availability from its own poll and registry view.
//! Acknowledgements never contact an execution lane; only the existing worker
//! start path can turn a retained hint into an authoritative transition.

use std::{
    fmt::Debug,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokeira_clock::Clock;
use tokeira_kernel::{
    ContinueAsNewVersioningBehavior, LoadedRun, VersioningBehavior, WorkerDeploymentVersionRef,
    WorkflowState,
};
use tokeira_storage::{
    RunRepository, WorkflowDispatchRouting, WorkflowOfferHint, derive_workflow_dispatch,
};
use tokeira_types::{BuildId, DeploymentId, QueueKey, ShardId, WorkflowId};

use crate::{
    RuntimeShutdownHandle,
    broker::InMemoryBroker,
    deployment_registry::DeploymentRegistry,
    discovery::{PlacementQueueHomes, QueueHomeProvider},
    runtime::workflow_task::{resolve_workflow_task_target_version, routing_config_target},
    shard::ShardAcquisition,
    workflow_offers::{OfferAdmission, OfferSource},
};

mod sender;
mod sweep;
#[cfg(test)]
mod tests;
use sender::PublicationSource;

/// Resolve a normal queue from the caller's state using the start path's policy.
/// Local publication and recovery reuse their state; deferred sender fallback reloads it.
pub(crate) async fn fallback_target(
    registry: Option<&DeploymentRegistry>,
    state: &WorkflowState,
    mut queue: QueueKey,
) -> Result<QueueKey> {
    if queue.deployment.is_some() {
        return Ok(queue);
    }
    if let Some(registry) = registry {
        let preferred = state
            .effective_deployment()
            .map(|v| v.deployment_name.as_str())
            .or(state.worker_deployment_name.as_deref());
        let routing = registry
            .workflow_task_routing_config(queue.namespace_id, &queue.task_queue.0, preferred)
            .await?;
        if let Some(version) =
            resolve_workflow_task_target_version(&routing, state).deployment_version
        {
            queue.deployment = Some(DeploymentId(version.deployment_name));
            queue.build_id = Some(BuildId(version.build_id));
        }
    }
    Ok(queue)
}

/// Publication found that this incarnation is no longer pending.
pub(crate) fn record_obsolete_publication() {
    sender::record(PublicationSource::Publication, "obsolete");
}

/// Semantic failures supported by stock-default worker versioning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StickyUnavailableReason {
    /// No eligible poll within ten seconds, including worker shutdown.
    NoRecentPoller,
    /// The queue's current/ramping target differs from the task's deployment.
    DeploymentChanged,
}

/// Volatile admission acknowledgement; never a start receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowOfferOutcome {
    /// Retained in the receiving acquisition.
    Admitted,
    /// This acquisition already retains the incarnation.
    Known,
    /// The publisher may attempt the original normal queue.
    StickyUnavailable(StickyUnavailableReason),
    /// Placement or the receiving acquisition changed.
    NotHome,
    /// Admission exhausted a count or byte limit.
    Full,
    /// The original absolute deadline elapsed.
    Expired,
}

/// Bounded failure categories shared by runtime and transport diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WorkflowOfferError {
    /// Trusted routing or registry data could not be read.
    #[error("workflow offer routing unavailable")]
    Routing,
    /// Credentials could not be minted or were rejected.
    #[error("workflow offer authentication failed")]
    Authentication,
    /// Request/response or handler capacity was exhausted.
    #[error("workflow offer capacity exceeded")]
    Capacity,
    /// No definite admission reply was received.
    #[error("workflow offer transport unavailable")]
    Transport,
    /// The bounded attempt elapsed; admission may have succeeded.
    #[error("workflow offer deadline exceeded")]
    Deadline,
    /// The wire hint or reply is malformed.
    #[error("malformed workflow offer")]
    Invalid,
}
impl WorkflowOfferError {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Routing => "routing",
            Self::Authentication => "authentication",
            Self::Capacity => "capacity",
            Self::Transport => "transport",
            Self::Deadline => "deadline",
            Self::Invalid => "invalid",
        }
    }
}

/// Source routing operands; these are hints and confer no write authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowOfferDirective {
    /// Stable input for the current/ramping split at the receiving home.
    pub workflow_id: WorkflowId,
    /// Effective behavior after overrides and transitions.
    pub behavior: VersioningBehavior,
    /// Source deployment, including a pinned target when applicable.
    pub deployment: Option<WorkerDeploymentVersionRef>,
    /// Legacy assigned build bypasses deployment fallback; rules remain disabled.
    pub assigned_build_id: Option<BuildId>,
    /// Initial continue-as-new routing can explicitly choose ramping.
    pub use_ramping_version: bool,
}
impl WorkflowOfferDirective {
    pub(crate) fn from_state(state: &WorkflowState) -> Self {
        let behavior = state.effective_behavior();
        // MakeDirectiveForWorkflowTask, common/worker_versioning/worker_versioning.go
        // @ v1.31.0: modern behavior takes precedence over legacy assigned builds.
        let assigned_build_id = (behavior == VersioningBehavior::Unspecified)
            .then(|| {
                state.assigned_build_id().cloned().or_else(|| {
                    state
                        .versioning_info
                        .as_ref()
                        .and_then(|info| info.most_recent_worker_version_stamp.as_ref())
                        .filter(|stamp| stamp.use_versioning && !stamp.build_id.is_empty())
                        .map(|stamp| BuildId(stamp.build_id.clone()))
                })
            })
            .flatten();
        Self {
            workflow_id: state.workflow_id.clone(),
            behavior,
            deployment: state.effective_deployment().cloned(),
            assigned_build_id,
            use_ramping_version: state.previous_started_event_id == 0
                && state.versioning_info.as_ref().is_some_and(|info| {
                    info.continue_as_new_initial_versioning_behavior
                        == ContinueAsNewVersioningBehavior::UseRampingVersion
                }),
        }
    }
}

/// Dispatch-row envelope plus the source operands needed for live sticky checks.
/// This type is never persisted and contains no workflow inputs or credentials.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowOffer {
    /// Immutable schedule/origin and the selected volatile destination.
    pub hint: WorkflowOfferHint,
    /// Receiver-verifiable routing operands, revalidated by authoritative start.
    pub directive: WorkflowOfferDirective,
}
impl WorkflowOffer {
    pub(crate) fn from_state(state: &WorkflowState, home: ShardId) -> Result<Option<Self>> {
        let Some(row) = derive_workflow_dispatch(state, home) else {
            return Ok(None);
        };
        let mut hint = WorkflowOfferHint::from_dispatch(&row)?;
        if let Some(origin) = &mut hint.sticky_origin
            && let Some(version) = state.effective_deployment()
        {
            hint.task
                .queue
                .deployment
                .get_or_insert_with(|| DeploymentId(version.deployment_name.clone()));
            hint.task
                .queue
                .build_id
                .get_or_insert_with(|| BuildId(version.build_id.clone()));
            origin.sticky_queue = hint.task.queue.clone();
        }
        Ok(Some(Self {
            hint,
            directive: WorkflowOfferDirective::from_state(state),
        }))
    }
}

pub(crate) fn record_received(result: &Result<WorkflowOfferOutcome, WorkflowOfferError>) {
    metrics::counter!(crate::metrics::WORKFLOW_OFFER_DELIVERY_TOTAL, "source" => "pushed", "outcome" => match result {
        Ok(WorkflowOfferOutcome::Admitted) => "admitted", Ok(WorkflowOfferOutcome::Known) => "known",
        Ok(WorkflowOfferOutcome::NotHome) => "ownership", Ok(WorkflowOfferOutcome::Full) => "capacity", Ok(WorkflowOfferOutcome::Expired) => "expired",
        Ok(WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::NoRecentPoller)) => "sticky_no_recent_poller",
        Ok(WorkflowOfferOutcome::StickyUnavailable(StickyUnavailableReason::DeploymentChanged)) => "sticky_deployment_changed",
        Err(error) => error.label(),
    }).increment(1);
}

/// Edge supplies trusted placement, pooled channels and purpose-authenticated RPCs.
#[async_trait]
pub trait RemoteWorkflowOfferSink: Debug + Send + Sync {
    /// One attempt, including an optional trusted placement refresh. No endpoint
    /// from a response may be followed. The caller enforces the whole attempt budget.
    async fn offer(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        refresh: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError>;
}

pub(crate) struct WorkflowDelivery<R> {
    repo: Arc<R>,
    registry: Arc<RwLock<Option<DeploymentRegistry>>>,
    broker: InMemoryBroker,
    homes: Arc<PlacementQueueHomes>,
    sender: RwLock<Option<sender::WorkflowOfferSender>>,
    shutdown: RuntimeShutdownHandle,
    clock: Arc<dyn Clock>,
    sweeps: sweep::Sweeps,
}
impl<R> Debug for WorkflowDelivery<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowDelivery").finish_non_exhaustive()
    }
}
impl<R: RunRepository + 'static> WorkflowDelivery<R> {
    pub(crate) fn new(
        repo: Arc<R>,
        registry: Arc<RwLock<Option<DeploymentRegistry>>>,
        broker: InMemoryBroker,
        homes: Arc<PlacementQueueHomes>,
        shutdown: RuntimeShutdownHandle,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new(Self {
            repo,
            registry,
            broker,
            homes,
            sender: RwLock::new(None),
            shutdown,
            clock,
            sweeps: sweep::Sweeps::default(),
        })
    }
    pub(crate) fn install_sender(self: &Arc<Self>, sink: Arc<dyn RemoteWorkflowOfferSink>) {
        let mut slot = self.sender.write().expect("offer sender lock poisoned");
        assert!(slot.is_none(), "offer sender is installed once at startup");
        *slot = Some(sender::WorkflowOfferSender::spawn(self.clone(), sink));
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
    fn acquisition(&self, home: ShardId) -> Option<ShardAcquisition> {
        let owner = self.homes.owner.read().expect("shard owner lock poisoned");
        owner
            .acquisition(home)
            .filter(|a| owner.acquisition_active(a))
    }
    async fn routing_target(
        &self,
        offer: &WorkflowOffer,
    ) -> Result<Option<WorkerDeploymentVersionRef>, WorkflowOfferError> {
        let registry = self
            .registry
            .read()
            .expect("deployment registry lock poisoned")
            .clone();
        let Some(registry) = registry else {
            return Ok(None);
        };
        let queue = offer
            .hint
            .sticky_origin
            .as_ref()
            .map_or(&offer.hint.task.queue, |origin| &origin.normal_queue);
        let routing = registry
            .workflow_task_routing_config(
                queue.namespace_id,
                &queue.task_queue.0,
                offer
                    .directive
                    .deployment
                    .as_ref()
                    .map(|version| version.deployment_name.as_str()),
            )
            .await
            .map_err(|_| WorkflowOfferError::Routing)?;
        if offer.directive.use_ramping_version
            && (routing.ramping_version.is_some() || routing.ramping_to_unversioned)
        {
            return Ok(routing.ramping_version.map(|v| WorkerDeploymentVersionRef {
                deployment_name: v.deployment_name.0,
                build_id: v.build_id.0,
            }));
        }
        // Use the same current/ramping selection as Tokeira's start path.
        Ok(routing_config_target(
            &routing,
            &offer.directive.workflow_id,
        ))
    }
    async fn deployment_changed(&self, offer: &WorkflowOffer) -> Result<bool, WorkflowOfferError> {
        // getPhysicalQueuesForAdd, service/matching/task_queue_partition_manager.go
        // @ v1.31.0: pinned sticky tasks return before deployment comparison;
        // assigned builds bypass it, and a nil target is not unavailability.
        let target = self.routing_target(offer).await?;
        Ok(offer.directive.behavior != VersioningBehavior::Pinned
            && offer
                .directive
                .assigned_build_id
                .as_ref()
                .is_none_or(|id| id.0.is_empty())
            && target.is_some()
            && offer.directive.deployment != target)
    }
    async fn sticky_reason(
        &self,
        offer: &WorkflowOffer,
        published_deployment_change: Option<bool>,
    ) -> Result<Option<StickyUnavailableReason>, WorkflowOfferError> {
        // AddWorkflowTask checks stickyWorkerAvailable before pm.AddTask reaches
        // deployment routing (service/matching/matching_engine.go @ v1.31.0).
        // Missing pollers therefore remain unavailable when policy reads fail.
        let origin = offer
            .hint
            .sticky_origin
            .as_ref()
            .expect("sticky offer has origin");
        if !self
            .broker
            .sticky_available(&origin.sticky_queue, &origin.worker)
            .await
        {
            return Ok(Some(StickyUnavailableReason::NoRecentPoller));
        }
        // The lane just resolved policy from committed intent. Pushed offers and
        // later sender attempts must instead use this receiver's current view.
        let changed = match published_deployment_change {
            Some(changed) => changed,
            None => self.deployment_changed(offer).await?,
        };
        Ok(changed.then_some(StickyUnavailableReason::DeploymentChanged))
    }
    /// Admit from the receiving home's current view without entering a start lane.
    pub(crate) async fn receive(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        source: OfferSource,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        self.receive_with_policy(offer, destination, source, None)
            .await
    }
    async fn receive_with_policy(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        source: OfferSource,
        published_deployment_change: Option<bool>,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        if self.destination(&offer.hint.task.queue) != destination {
            return Ok(WorkflowOfferOutcome::NotHome);
        }
        let Some(home) = self.homes.local_home(&offer.hint.task.queue) else {
            return Ok(WorkflowOfferOutcome::NotHome);
        };
        if offer
            .hint
            .task
            .sticky_deadline
            .is_some_and(|deadline| deadline <= self.clock.now())
        {
            return Ok(WorkflowOfferOutcome::Expired);
        }
        let mut retarget = false;
        let mut unavailable = None;
        if offer.hint.task.sticky_preferred.is_some() {
            unavailable = self
                .sticky_reason(offer, published_deployment_change)
                .await?;
        } else if let Some(origin) = &offer.hint.sticky_origin {
            // A peer cannot evict a retained sticky revision by arriving later.
            // Only this home's own current decision authorizes replacement.
            if self.destination(&origin.sticky_queue) == destination {
                retarget = self
                    .sticky_reason(offer, published_deployment_change)
                    .await?
                    .is_some();
            }
        }
        if !home.valid()
            || self
                .homes
                .local_home(&offer.hint.task.queue)
                .is_none_or(|current| current.generation != home.generation)
        {
            return Ok(WorkflowOfferOutcome::NotHome);
        }
        if offer
            .hint
            .task
            .sticky_deadline
            .is_some_and(|deadline| deadline <= self.clock.now())
        {
            return Ok(WorkflowOfferOutcome::Expired);
        }
        if let Some(reason) = unavailable {
            return Ok(WorkflowOfferOutcome::StickyUnavailable(reason));
        }
        let result = self
            .broker
            .admit_workflow_offer_at_home(
                offer.hint.task.clone(),
                source,
                Some(offer.hint.scheduled_at),
                None,
                retarget,
                Some(offer.hint.execution_home),
            )
            .await;
        Ok(match result {
            OfferAdmission::Admitted => WorkflowOfferOutcome::Admitted,
            OfferAdmission::Known => WorkflowOfferOutcome::Known,
            OfferAdmission::Full => WorkflowOfferOutcome::Full,
            OfferAdmission::NotHome => WorkflowOfferOutcome::NotHome,
            OfferAdmission::Expired => WorkflowOfferOutcome::Expired,
        })
    }
    async fn fallback(
        &self,
        mut offer: WorkflowOffer,
        source: PublicationSource,
    ) -> Result<Option<WorkflowOffer>, WorkflowOfferError> {
        let origin = offer
            .hint
            .sticky_origin
            .as_ref()
            .expect("sticky fallback has origin");
        let LoadedRun::Existing(state) = tokeira_storage::read_probe::scope(
            match source {
                PublicationSource::Publication => "publication-fallback",
                PublicationSource::Recovery => "sticky-recovery-fallback",
            },
            self.repo.load_run(offer.hint.task.run_key),
        )
        .await
        .map_err(|_| WorkflowOfferError::Routing)?
        else {
            return Ok(None);
        };
        let Some(row) = derive_workflow_dispatch(&state, offer.hint.execution_home) else {
            return Ok(None);
        };
        if row.incarnation.logical_seq != offer.hint.task.logical_seq {
            return Ok(None);
        }
        let mut queue = origin.normal_queue.clone();
        if matches!(offer.hint.routing, WorkflowDispatchRouting::Live) {
            queue.deployment = None;
            queue.build_id = None;
            let registry = self
                .registry
                .read()
                .expect("deployment registry lock poisoned")
                .clone();
            queue = fallback_target(registry.as_ref(), &state, queue)
                .await
                .map_err(|_| WorkflowOfferError::Routing)?;
        }
        offer.hint.task.queue = queue;
        offer.hint.task.sticky_preferred = None;
        offer.hint.task.normal_queue = None;
        // History retries with the sticky TTL; arrival and fallback do not renew it
        // (service/history/transfer_queue_active_task_executor.go @ v1.31.0).
        offer.hint.task.sticky_deadline = Some(origin.deadline);
        Ok(Some(offer))
    }
    /// Deliver committed intent, reusing the publisher's normal target locally.
    /// A sticky offer must carry that target; sender retries resolve it afresh off-lane.
    pub(crate) async fn publish(&self, offer: WorkflowOffer, local_fallback: Option<QueueKey>) {
        let Some(acquisition) = self.acquisition(offer.hint.execution_home) else {
            sender::record(PublicationSource::Publication, "ownership");
            return;
        };
        let destination = self.destination(&offer.hint.task.queue);
        let sender = self
            .sender
            .read()
            .expect("offer sender lock poisoned")
            .clone();
        if self.homes.local_home(&offer.hint.task.queue).is_some() {
            let started = tokio::time::Instant::now();
            let result = tokio::time::timeout(
                sender::ATTEMPT,
                self.receive_with_policy(
                    &offer,
                    destination,
                    OfferSource::Local,
                    Some(
                        offer.hint.sticky_origin.is_some()
                            && offer.hint.task.sticky_preferred.is_none(),
                    ),
                ),
            )
            .await;
            sender::record_result(
                PublicationSource::Publication,
                result
                    .as_ref()
                    .map(|r| *r)
                    .unwrap_or(Err(WorkflowOfferError::Deadline)),
            );
            if let Ok(Ok(WorkflowOfferOutcome::StickyUnavailable(reason))) = result {
                sender::record_sticky(reason);
                if let Some(sender) = sender {
                    sender.try_enqueue(
                        offer,
                        acquisition,
                        PublicationSource::Publication,
                        destination,
                        started,
                        true,
                        self.clock.now(),
                    );
                } else {
                    // Exclusive modes have no sender or transport. Their original
                    // normal destination is local, under the same absolute budget.
                    let _ = tokio::time::timeout_at(started + sender::TOTAL, async {
                        if let Some(queue) = local_fallback {
                            let mut fallback = offer;
                            fallback.hint.task.queue = queue;
                            fallback.hint.task.sticky_preferred = None;
                            fallback.hint.task.normal_queue = None;
                            // Origin and its absolute deadline came from committed intent.
                            let result = self
                                .receive_with_policy(
                                    &fallback,
                                    self.destination(&fallback.hint.task.queue),
                                    OfferSource::Local,
                                    Some(false),
                                )
                                .await;
                            sender::record_result(PublicationSource::Publication, result);
                        }
                    })
                    .await;
                }
            }
        } else if let Some(sender) = sender {
            sender.try_enqueue(
                offer,
                acquisition,
                PublicationSource::Publication,
                destination,
                tokio::time::Instant::now(),
                false,
                self.clock.now(),
            );
        }
    }
}
