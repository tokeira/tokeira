//! Offer wire translation and purpose-authenticated, trusted-placement transport.
//! Runtime owns sticky decisions; this adapter never reads or mutates run state.

use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use time::OffsetDateTime;
use tokeira_kernel::{VersioningBehavior, WorkerDeploymentVersionRef};
use tokeira_proto::internal::runtime as wire;
use tokeira_runtime::{
    RemoteWorkflowOfferSink, StickyUnavailableReason, WorkflowOffer, WorkflowOfferDirective,
    WorkflowOfferError, WorkflowOfferOutcome,
};
use tokeira_storage::{
    DispatchableWorkflowTask, StickyWorkflowOfferOrigin, WorkflowDispatchRouting, WorkflowOfferHint,
};
use tokeira_types::{
    BuildId, DeploymentId, LogicalTaskSeq, NamespaceId, QueueKey, RunKey, ShardId, TaskKind,
    TaskQueueName, WorkerIdentity, WorkflowId,
};
use tonic::Status;
use uuid::Uuid;

use super::{
    OFFER_TIMEOUT, PeerOfferEndpoint, invalid, offer_client, timestamp, validate_request,
    validate_response,
};
use crate::{
    RoutingCache,
    grpc::peer_transport::{PeerChannels, PeerConnector},
    routing_cache::refresh_bundle_from_controller,
};

fn encode_queue(queue: &QueueKey) -> wire::OfferQueue {
    wire::OfferQueue {
        name: queue.task_queue.0.clone(),
        deployment: queue.deployment.as_ref().map(|v| v.0.clone()),
        build_id: queue.build_id.as_ref().map(|v| v.0.clone()),
    }
}
fn decode_queue(queue: &wire::OfferQueue, namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(queue.name.clone()),
        task_kind: TaskKind::Workflow,
        deployment: queue.deployment.clone().map(DeploymentId),
        build_id: queue.build_id.clone().map(BuildId),
    }
}
fn encode_time(time: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: time.unix_timestamp(),
        nanos: time.nanosecond() as i32,
    }
}
fn decode_time(time: Option<&prost_types::Timestamp>) -> Result<OffsetDateTime, Status> {
    let (seconds, nanos) = timestamp(time)?;
    OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(seconds) * 1_000_000_000 + i128::from(nanos),
    )
    .map_err(|_| invalid())
}

pub(crate) fn decode(value: &wire::OfferWorkflowTaskRequest) -> Result<WorkflowOffer, Status> {
    validate_request(value)?;
    let namespace = NamespaceId(Uuid::from_slice(&value.namespace_id).map_err(|_| invalid())?);
    let selected = decode_queue(
        value.selected_queue.as_ref().ok_or_else(invalid)?,
        namespace,
    );
    let sticky = value.queue_kind == wire::OfferQueueKind::Sticky as i32;
    let origin = value
        .sticky_origin
        .as_ref()
        .map(|origin| {
            Ok::<_, Status>(StickyWorkflowOfferOrigin {
                sticky_queue: decode_queue(
                    origin.sticky_queue.as_ref().ok_or_else(invalid)?,
                    namespace,
                ),
                normal_queue: decode_queue(
                    origin.normal_queue.as_ref().ok_or_else(invalid)?,
                    namespace,
                ),
                worker: WorkerIdentity(origin.worker_identity.clone()),
                deadline: decode_time(origin.deadline.as_ref())?,
            })
        })
        .transpose()?;
    let routing = value.routing.as_ref().ok_or_else(invalid)?;
    let directive = value.version_directive.as_ref().ok_or_else(invalid)?;
    Ok(WorkflowOffer {
        hint: WorkflowOfferHint {
            task: DispatchableWorkflowTask {
                run_key: RunKey(Uuid::from_slice(&value.run_key).map_err(|_| invalid())?),
                logical_seq: LogicalTaskSeq(value.logical_seq),
                queue: selected,
                sticky_preferred: origin.as_ref().filter(|_| sticky).map(|o| o.worker.clone()),
                normal_queue: origin
                    .as_ref()
                    .filter(|_| sticky)
                    .map(|o| o.normal_queue.clone()),
                sticky_deadline: origin.as_ref().map(|o| o.deadline),
                priority: value.priority.as_ref().map(|p| tokeira_kernel::Priority {
                    priority_key: p.priority_key,
                    fairness_key: p.fairness_key.clone(),
                    fairness_weight: p.fairness_weight,
                }),
                order: None,
            },
            execution_home: ShardId(value.execution_home),
            scheduled_at: decode_time(value.scheduled_at.as_ref())?,
            routing: if routing.mode == wire::OfferRoutingMode::Live as i32 {
                WorkflowDispatchRouting::Live
            } else {
                WorkflowDispatchRouting::Exact {
                    deployment: DeploymentId(routing.deployment.clone().ok_or_else(invalid)?),
                    build_id: routing.build_id.clone().map(BuildId),
                }
            },
            sticky_origin: origin,
        },
        directive: WorkflowOfferDirective {
            workflow_id: WorkflowId(value.workflow_id.clone()),
            behavior: match wire::OfferVersionBehavior::try_from(directive.behavior)
                .map_err(|_| invalid())?
            {
                wire::OfferVersionBehavior::Unversioned => VersioningBehavior::Unspecified,
                wire::OfferVersionBehavior::Pinned => VersioningBehavior::Pinned,
                wire::OfferVersionBehavior::AutoUpgrade => VersioningBehavior::AutoUpgrade,
                _ => return Err(invalid()),
            },
            deployment: directive
                .deployment
                .as_ref()
                .zip(directive.build_id.as_ref())
                .map(|(d, b)| WorkerDeploymentVersionRef {
                    deployment_name: d.clone(),
                    build_id: b.clone(),
                }),
            assigned_build_id: directive.assigned_build_id.clone().map(BuildId),
            use_ramping_version: value.use_ramping_version,
        },
    })
}
fn encode(offer: &WorkflowOffer, destination: ShardId) -> wire::OfferWorkflowTaskRequest {
    let hint = &offer.hint;
    let (mode, deployment, build_id) = match &hint.routing {
        WorkflowDispatchRouting::Live => (wire::OfferRoutingMode::Live, None, None),
        WorkflowDispatchRouting::Exact {
            deployment,
            build_id,
        } => (
            wire::OfferRoutingMode::Exact,
            Some(deployment.0.clone()),
            build_id.as_ref().map(|b| b.0.clone()),
        ),
    };
    wire::OfferWorkflowTaskRequest {
        run_key: hint.task.run_key.0.as_bytes().to_vec(),
        logical_seq: hint.task.logical_seq.0,
        execution_home: hint.execution_home.0,
        destination_home: destination.0,
        namespace_id: hint.task.queue.namespace_id.0.as_bytes().to_vec(),
        selected_queue: Some(encode_queue(&hint.task.queue)),
        queue_kind: if hint.task.sticky_preferred.is_some() {
            wire::OfferQueueKind::Sticky
        } else {
            wire::OfferQueueKind::Normal
        } as i32,
        routing: Some(wire::OfferRouting {
            mode: mode as i32,
            deployment,
            build_id,
        }),
        version_directive: Some(wire::OfferVersionDirective {
            behavior: match offer.directive.behavior {
                VersioningBehavior::Pinned => wire::OfferVersionBehavior::Pinned,
                VersioningBehavior::AutoUpgrade => wire::OfferVersionBehavior::AutoUpgrade,
                _ => wire::OfferVersionBehavior::Unversioned,
            } as i32,
            deployment: offer
                .directive
                .deployment
                .as_ref()
                .map(|v| v.deployment_name.clone()),
            build_id: offer
                .directive
                .deployment
                .as_ref()
                .map(|v| v.build_id.clone()),
            assigned_build_id: offer
                .directive
                .assigned_build_id
                .as_ref()
                .map(|b| b.0.clone()),
        }),
        scheduled_at: Some(encode_time(hint.scheduled_at)),
        priority: hint.task.priority.as_ref().map(|p| wire::OfferPriority {
            priority_key: p.priority_key,
            fairness_key: p.fairness_key.clone(),
            fairness_weight: p.fairness_weight,
        }),
        sticky_origin: hint
            .sticky_origin
            .as_ref()
            .map(|o| wire::OfferStickyOrigin {
                sticky_queue: Some(encode_queue(&o.sticky_queue)),
                normal_queue: Some(encode_queue(&o.normal_queue)),
                worker_identity: o.worker.0.clone(),
                deadline: Some(encode_time(o.deadline)),
            }),
        workflow_id: offer.directive.workflow_id.0.clone(),
        use_ramping_version: offer.directive.use_ramping_version,
    }
}
pub(crate) fn response(outcome: WorkflowOfferOutcome) -> wire::OfferWorkflowTaskResponse {
    let (outcome, reason) = match outcome {
        WorkflowOfferOutcome::Admitted => (wire::OfferOutcome::Admitted, None),
        WorkflowOfferOutcome::Known => (wire::OfferOutcome::Known, None),
        WorkflowOfferOutcome::NotHome => (wire::OfferOutcome::NotHome, None),
        WorkflowOfferOutcome::Full => (wire::OfferOutcome::Full, None),
        WorkflowOfferOutcome::Expired => (wire::OfferOutcome::Expired, None),
        WorkflowOfferOutcome::StickyUnavailable(reason) => (
            wire::OfferOutcome::StickyUnavailable,
            Some(match reason {
                StickyUnavailableReason::NoRecentPoller => {
                    wire::OfferStickyUnavailableReason::NoRecentPoller
                }
                StickyUnavailableReason::DeploymentChanged => {
                    wire::OfferStickyUnavailableReason::DeploymentChanged
                }
            } as i32),
        ),
    };
    wire::OfferWorkflowTaskResponse {
        outcome: outcome as i32,
        sticky_unavailable_reason: reason,
    }
}
fn outcome(
    value: &wire::OfferWorkflowTaskResponse,
) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
    validate_response(value).map_err(|_| WorkflowOfferError::Invalid)?;
    Ok(
        match wire::OfferOutcome::try_from(value.outcome)
            .map_err(|_| WorkflowOfferError::Invalid)?
        {
            wire::OfferOutcome::Admitted => WorkflowOfferOutcome::Admitted,
            wire::OfferOutcome::Known => WorkflowOfferOutcome::Known,
            wire::OfferOutcome::NotHome => WorkflowOfferOutcome::NotHome,
            wire::OfferOutcome::Full => WorkflowOfferOutcome::Full,
            wire::OfferOutcome::Expired => WorkflowOfferOutcome::Expired,
            wire::OfferOutcome::StickyUnavailable => {
                WorkflowOfferOutcome::StickyUnavailable(match value.sticky_unavailable_reason {
                    Some(1) => StickyUnavailableReason::NoRecentPoller,
                    Some(2) => StickyUnavailableReason::DeploymentChanged,
                    // Legacy reason numbers stay reserved on the wire. A current peer
                    // cannot produce them while the build-id rule APIs are disabled.
                    _ => return Err(WorkflowOfferError::Invalid),
                })
            }
            _ => return Err(WorkflowOfferError::Invalid),
        },
    )
}
pub(crate) fn status(error: WorkflowOfferError) -> Status {
    match error {
        WorkflowOfferError::Invalid => invalid(),
        WorkflowOfferError::Authentication => {
            Status::unauthenticated("workflow offer authentication failed")
        }
        WorkflowOfferError::Capacity => {
            Status::resource_exhausted("workflow offer capacity exceeded")
        }
        WorkflowOfferError::Deadline => {
            Status::deadline_exceeded("workflow offer deadline exceeded")
        }
        WorkflowOfferError::Routing | WorkflowOfferError::Transport => {
            Status::unavailable("workflow offer routing or transport unavailable")
        }
    }
}
fn failure(status: Status) -> WorkflowOfferError {
    match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            WorkflowOfferError::Authentication
        }
        tonic::Code::ResourceExhausted => WorkflowOfferError::Capacity,
        tonic::Code::DeadlineExceeded => WorkflowOfferError::Deadline,
        tonic::Code::InvalidArgument => WorkflowOfferError::Invalid,
        _ => WorkflowOfferError::Transport,
    }
}

/// Offer-only client installed with the validated peer capability at startup.
#[derive(Debug)]
pub struct PeerWorkflowOfferSink {
    routing: Arc<RoutingCache>,
    controller: String,
    peer: Arc<PeerOfferEndpoint>,
    channels: PeerChannels,
    destinations: Arc<Mutex<HashMap<String, usize>>>,
}
impl PeerWorkflowOfferSink {
    /// Reuse the shared placement subscription and validated signing material.
    pub fn new(
        routing: Arc<RoutingCache>,
        controller: String,
        peer: Arc<PeerOfferEndpoint>,
    ) -> Self {
        Self {
            routing,
            controller,
            peer,
            channels: PeerChannels::default(),
            destinations: Arc::default(),
        }
    }
    fn prepare(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
    ) -> Result<PreparedOffer, WorkflowOfferError> {
        let snapshot = self.routing.snapshot();
        let owner = snapshot
            .lookup_bundle_owner(destination)
            .ok_or(WorkflowOfferError::Routing)?;
        let endpoint = snapshot
            .lookup_node_endpoint(owner.node_id)
            .ok_or(WorkflowOfferError::Routing)?;
        let authority = endpoint.as_authority();
        // Several homes can share a peer. Keep the per-peer cap in addition to
        // the scheduler's per-home fairness/cap, and retain only active entries.
        let mut destinations = self
            .destinations
            .lock()
            .expect("offer destination lock poisoned");
        let active = destinations.entry(authority.clone()).or_default();
        if *active >= 8 {
            return Err(WorkflowOfferError::Capacity);
        }
        *active += 1;
        drop(destinations);
        let slot = DestinationSlot {
            destinations: self.destinations.clone(),
            authority: authority.clone(),
        };
        let channel = self
            .channels
            .get(&authority)
            .map_err(|_| WorkflowOfferError::Transport)?;
        let now = self
            .peer
            .unix_seconds()
            .map_err(|_| WorkflowOfferError::Authentication)?;
        let token = self
            .peer
            .signer()
            .token_at(now)
            .map_err(|_| WorkflowOfferError::Authentication)?;
        let value = encode(offer, destination);
        validate_request(&value).map_err(failure)?;
        let mut request = tonic::Request::new(value);
        request.set_timeout(OFFER_TIMEOUT);
        request.metadata_mut().insert(
            "authorization",
            token
                .authorization()
                .parse()
                .map_err(|_| WorkflowOfferError::Authentication)?,
        );
        Ok(PreparedOffer {
            channel,
            request,
            _slot: slot,
        })
    }

    /// Reach peers through the engine's connector, including in-process harnesses.
    pub fn with_peer_connector(mut self, connector: PeerConnector) -> Self {
        self.channels = PeerChannels::with_connector(connector);
        self
    }
}
#[async_trait]
impl RemoteWorkflowOfferSink for PeerWorkflowOfferSink {
    async fn offer(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        refresh: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        tokio::time::timeout(OFFER_TIMEOUT, async {
            if refresh {
                refresh_bundle_from_controller(&self.routing, self.controller.clone(), destination)
                    .await
                    .map_err(|_| WorkflowOfferError::Routing)?;
            }
            let prepared = self.prepare(offer, destination)?;
            let response = offer_client(prepared.channel)
                .offer_workflow_task(prepared.request)
                .await
                .map_err(failure)?;
            outcome(response.get_ref())
        })
        .await
        .map_err(|_| WorkflowOfferError::Deadline)?
    }
}

struct DestinationSlot {
    destinations: Arc<Mutex<HashMap<String, usize>>>,
    authority: String,
}
impl Drop for DestinationSlot {
    fn drop(&mut self) {
        let mut destinations = self
            .destinations
            .lock()
            .expect("offer destination lock poisoned");
        let count = destinations
            .get_mut(&self.authority)
            .expect("attempt owns destination slot");
        *count -= 1;
        if *count == 0 {
            destinations.remove(&self.authority);
        }
    }
}
struct PreparedOffer {
    channel: tonic::transport::Channel,
    request: tonic::Request<wire::OfferWorkflowTaskRequest>,
    _slot: DestinationSlot,
}

#[cfg(test)]
#[path = "delivery_tests.rs"]
mod tests;
