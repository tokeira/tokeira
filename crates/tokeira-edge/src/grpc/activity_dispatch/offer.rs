//! Activity hint wire policy and push transport. This path never starts work.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use prost::Message as _;
use time::OffsetDateTime;
use tokeira_kernel::Priority;
use tokeira_proto::internal::runtime::{
    self as wire, activity_task_dispatch_client::ActivityTaskDispatchClient,
};
use tokeira_runtime::{
    ActivityOfferError, ActivityOfferOutcome, ActivityQueueOffer, RemoteActivityOfferSink,
};
use tokeira_storage::DispatchableActivityTask;
use tokeira_types::{
    BuildId, DeploymentId, NamespaceId, QueueKey, RunKey, ShardId, TaskKind, TaskQueueName,
};
use tonic::{Code, Status};
use uuid::Uuid;

use super::super::{
    peer_transport::{PeerChannels, PeerConnector},
    workflow_offer::{
        MAX_OFFER_REQUEST_BYTES, MAX_OFFER_RESPONSE_BYTES, OFFER_TIMEOUT, OfferResponseLimit,
        PeerOfferEndpoint,
    },
};
use crate::{RoutingCache, routing_cache::refresh_bundle_from_controller};

pub(crate) const OFFER_PATH: &str =
    "/tokeira.internal.runtime.v1.ActivityTaskDispatch/OfferActivityTask";

pub(crate) fn record_failure(status: &Status) {
    metrics::counter!(tokeira_runtime::metrics::ACTIVITY_OFFER_DELIVERY_TOTAL, "source" => "pushed", "outcome" => match status.code() {
        Code::Unauthenticated | Code::PermissionDenied => "authentication",
        Code::ResourceExhausted => "capacity", Code::DeadlineExceeded => "deadline",
        Code::InvalidArgument => "invalid", Code::Unimplemented => "unimplemented", _ => "transport",
    }).increment(1);
}

fn invalid() -> Status {
    Status::invalid_argument("invalid activity offer coordinates")
}

fn decode_time(value: Option<&prost_types::Timestamp>) -> Result<OffsetDateTime, Status> {
    let value = value.ok_or_else(invalid)?;
    if !(0..1_000_000_000).contains(&value.nanos)
        || !(-62_135_596_800..=253_402_300_799).contains(&value.seconds)
    {
        return Err(invalid());
    }
    OffsetDateTime::from_unix_timestamp(value.seconds)
        .and_then(|v| v.replace_nanosecond(value.nanos as u32))
        .map_err(|_| invalid())
}
fn encode_time(value: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

pub(super) fn decode(
    value: &wire::OfferActivityTaskRequest,
    homes: u32,
) -> Result<ActivityQueueOffer, Status> {
    if value.encoded_len() > MAX_OFFER_REQUEST_BYTES {
        return Err(Status::resource_exhausted("activity offer exceeds 64 KiB"));
    }
    let run = Uuid::from_slice(&value.run_key).map_err(|_| invalid())?;
    let namespace = Uuid::from_slice(&value.namespace_id).map_err(|_| invalid())?;
    let queue = value.selected_queue.as_ref().ok_or_else(invalid)?;
    let dispatch_at = decode_time(value.dispatch_at.as_ref())?;
    let expires_at = decode_time(value.expires_at.as_ref())?;
    if run.is_nil()
        || namespace.is_nil()
        || value.activity_id.is_empty()
        || value.schedule_event_id <= 0
        || value.attempt == 0
        || value.execution_home >= homes
        || value.destination_home >= homes
        || queue.name.is_empty()
        || expires_at <= dispatch_at
        || value
            .priority
            .as_ref()
            .is_some_and(|p| !p.fairness_weight.is_finite() || p.fairness_weight < 0.0)
    {
        return Err(invalid());
    }
    Ok(ActivityQueueOffer {
        task: DispatchableActivityTask {
            run_key: RunKey(run),
            activity_id: value.activity_id.clone(),
            schedule_event_id: value.schedule_event_id,
            attempt: value.attempt,
            stamp: value.stamp,
            dispatch_revision: value.dispatch_revision,
            queue: QueueKey {
                namespace_id: NamespaceId(namespace),
                task_queue: TaskQueueName(queue.name.clone()),
                task_kind: TaskKind::Activity,
                deployment: queue.deployment.clone().map(DeploymentId),
                build_id: queue.build_id.clone().map(BuildId),
            },
            priority: value.priority.as_ref().map(|p| Priority {
                priority_key: p.priority_key,
                fairness_key: p.fairness_key.clone(),
                fairness_weight: p.fairness_weight,
            }),
            order: None,
        },
        execution_home: ShardId(value.execution_home),
        dispatch_at,
        expires_at,
    })
}

pub(super) fn encode(
    offer: &ActivityQueueOffer,
    destination: ShardId,
) -> wire::OfferActivityTaskRequest {
    let task = &offer.task;
    wire::OfferActivityTaskRequest {
        run_key: task.run_key.0.as_bytes().to_vec(),
        activity_id: task.activity_id.clone(),
        schedule_event_id: task.schedule_event_id,
        attempt: task.attempt,
        stamp: task.stamp,
        execution_home: offer.execution_home.0,
        destination_home: destination.0,
        namespace_id: task.queue.namespace_id.0.as_bytes().to_vec(),
        selected_queue: Some(wire::OfferQueue {
            name: task.queue.task_queue.0.clone(),
            deployment: task.queue.deployment.as_ref().map(|v| v.0.clone()),
            build_id: task.queue.build_id.as_ref().map(|v| v.0.clone()),
        }),
        dispatch_revision: task.dispatch_revision,
        dispatch_at: Some(encode_time(offer.dispatch_at)),
        expires_at: Some(encode_time(offer.expires_at)),
        priority: task.priority.as_ref().map(|p| wire::OfferPriority {
            priority_key: p.priority_key,
            fairness_key: p.fairness_key.clone(),
            fairness_weight: p.fairness_weight,
        }),
    }
}

pub(super) fn response(outcome: ActivityOfferOutcome) -> wire::OfferActivityTaskResponse {
    wire::OfferActivityTaskResponse {
        outcome: match outcome {
            ActivityOfferOutcome::Admitted => wire::ActivityOfferOutcome::Admitted,
            ActivityOfferOutcome::Known => wire::ActivityOfferOutcome::Known,
            ActivityOfferOutcome::NotHome => wire::ActivityOfferOutcome::NotHome,
            ActivityOfferOutcome::Full => wire::ActivityOfferOutcome::Full,
            ActivityOfferOutcome::Expired => wire::ActivityOfferOutcome::Expired,
            ActivityOfferOutcome::NotDue => wire::ActivityOfferOutcome::NotDue,
        } as i32,
    }
}
pub(super) fn outcome(
    reply: &wire::OfferActivityTaskResponse,
) -> Result<ActivityOfferOutcome, ActivityOfferError> {
    if reply.encoded_len() > MAX_OFFER_RESPONSE_BYTES {
        return Err(ActivityOfferError::Capacity);
    }
    match wire::ActivityOfferOutcome::try_from(reply.outcome) {
        Ok(wire::ActivityOfferOutcome::Admitted) => Ok(ActivityOfferOutcome::Admitted),
        Ok(wire::ActivityOfferOutcome::Known) => Ok(ActivityOfferOutcome::Known),
        Ok(wire::ActivityOfferOutcome::NotHome) => Ok(ActivityOfferOutcome::NotHome),
        Ok(wire::ActivityOfferOutcome::Full) => Ok(ActivityOfferOutcome::Full),
        Ok(wire::ActivityOfferOutcome::Expired) => Ok(ActivityOfferOutcome::Expired),
        Ok(wire::ActivityOfferOutcome::NotDue) => Ok(ActivityOfferOutcome::NotDue),
        _ => Err(ActivityOfferError::Invalid),
    }
}
fn failure(status: Status) -> ActivityOfferError {
    match status.code() {
        Code::Unimplemented => ActivityOfferError::Unimplemented,
        Code::Unauthenticated | Code::PermissionDenied => ActivityOfferError::Authentication,
        Code::ResourceExhausted | Code::OutOfRange => ActivityOfferError::Capacity,
        Code::InvalidArgument => ActivityOfferError::Invalid,
        Code::DeadlineExceeded => ActivityOfferError::Deadline,
        _ => ActivityOfferError::Transport,
    }
}

/// Trusted placement and existing push credentials, with separate activity capacity.
#[derive(Debug)]
pub struct PeerActivityOfferSink {
    routing: Arc<RoutingCache>,
    controller: String,
    peer: Arc<PeerOfferEndpoint>,
    channels: PeerChannels,
    destinations: Arc<Mutex<HashMap<String, usize>>>,
}
impl PeerActivityOfferSink {
    /// Construct only after peer bootstrap validates the existing credential file.
    pub fn new(
        routing: Arc<RoutingCache>,
        controller: String,
        peer: Arc<PeerOfferEndpoint>,
    ) -> Self {
        Self {
            routing,
            controller,
            peer,
            channels: Default::default(),
            destinations: Arc::default(),
        }
    }
    /// Reach peers through the engine connector, including authenticated in-process tests.
    pub fn with_peer_connector(mut self, connector: PeerConnector) -> Self {
        self.channels = PeerChannels::with_connector(connector);
        self
    }
}
#[async_trait]
impl RemoteActivityOfferSink for PeerActivityOfferSink {
    async fn offer(
        &self,
        offer: &ActivityQueueOffer,
        destination: ShardId,
        refresh: bool,
    ) -> Result<ActivityOfferOutcome, ActivityOfferError> {
        tokio::time::timeout(OFFER_TIMEOUT, async {
            if refresh {
                refresh_bundle_from_controller(&self.routing, self.controller.clone(), destination)
                    .await
                    .map_err(|_| ActivityOfferError::Routing)?;
            }
            let snapshot = self.routing.snapshot();
            let owner = snapshot
                .lookup_bundle_owner(destination)
                .ok_or(ActivityOfferError::Routing)?;
            let authority = snapshot
                .lookup_node_endpoint(owner.node_id)
                .ok_or(ActivityOfferError::Routing)?
                .as_authority();
            let _slot = {
                let mut destinations = self
                    .destinations
                    .lock()
                    .expect("activity destination lock poisoned");
                let count = destinations.entry(authority.clone()).or_default();
                if *count >= 8 {
                    return Err(ActivityOfferError::Capacity);
                }
                *count += 1;
                DestinationSlot {
                    destinations: self.destinations.clone(),
                    authority: authority.clone(),
                }
            };
            let channel = self
                .channels
                .get(&authority)
                .map_err(|_| ActivityOfferError::Transport)?;
            let now = self
                .peer
                .unix_seconds()
                .map_err(|_| ActivityOfferError::Authentication)?;
            let token = self
                .peer
                .signer()
                .token_at(now)
                .map_err(|_| ActivityOfferError::Authentication)?;
            let body = encode(offer, destination);
            if body.encoded_len() > MAX_OFFER_REQUEST_BYTES {
                return Err(ActivityOfferError::Capacity);
            }
            let mut request = tonic::Request::new(body);
            request.set_timeout(OFFER_TIMEOUT);
            request.metadata_mut().insert(
                "authorization",
                token
                    .authorization()
                    .parse()
                    .map_err(|_| ActivityOfferError::Authentication)?,
            );
            let reply = ActivityTaskDispatchClient::new(OfferResponseLimit::new(channel))
                .max_encoding_message_size(MAX_OFFER_REQUEST_BYTES)
                .max_decoding_message_size(MAX_OFFER_RESPONSE_BYTES)
                .offer_activity_task(request)
                .await
                .map_err(failure)?;
            outcome(reply.get_ref())
        })
        .await
        .map_err(|_| ActivityOfferError::Deadline)?
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
            .expect("activity destination lock poisoned");
        let count = destinations
            .get_mut(&self.authority)
            .expect("attempt owns destination slot");
        *count -= 1;
        if *count == 0 {
            destinations.remove(&self.authority);
        }
    }
}
