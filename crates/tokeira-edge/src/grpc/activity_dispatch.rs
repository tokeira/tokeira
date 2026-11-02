//! Activity matches cross homes as bounded, purpose-authenticated protobufs.
//!
//! The owner repeats worker admission and projects the exact success before
//! commit. Native payload fields stay binary. A retry retains the match UUID;
//! neither a reply nor its routing coordinates can authorize another task.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use http::HeaderMap;
use prost::Message as _;
use time::OffsetDateTime;
use tokeira_kernel::Priority;
use tokeira_proto::{
    conversions::common::{headers_to_domain, payloads_to_domain},
    internal::runtime::{
        self as wire,
        activity_task_dispatch_client::ActivityTaskDispatchClient,
        activity_task_dispatch_server::{ActivityTaskDispatch, ActivityTaskDispatchServer},
    },
};
use tokeira_runtime::{
    ActivityStartOutcome, ActivityTaskStartRequest, ActivityTaskStarter,
    RetryableActivityTaskStart, StartedActivityTask, TokeiraRuntime,
};
use tokeira_storage::{DispatchableActivityTask, RunRepository};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, NamespaceId, QueueKey, RetryPolicy, RunId, RunKey,
    ShardId, TaskKind, TaskQueueName, WorkerIdentity, WorkerTaskClass, WorkerTaskOrigin,
};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

use crate::{
    Action, EdgeContext, EdgeInterceptors, RoutingCache,
    interceptors::normalize_worker_poll_target,
    workflow_service::{resolved_namespace_id, scoped_worker_allows_task_origin},
};

const MAX_REQUEST: usize = 64 * 1024;
pub(super) const MAX_RESPONSE: usize = 128 * 1024 * 1024;
pub(super) const START_PATH: &str =
    "/tokeira.internal.runtime.v1.ActivityTaskDispatch/StartActivityTask";
const MAX_METADATA: usize = 8 * 1024;
const PEER_HEADER: &str = "x-tokeira-peer-authorization";

/// Authentication material exists only for the lifetime of this admitted poll.
#[derive(Clone)]
pub(crate) struct ForwardedActivityPoll {
    pub(crate) headers: HeaderMap,
    pub(crate) namespace: String,
    pub(crate) scoped_versioned: bool,
}

tokio::task_local! {
    pub(crate) static FORWARDED_ACTIVITY_POLL: ForwardedActivityPoll;
}

/// Both local and remote RPCs install this before calling the common owner path.
pub(crate) struct ActivityResponsePreflight {
    context: EdgeContext,
    namespace: String,
    namespace_id: NamespaceId,
}

impl fmt::Debug for ActivityResponsePreflight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActivityResponsePreflight")
            .finish_non_exhaustive()
    }
}

impl ActivityResponsePreflight {
    pub(crate) fn new(
        context: EdgeContext,
        namespace: String,
        namespace_id: NamespaceId,
    ) -> Arc<Self> {
        Arc::new(Self {
            context,
            namespace,
            namespace_id,
        })
    }
}

impl tokeira_runtime::ActivityStartResponsePreflight for ActivityResponsePreflight {
    fn validate(&self, task: &StartedActivityTask) -> Result<()> {
        ensure!(
            task.origin.namespace_id == self.namespace_id,
            Status::permission_denied("activity namespace mismatch")
        );
        ensure!(
            scoped_worker_allows_task_origin(
                &self.context,
                Action::PollActivityTaskQueue,
                &task.origin
            ),
            Status::permission_denied("activity origin is outside worker scope")
        );
        if let Some(policy) = &task.retry_policy {
            i32::try_from(policy.maximum_attempts)
                .map_err(|_| tokeira_runtime::InvalidActivityStart)?;
            ensure!(
                policy.backoff_coefficient.is_finite(),
                tokeira_runtime::InvalidActivityStart
            );
        }
        for value in [
            task.schedule_to_close_timeout,
            task.start_to_close_timeout,
            task.heartbeat_timeout,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(!value.is_negative(), tokeira_runtime::InvalidActivityStart);
        }
        let response = project(task.clone(), self.namespace_id, &self.namespace)?;
        if response.encoded_len() > MAX_RESPONSE {
            return Err(tokeira_runtime::ActivityStartTooLarge.into());
        }
        Ok(())
    }
}

fn project(
    task: StartedActivityTask,
    namespace_id: NamespaceId,
    namespace: &str,
) -> Result<wire::StartActivityTaskResponse> {
    i32::try_from(task.attempt).map_err(|_| tokeira_runtime::InvalidActivityStart)?;
    let origin = wire::ActivityStartOrigin {
        namespace_id: namespace_id.0.as_bytes().to_vec(),
        normal_task_queue: task.origin.normal_task_queue.0.clone(),
        deployment: Some(task.origin.deployment.0.clone()),
        build_id: Some(task.origin.build_id.0.clone()),
    };
    let replayed = task.replayed;
    let response =
        crate::translate::from_internal::poll_activity_response(task, namespace_id, namespace)?;
    Ok(wire::StartActivityTaskResponse {
        task: Some(super::translate::poll_activity_response_to_proto(response)),
        origin: Some(origin),
        replayed,
    })
}

fn validate_request(request: &wire::StartActivityTaskRequest) -> std::result::Result<(), Status> {
    if request.encoded_len() > MAX_REQUEST {
        return Err(Status::resource_exhausted(
            "activity start request exceeds 64 KiB",
        ));
    }
    if Uuid::from_slice(&request.run_key).map_or(true, |id| id.is_nil())
        || Uuid::from_slice(&request.request_id).map_or(true, |id| id.is_nil())
        || request.activity_id.is_empty()
        || request.namespace.is_empty()
        || request.task_queue.is_empty()
        || request.schedule_event_id <= 0
        || request.attempt == 0
        || i32::try_from(request.attempt).is_err()
        || i64::try_from(request.stamp).is_err()
        || request.dispatch_revision < 0
    {
        return Err(Status::invalid_argument(
            "invalid activity start coordinates",
        ));
    }
    Ok(())
}

/// Uses the existing placement subscription; response bodies never supply routes.
#[derive(Debug)]
pub struct RemoteActivityTaskStarter {
    cache: Arc<RoutingCache>,
    channels: super::peer_transport::PeerChannels,
    peer: Arc<super::workflow_offer::PeerOfferEndpoint>,
}

impl RemoteActivityTaskStarter {
    /// Startup supplies already validated peer material and the shared directory.
    pub fn new(
        cache: Arc<RoutingCache>,
        peer: Arc<super::workflow_offer::PeerOfferEndpoint>,
    ) -> Self {
        Self {
            cache,
            peer,
            channels: Default::default(),
        }
    }
}

#[async_trait]
impl ActivityTaskStarter for RemoteActivityTaskStarter {
    async fn start(
        &self,
        home: ShardId,
        request: ActivityTaskStartRequest,
        deadline: tokio::time::Instant,
    ) -> Result<ActivityStartOutcome> {
        let context = FORWARDED_ACTIVITY_POLL
            .try_with(Clone::clone)
            .map_err(|_| anyhow!("remote activity start requires admitted poll context"))?;
        let snapshot = self.cache.snapshot();
        let owner = snapshot
            .lookup_bundle_owner(home)
            .ok_or_else(|| RetryableActivityTaskStart("execution home has no route".into()))?;
        let endpoint = snapshot
            .lookup_node_endpoint(owner.node_id)
            .ok_or_else(|| RetryableActivityTaskStart("execution owner has no endpoint".into()))?;
        let mut client =
            ActivityTaskDispatchClient::new(super::workflow_offer::OfferResponseLimit::new(
                self.channels.get(&endpoint.as_authority())?,
            ))
            .max_decoding_message_size(MAX_RESPONSE)
            .max_encoding_message_size(MAX_REQUEST);
        let offered = &request.offered;
        let body = wire::StartActivityTaskRequest {
            run_key: offered.run_key.0.as_bytes().to_vec(),
            activity_id: offered.activity_id.clone(),
            schedule_event_id: offered.schedule_event_id,
            attempt: offered.attempt,
            stamp: offered.stamp,
            request_id: request.request_id.as_bytes().to_vec(),
            namespace: context.namespace.clone(),
            task_queue: offered.queue.task_queue.0.clone(),
            deployment: offered
                .queue
                .deployment
                .as_ref()
                .map(|value| value.0.clone()),
            build_id: offered.queue.build_id.as_ref().map(|value| value.0.clone()),
            dispatch_revision: offered.dispatch_revision,
            worker_identity: request.worker.0.clone(),
            scoped_versioned: context.scoped_versioned,
        };
        validate_request(&body)?;
        let mut headers = context.headers;
        headers.remove(PEER_HEADER);
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let token = self.peer.signer().activity_start_token_at(now)?;
        headers.insert(PEER_HEADER, token.authorization().parse()?);
        let mut rpc = Request::new(body);
        *rpc.metadata_mut() = super::peer_transport::start_metadata(headers);
        let metadata_bytes =
            rpc.metadata()
                .clone()
                .into_headers()
                .iter()
                .try_fold(0usize, |size, (key, value)| {
                    size.checked_add(key.as_str().len())?
                        .checked_add(value.len())
                });
        ensure!(
            metadata_bytes.is_some_and(|size| size <= MAX_METADATA),
            Status::resource_exhausted("activity start metadata exceeds 8 KiB")
        );
        let until = deadline.min(tokio::time::Instant::now() + Duration::from_secs(10));
        let result = tokio::time::timeout_at(until, client.start_activity_task(rpc))
            .await
            .map_err(|_| RetryableActivityTaskStart("owner attempt timed out".into()))?;
        let response = match result {
            Ok(response) => response.into_inner(),
            Err(status)
                if matches!(
                    status.code(),
                    Code::Unavailable
                        | Code::DeadlineExceeded
                        | Code::Cancelled
                        | Code::Internal
                        | Code::Unknown
                        | Code::DataLoss
                ) =>
            {
                return Err(RetryableActivityTaskStart(status.to_string()).into());
            }
            Err(status) => return Err(status.into()),
        };
        decode_response(response, &request, &context.namespace)
    }
}

/// A peer-authenticated route with independent worker admission and owner fencing.
pub struct ActivityTaskDispatchGrpc<R> {
    runtime: Arc<TokeiraRuntime<R>>,
    interceptors: Arc<EdgeInterceptors>,
    peer: Option<Arc<super::workflow_offer::PeerOfferEndpoint>>,
}

impl<R> fmt::Debug for ActivityTaskDispatchGrpc<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActivityTaskDispatchGrpc")
            .finish_non_exhaustive()
    }
}

impl<R: RunRepository + 'static> ActivityTaskDispatchGrpc<R> {
    /// The gate removes this route in local/no-peer mode before body decoding.
    pub fn new(
        runtime: Arc<TokeiraRuntime<R>>,
        interceptors: Arc<EdgeInterceptors>,
        peer: Option<Arc<super::workflow_offer::PeerOfferEndpoint>>,
    ) -> Self {
        Self {
            runtime,
            interceptors,
            peer,
        }
    }

    /// Bound decoding and response encoding independently of public poll limits.
    pub fn into_service(
        self,
    ) -> super::workflow_offer::OfferGate<ActivityTaskDispatchServer<Self>> {
        let peer = self.peer.clone();
        super::workflow_offer::OfferGate::new(
            ActivityTaskDispatchServer::new(self)
                .max_decoding_message_size(MAX_REQUEST)
                .max_encoding_message_size(MAX_RESPONSE),
            peer,
        )
    }
}

#[tonic::async_trait]
impl<R: RunRepository + 'static> ActivityTaskDispatch for ActivityTaskDispatchGrpc<R> {
    async fn start_activity_task(
        &self,
        request: Request<wire::StartActivityTaskRequest>,
    ) -> std::result::Result<Response<wire::StartActivityTaskResponse>, Status> {
        if self.peer.is_none() {
            return Err(Status::unimplemented("unknown method"));
        }
        validate_request(request.get_ref())?;
        let headers = super::metadata::metadata_to_header_map(request.metadata());
        let request = request.into_inner();
        let context = self
            .interceptors
            .begin_worker_preflight(
                &headers,
                Some(&request.namespace),
                Action::PollActivityTaskQueue,
                false,
            )
            .await?;
        self.interceptors
            .authorize_worker_target(
                &context,
                Action::PollActivityTaskQueue,
                normalize_worker_poll_target(
                    &request.task_queue,
                    None,
                    false,
                    request.scoped_versioned,
                    request.deployment.as_deref(),
                    request.build_id.as_deref(),
                    WorkerTaskClass::Activity,
                ),
            )
            .await?;
        let namespace_id = resolved_namespace_id(&context, &request.namespace)?;
        let namespace = context
            .namespace
            .as_ref()
            .ok_or_else(|| Status::internal("admission returned no namespace"))?
            .name
            .clone();
        let offered = DispatchableActivityTask {
            run_key: RunKey(
                Uuid::from_slice(&request.run_key)
                    .map_err(|_| Status::invalid_argument("run_key"))?,
            ),
            queue: QueueKey {
                namespace_id,
                task_queue: TaskQueueName(request.task_queue),
                task_kind: TaskKind::Activity,
                deployment: request.deployment.map(DeploymentId),
                build_id: request.build_id.map(BuildId),
            },
            activity_id: request.activity_id,
            schedule_event_id: request.schedule_event_id,
            attempt: request.attempt,
            stamp: request.stamp,
            dispatch_revision: request.dispatch_revision,
            priority: None,
            order: None,
        };
        let request = ActivityTaskStartRequest {
            offered,
            request_id: Uuid::from_slice(&request.request_id)
                .map_err(|_| Status::invalid_argument("request_id"))?,
            worker: WorkerIdentity(request.worker_identity),
        };
        let outcome = tokeira_runtime::scope_activity_start_response(
            ActivityResponsePreflight::new(context, namespace.clone(), namespace_id),
            self.runtime.start_forwarded_activity_task(request),
        )
        .await
        .map_err(start_status)?;
        let response = match outcome {
            ActivityStartOutcome::Started(task) | ActivityStartOutcome::Replayed(task) => {
                project(task, namespace_id, &namespace).map_err(start_status)?
            }
            ActivityStartOutcome::Stale | ActivityStartOutcome::Suppressed => {
                wire::StartActivityTaskResponse::default()
            }
        };
        Ok(Response::new(response))
    }
}

fn start_status(error: anyhow::Error) -> Status {
    if let Some(status) = error.downcast_ref::<Status>() {
        return status.clone();
    }
    if error.is::<tokeira_runtime::InvalidActivityStart>() {
        return Status::invalid_argument(error.to_string());
    }
    if error.is::<tokeira_runtime::ActivityStartTooLarge>() {
        return Status::resource_exhausted(error.to_string());
    }
    Status::unavailable(error.to_string())
}

fn timestamp(value: Option<prost_types::Timestamp>) -> Result<Option<OffsetDateTime>> {
    value
        .map(|value| {
            ensure!(
                (0..1_000_000_000).contains(&value.nanos),
                "invalid activity timestamp nanos"
            );
            Ok(OffsetDateTime::from_unix_timestamp(value.seconds)?
                .replace_nanosecond(value.nanos as u32)?)
        })
        .transpose()
}

fn duration(value: Option<prost_types::Duration>) -> Result<Option<time::Duration>> {
    value
        .map(|value| {
            crate::grpc::translate::validate_and_cap_proto_duration(Some(&value))
                .map_err(|error| anyhow::anyhow!(error))
        })
        .transpose()
}

// Disabled retries bypass admission validation on the owner as well as at the
// receiving node (common/retrypolicy/retry_policy.go:65-68 @ v1.31.0).
fn retry_duration(
    value: Option<prost_types::Duration>,
    maximum_attempts: i32,
) -> Result<Option<time::Duration>> {
    if maximum_attempts == 1 {
        Ok(crate::grpc::translate::proto_duration_value(value.as_ref()))
    } else {
        duration(value)
    }
}

fn decode_response(
    response: wire::StartActivityTaskResponse,
    request: &ActivityTaskStartRequest,
    namespace: &str,
) -> Result<ActivityStartOutcome> {
    ensure!(
        response.encoded_len() <= MAX_RESPONSE,
        "activity start reply exceeds envelope"
    );
    let Some(task) = response.task else {
        ensure!(
            response.origin.is_none() && !response.replayed,
            "malformed empty activity reply"
        );
        return Ok(ActivityStartOutcome::Stale);
    };
    let origin = response
        .origin
        .ok_or_else(|| anyhow!("activity reply has no origin"))?;
    let offered = &request.offered;
    let expected_origin = WorkerTaskOrigin::from_queue_key(
        &offered.queue,
        offered.queue.task_queue.clone(),
        WorkerTaskClass::Activity,
    );
    let origin = WorkerTaskOrigin {
        namespace_id: NamespaceId(Uuid::from_slice(&origin.namespace_id)?),
        normal_task_queue: TaskQueueName(origin.normal_task_queue),
        task_class: WorkerTaskClass::Activity,
        deployment: DeploymentId(origin.deployment.unwrap_or_default()),
        build_id: BuildId(origin.build_id.unwrap_or_default()),
    };
    ensure!(
        origin == expected_origin,
        "activity reply origin differs from admitted match"
    );
    let (token, namespace_id) = crate::task_token::decode::<ActivityTaskToken>(&task.task_token)?;
    ensure!(
        namespace_id == Some(offered.queue.namespace_id)
            && token.run_key == offered.run_key
            && token.activity_id == offered.activity_id
            && token.schedule_event_id == offered.schedule_event_id
            && token.attempt == offered.attempt
            && task.activity_id == offered.activity_id
            && task.attempt > 0
            && u32::try_from(task.attempt)? == offered.attempt,
        "activity reply identity differs from match"
    );
    ensure!(
        if response.replayed {
            task.workflow_namespace.is_empty()
                && task.workflow_type.is_none()
                && task.retry_policy.is_none()
                && task.heartbeat_details.is_none()
        } else {
            task.workflow_namespace == namespace && task.workflow_type.is_some()
        },
        "activity reply has invalid fresh/replay shape"
    );
    let execution = task
        .workflow_execution
        .ok_or_else(|| anyhow!("activity reply has no workflow execution"))?;
    let retry_policy = task
        .retry_policy
        .map(|policy| -> Result<_> {
            Ok(RetryPolicy {
                initial_interval: retry_duration(policy.initial_interval, policy.maximum_attempts)?
                    .ok_or_else(|| anyhow!("retry interval missing"))?,
                backoff_coefficient: policy.backoff_coefficient,
                maximum_interval: retry_duration(policy.maximum_interval, policy.maximum_attempts)?,
                maximum_attempts: u32::try_from(policy.maximum_attempts)?,
                non_retryable_error_types: policy.non_retryable_error_types,
            })
        })
        .transpose()?;
    let started = StartedActivityTask {
        replayed: response.replayed,
        priority: task.priority.map(|value| Priority {
            priority_key: value.priority_key,
            fairness_key: value.fairness_key,
            fairness_weight: value.fairness_weight,
        }),
        run_key: offered.run_key,
        run_id: RunId(Uuid::parse_str(&execution.run_id)?),
        activity_id: task.activity_id,
        activity_type: task
            .activity_type
            .ok_or_else(|| anyhow!("activity type missing"))?
            .name,
        task_queue: offered.queue.task_queue.clone(),
        token,
        input: task
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        attempt: offered.attempt,
        workflow_id: execution.workflow_id,
        workflow_type: task
            .workflow_type
            .map(|value| value.name)
            .unwrap_or_default(),
        header: task.header.as_ref().map(headers_to_domain),
        retry_policy,
        heartbeat_details: task.heartbeat_details.as_ref().map(payloads_to_domain),
        scheduled_time: timestamp(task.scheduled_time)?
            .ok_or_else(|| anyhow!("schedule time missing"))?,
        current_attempt_scheduled_time: timestamp(task.current_attempt_scheduled_time)?,
        started_time: timestamp(task.started_time)?.ok_or_else(|| anyhow!("start time missing"))?,
        schedule_to_close_timeout: duration(task.schedule_to_close_timeout)?,
        start_to_close_timeout: duration(task.start_to_close_timeout)?,
        heartbeat_timeout: duration(task.heartbeat_timeout)?,
        origin,
    };
    Ok(if response.replayed {
        ActivityStartOutcome::Replayed(started)
    } else {
        ActivityStartOutcome::Started(started)
    })
}

#[cfg(test)]
mod tests;
