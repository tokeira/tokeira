//! Authenticated internal forwarding of a matched workflow-task incarnation.
//!
//! The queue owner forwards the admitted poll's credentials and exact worker
//! target. The execution owner authenticates and authorizes them again before
//! entering its serving gate. No endpoint accepts arbitrary kernel commands.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use http::HeaderMap;
use prost::Message as _;
use tokeira_proto::internal::runtime::{
    StartWorkflowTaskRequest, StartWorkflowTaskResponse,
    workflow_task_dispatch_client::WorkflowTaskDispatchClient,
    workflow_task_dispatch_server::{WorkflowTaskDispatch, WorkflowTaskDispatchServer},
};
use tokeira_runtime::{
    StartedWorkflowTask, TokeiraRuntime, WorkflowOfferRejected, WorkflowTaskStarter,
};
use tokeira_storage::{DispatchableWorkflowTask, RunRepository};
use tokeira_types::{
    BuildId, DeploymentId, LogicalTaskSeq, QueueKey, RunKey, ShardId, TaskKind, TaskQueueName,
    WorkerIdentity, WorkerTaskClass,
};
use tonic::{Code, Request, Response, Status};

use crate::{
    Action, EdgeInterceptors, ProtocolMessageDto, RoutingCache, WorkflowQueryDto, WorkflowService,
    grpc::metadata::metadata_to_header_map, interceptors::normalize_worker_poll_target,
    workflow_service::resolved_namespace_id,
};

const MAX_START_REQUEST_BYTES: usize = 64 * 1024;
const MAX_START_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Only transport payloads cross homes; completion channels remain on the
/// execution owner, where public workflow-task reply forwarding returns the
/// worker's response.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkflowTaskDecorations {
    pub(crate) queries: HashMap<String, WorkflowQueryDto>,
    pub(crate) messages: Vec<ProtocolMessageDto>,
}

/// Request-local authorization material; never stored with offers or run state.
#[derive(Clone)]
pub(crate) struct ForwardedPoll {
    pub(crate) headers: HeaderMap,
    pub(crate) namespace: String,
    pub(crate) sticky: bool,
    pub(crate) normal_task_queue: Option<String>,
    pub(crate) scoped_versioned: bool,
    pub(crate) decorations: Arc<Mutex<Option<WorkflowTaskDecorations>>>,
}

tokio::task_local! {
    pub(crate) static FORWARDED_POLL: ForwardedPoll;
}

/// Uses the same controller snapshot as edge routing to locate the execution
/// owner. A stale route fails the poll; a later poll obtains a fresh route.
#[derive(Debug)]
pub struct RemoteWorkflowTaskStarter {
    cache: Arc<RoutingCache>,
}

impl RemoteWorkflowTaskStarter {
    /// Construct from the engine's shared, continuously refreshed routing cache.
    pub fn new(cache: Arc<RoutingCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl WorkflowTaskStarter for RemoteWorkflowTaskStarter {
    async fn start(
        &self,
        home: ShardId,
        offered: DispatchableWorkflowTask,
        worker: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let context = FORWARDED_POLL
            .try_with(Clone::clone)
            .map_err(|_| anyhow!("remote workflow start requires an authorized poll context"))?;
        let snapshot = self.cache.snapshot();
        let owner = snapshot
            .lookup_bundle_owner(home)
            .ok_or_else(|| anyhow!("workflow execution home has no route"))?;
        let endpoint = snapshot
            .lookup_node_endpoint(owner.node_id)
            .ok_or_else(|| anyhow!("workflow execution owner has no endpoint"))?;
        let endpoint =
            tonic::transport::Endpoint::from_shared(format!("http://{}", endpoint.as_authority()))?
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10));
        let mut client = WorkflowTaskDispatchClient::new(endpoint.connect().await?)
            .max_decoding_message_size(MAX_START_RESPONSE_BYTES)
            .max_encoding_message_size(MAX_START_REQUEST_BYTES);
        let mut request = Request::new(StartWorkflowTaskRequest {
            run_key: offered.run_key.0.as_bytes().to_vec(),
            logical_seq: offered.logical_seq.0,
            namespace: context.namespace,
            task_queue: offered.queue.task_queue.0,
            deployment: offered.queue.deployment.map(|value| value.0),
            build_id: offered.queue.build_id.map(|value| value.0),
            worker_identity: worker.0,
            sticky: context.sticky,
            normal_task_queue: context.normal_task_queue,
            scoped_versioned: context.scoped_versioned,
        });
        if request.get_ref().encoded_len() > MAX_START_REQUEST_BYTES {
            return Err(Status::resource_exhausted(
                "internal workflow-start request exceeds 64 KiB",
            )
            .into());
        }
        // Preserve configured authentication headers without forwarding HTTP
        // framing or routing headers from the public socket.
        let mut headers = context.headers;
        for name in [
            "host",
            "content-length",
            "content-type",
            "connection",
            "te",
            "grpc-timeout",
        ] {
            headers.remove(name);
        }
        *request.metadata_mut() = tonic::metadata::MetadataMap::from_headers(headers);
        let response = match client.start_workflow_task(request).await {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::NotFound => {
                return Err(WorkflowOfferRejected.into());
            }
            Err(status) => return Err(status.into()),
        };
        let started = serde_json::from_slice(&response.started_task_json)?;
        let decorations = serde_json::from_slice(&response.decorations_json)?;
        *context
            .decorations
            .lock()
            .expect("forwarded decorations lock poisoned") = Some(decorations);
        Ok(started)
    }
}

/// The internal service retains the public poll's authorization contract while
/// committing only on a locally serving execution home.
pub struct WorkflowTaskDispatchGrpc<R> {
    runtime: Arc<TokeiraRuntime<R>>,
    interceptors: Arc<EdgeInterceptors>,
    workflow_service: WorkflowService,
}

impl<R> fmt::Debug for WorkflowTaskDispatchGrpc<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkflowTaskDispatchGrpc")
            .finish_non_exhaustive()
    }
}
impl<R> Clone for WorkflowTaskDispatchGrpc<R> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            interceptors: self.interceptors.clone(),
            workflow_service: self.workflow_service.clone(),
        }
    }
}
impl<R: RunRepository + 'static> WorkflowTaskDispatchGrpc<R> {
    /// Construct around the public edge's runtime, interceptors and service.
    /// `workflow_service` must share its pending-query store with the public
    /// completion handler, which receives the execution owner's task tokens.
    pub fn new(
        runtime: Arc<TokeiraRuntime<R>>,
        interceptors: Arc<EdgeInterceptors>,
        workflow_service: WorkflowService,
    ) -> Self {
        Self {
            runtime,
            interceptors,
            workflow_service,
        }
    }
    /// Bound the input envelope before protobuf decoding; no history is accepted.
    pub fn into_service(self) -> WorkflowTaskDispatchServer<Self> {
        WorkflowTaskDispatchServer::new(self)
            .max_decoding_message_size(MAX_START_REQUEST_BYTES)
            .max_encoding_message_size(MAX_START_RESPONSE_BYTES)
    }
}

#[tonic::async_trait]
impl<R: RunRepository + 'static> WorkflowTaskDispatch for WorkflowTaskDispatchGrpc<R> {
    async fn start_workflow_task(
        &self,
        request: Request<StartWorkflowTaskRequest>,
    ) -> std::result::Result<Response<StartWorkflowTaskResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let request = request.into_inner();
        let context = self
            .interceptors
            .begin_worker_preflight(
                &headers,
                Some(&request.namespace),
                Action::PollWorkflowTaskQueue,
                false,
            )
            .await?;
        self.interceptors
            .authorize_worker_target(
                &context,
                Action::PollWorkflowTaskQueue,
                normalize_worker_poll_target(
                    &request.task_queue,
                    request.normal_task_queue.as_deref(),
                    request.sticky,
                    request.scoped_versioned,
                    request.deployment.as_deref(),
                    request.build_id.as_deref(),
                    WorkerTaskClass::Workflow,
                ),
            )
            .await?;
        let namespace_id = resolved_namespace_id(&context, &request.namespace)?;
        let run_key = RunKey(
            uuid::Uuid::from_slice(&request.run_key)
                .map_err(|_| Status::invalid_argument("run_key must be a UUID"))?,
        );
        if request.sticky {
            let tokeira_kernel::LoadedRun::Existing(state) = self
                .runtime
                .repo()
                .load_run(run_key)
                .await
                .map_err(|_| Status::unavailable("workflow state could not be loaded"))?
            else {
                return Err(Status::not_found("workflow offer is no longer startable"));
            };
            // Sticky policy uses the supplied normal queue. Bind that coordinate
            // to authority before trusting the authorization decision above.
            if state.namespace_id != namespace_id
                || request
                    .normal_task_queue
                    .as_deref()
                    .is_some_and(|normal| normal != state.task_queue.0)
            {
                return Err(Status::not_found("workflow offer is no longer startable"));
            }
            self.interceptors
                .authorize_worker_target(
                    &context,
                    Action::PollWorkflowTaskQueue,
                    normalize_worker_poll_target(
                        &request.task_queue,
                        Some(&state.task_queue.0),
                        true,
                        request.scoped_versioned,
                        request.deployment.as_deref(),
                        request.build_id.as_deref(),
                        WorkerTaskClass::Workflow,
                    ),
                )
                .await?;
        }
        let worker = WorkerIdentity(request.worker_identity);
        let task = DispatchableWorkflowTask {
            run_key,
            logical_seq: LogicalTaskSeq(request.logical_seq),
            queue: QueueKey {
                namespace_id,
                task_queue: TaskQueueName(request.task_queue),
                task_kind: TaskKind::Workflow,
                deployment: request.deployment.map(DeploymentId),
                build_id: request.build_id.map(BuildId),
            },
            sticky_preferred: request.sticky.then(|| worker.clone()),
            normal_queue: None,
            sticky_deadline: None,
            priority: None,
            order: None,
        };
        let started = self
            .runtime
            .start_forwarded_workflow_task(task, worker)
            .await
            .map_err(|error| {
                if error.is::<WorkflowOfferRejected>() {
                    return Status::not_found("workflow offer is no longer startable");
                }
                if let Some(rejected) = error.downcast_ref::<tokeira_runtime::KernelRejected>() {
                    match rejected.0 {
                        tokeira_kernel::Reject::MissingRun
                        | tokeira_kernel::Reject::RunClosed(_)
                        | tokeira_kernel::Reject::NoPendingWorkflowTask
                        | tokeira_kernel::Reject::WorkflowPaused
                        | tokeira_kernel::Reject::WorkflowTaskSeqMismatch { .. }
                        | tokeira_kernel::Reject::WorkflowTaskAlreadyStarted { .. } => {
                            return Status::not_found("workflow offer is no longer startable");
                        }
                        _ => {}
                    }
                }
                Status::unavailable(error.to_string())
            })?;
        let task_token = crate::task_token::encode(started.token.clone(), namespace_id)
            .map_err(|_| Status::internal("failed to encode workflow token"))?;
        // Match the immutable poll-history boundary, including a transient or
        // speculative suffix. Later events must not release queries early.
        let barrier = started
            .last_event_id_at_start
            .max(started.token.started_event_id);
        let decorations = self
            .workflow_service
            .workflow_task_decorations(&started, barrier, &task_token)
            .await
            .map_err(Status::from)?;
        let response = StartWorkflowTaskResponse {
            started_task_json: serde_json::to_vec(&started)
                .map_err(|_| Status::internal("failed to encode workflow start"))?,
            decorations_json: serde_json::to_vec(&decorations)
                .map_err(|_| Status::internal("failed to encode workflow attachments"))?,
        };
        // An encoder-side refusal can otherwise reset the HTTP/2 stream and
        // hide the reason. Keep the transport cap as defense in depth while
        // returning a stable explicit error for this known oversized envelope.
        if response.encoded_len() > MAX_START_RESPONSE_BYTES {
            return Err(Status::resource_exhausted(
                "internal workflow-start response exceeds 4 MiB",
            ));
        }
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EdgeContext, EdgeError, EdgeResult, InMemoryNamespaceCache, NamespaceCache,
        ResolvedNamespace, interceptors::Authenticator,
    };
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokeira_auth::{AuthPrincipal, Claims};
    use tokeira_proto::{
        enums::{CommandType, QueryResultType, WorkflowTaskFailedCause},
        public::temporal::api::{
            command::v1::{Command, ProtocolMessageCommandAttributes, command::Attributes},
            protocol::v1::Message as ProtocolMessage,
            query::v1::WorkflowQueryResult,
            update::v1::{Acceptance, Outcome, Response as UpdateResponse, outcome},
        },
        workflowservice::{
            RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskFailedRequest,
            workflow_service_server::WorkflowService as PublicWorkflowService,
        },
    };
    use tokeira_runtime::{
        BacklogConfig, LaneConfig, TimerScannerConfig, WorkflowTimeoutScannerConfig,
    };
    use tokeira_storage::InMemoryStore;
    use tokeira_types::{
        BundleOwner, IncarnationId, NodeEndpoint, PlacementConfig, RoutingSnapshot, ShardEpoch,
    };
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_util::sync::CancellationToken;

    struct WorkerAuth(AtomicUsize);
    #[async_trait]
    impl Authenticator for WorkerAuth {
        async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
            if headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some("Bearer synthetic-worker")
            {
                return Err(EdgeError::Unauthorized("worker credential required".into()));
            }
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
        async fn authorize(
            &self,
            _claims: Option<&Claims>,
            action: Action,
            namespace: Option<&str>,
        ) -> EdgeResult<Option<AuthPrincipal>> {
            assert!(matches!(
                action,
                Action::PollWorkflowTaskQueue
                    | Action::RespondWorkflowTaskCompleted
                    | Action::RespondWorkflowTaskFailed
            ));
            assert_eq!(namespace, Some("default"));
            Ok(None)
        }
    }

    #[tokio::test]
    async fn forwarded_start_and_replies_use_owner_waiters_and_reauthenticate() {
        let store = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(TokeiraRuntime::new(
            store.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        ));
        let context = EdgeContext {
            request_id: crate::request_id::RequestId::new("forwarded-start"),
            claims: None,
            auth_principal: None,
            namespace: None,
            received_at: time::OffsetDateTime::now_utc(),
            is_long_poll: false,
        };
        let request = crate::translate::to_internal::start_request(start_dto(), &context);
        let key = request.run_key;
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let execution = tokeira_types::ExecutionRef {
            namespace_id: request.namespace_id,
            workflow_id: request.workflow_id.clone(),
            run_id: Some(request.run_id),
        };
        runtime.start_workflow(request).await.unwrap();
        runtime
            .update_workflow(
                execution.clone(),
                "owner-update".into(),
                "update-handler".into(),
                tokeira_types::Payloads(vec![tokeira_types::Payload::new(
                    b"update-input".to_vec(),
                )]),
                tokeira_types::RequestContext::unattributed(time::OffsetDateTime::now_utc()),
                time::Duration::seconds(30),
                tokeira_runtime::UpdateWaitPolicy::Admitted,
                64,
            )
            .await
            .unwrap();
        let update_reply = runtime.poll_workflow_update(
            execution,
            "owner-update".into(),
            tokeira_runtime::UpdateWaitPolicy::Completed,
            time::Duration::seconds(30),
        );
        tokio::pin!(update_reply);
        tokio::select! {
            biased;
            result = &mut update_reply => panic!("update completed before the worker replied: {result:?}"),
            () = std::future::ready(()) => {},
        }
        let query_reply = buffer_query(&runtime, key, 0, b"query-input".to_vec());
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let auth = Arc::new(WorkerAuth(AtomicUsize::new(0)));
        let interceptors = Arc::new(EdgeInterceptors::configured(
            namespaces.clone(),
            auth.clone(),
            false,
        ));
        let pending = crate::PendingQueryStore::default();
        let workflow_service = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
            Arc::new(crate::grpc::runtime_adapter::RuntimeAdapter::new(
                runtime.clone(),
            )),
            Arc::new(crate::InMemoryExecutionResolver::new()),
            Arc::new(crate::EmptyVisibilityApi),
            store.clone(),
            Arc::new(crate::InMemoryOperatorApi::new("test", "test")),
            namespaces.clone(),
            interceptors.clone(),
            crate::PollerRegistry::default(),
            pending.clone(),
            runtime.buffered_queries(),
            runtime.broker(),
            crate::LongPollGate::new(crate::LongPollConfig::default()),
            Arc::new(crate::LocalOnlyRouter),
            crate::HistoryWaitRegistry::default(),
        );
        let public_service =
            crate::grpc::workflow_service::WorkflowServiceGrpc::new(workflow_service.clone());
        let owner_reply_service = public_service.clone();
        let service =
            WorkflowTaskDispatchGrpc::new(runtime.clone(), interceptors.clone(), workflow_service);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let accepting = stop.clone();
        let (sockets, incoming) = tokio::sync::mpsc::channel(4);
        let accept = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accepting.cancelled() => break,
                    socket = listener.accept() => { if sockets.send(socket.map(|(socket, _)| socket)).await.is_err() { break; } }
                }
            }
        });
        let shutdown = stop.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service.into_service())
                .add_service(public_service.into_service())
                .serve_with_incoming_shutdown(ReceiverStream::new(incoming), shutdown.cancelled())
                .await
                .unwrap();
        });
        let placement = PlacementConfig {
            shard_count: 1,
            bundle_count: 1,
            partition_count: 1,
            hash_version: 1,
        };
        let cache = Arc::new(RoutingCache::new(placement));
        let node = IncarnationId(uuid::Uuid::new_v4());
        let mut snapshot = RoutingSnapshot::new(placement);
        snapshot.execution_bundle_owners.insert(
            ShardId(0),
            BundleOwner {
                node_id: node,
                epoch: ShardEpoch::ZERO,
            },
        );
        snapshot.node_endpoints.insert(
            node,
            NodeEndpoint {
                host: address.ip().to_string(),
                port: address.port(),
            },
        );
        cache.replace_snapshot(snapshot);
        let starter = RemoteWorkflowTaskStarter::new(cache.clone());
        let offer = DispatchableWorkflowTask {
            run_key: key,
            logical_seq: LogicalTaskSeq::ONE,
            queue,
            sticky_preferred: None,
            normal_queue: None,
            sticky_deadline: None,
            priority: None,
            order: None,
        };
        let mut forwarding = ForwardedPoll {
            headers: HeaderMap::new(),
            namespace: "default".into(),
            sticky: false,
            normal_task_queue: None,
            scoped_versioned: false,
            decorations: Arc::new(Mutex::new(None)),
        };
        let before = store.snapshot().await.unwrap();
        let rejected = FORWARDED_POLL
            .scope(
                forwarding.clone(),
                starter.start(ShardId(0), offer.clone(), WorkerIdentity("worker".into())),
            )
            .await
            .unwrap_err();
        assert_eq!(
            rejected.downcast_ref::<Status>().unwrap().code(),
            Code::Unauthenticated
        );
        assert_eq!(store.snapshot().await.unwrap(), before);
        forwarding.headers.insert(
            "authorization",
            http::HeaderValue::from_static("Bearer synthetic-worker"),
        );
        let oversized_request = FORWARDED_POLL
            .scope(
                forwarding.clone(),
                starter.start(
                    ShardId(0),
                    offer.clone(),
                    WorkerIdentity("w".repeat(MAX_START_REQUEST_BYTES)),
                ),
            )
            .await
            .unwrap_err();
        let oversized_status = oversized_request.downcast_ref::<Status>().unwrap();
        assert_eq!(oversized_status.code(), Code::ResourceExhausted);
        let public_status: Status = crate::EdgeError::from(oversized_request).into();
        assert_eq!(public_status.code(), Code::ResourceExhausted);
        assert!(public_status.message().contains("64 KiB"));
        assert_eq!(store.snapshot().await.unwrap(), before);
        for mutate in 0..3 {
            let mut forged = offer.clone();
            let mut context = forwarding.clone();
            match mutate {
                0 => forged.queue.task_queue.0 = "another-authorized-queue".into(),
                1 => forged.queue.deployment = Some(DeploymentId("other-version".into())),
                _ => {
                    context.sticky = true;
                    context.normal_task_queue = Some("another-authorized-queue".into());
                }
            }
            let error = FORWARDED_POLL
                .scope(
                    context,
                    starter.start(ShardId(0), forged, WorkerIdentity("worker".into())),
                )
                .await
                .unwrap_err();
            assert!(error.is::<WorkflowOfferRejected>());
            assert_eq!(store.snapshot().await.unwrap(), before);
        }
        let started = FORWARDED_POLL
            .scope(
                forwarding.clone(),
                starter.start(ShardId(0), offer.clone(), WorkerIdentity("worker".into())),
            )
            .await
            .unwrap();
        assert_eq!(started.run_key, key);
        assert_eq!(started.token.shard_epoch, ShardEpoch::ZERO);
        let decorations = forwarding.decorations.lock().unwrap().take().unwrap();
        assert_eq!(decorations.messages.len(), 1);
        assert_eq!(decorations.messages[0].protocol_instance_id, "owner-update");
        assert!(runtime.pending_update_transports(key, false).is_empty());
        assert!(
            runtime
                .update_registry()
                .contains_registered_update(key, "owner-update")
        );
        assert_eq!(decorations.queries.len(), 1);
        let (query_id, query) = decorations.queries.into_iter().next().unwrap();
        assert_eq!(query.query_args.0[0].data, b"query-input");
        assert!(!runtime.buffered_queries().has_buffered(key));
        let token =
            crate::task_token::encode(started.token.clone(), offer.queue.namespace_id).unwrap();
        let committed = store.snapshot().await.unwrap();
        let stale = FORWARDED_POLL
            .scope(
                forwarding.clone(),
                starter.start(ShardId(0), offer.clone(), WorkerIdentity("worker".into())),
            )
            .await
            .unwrap_err();
        assert!(stale.is::<WorkflowOfferRejected>());
        assert_eq!(store.snapshot().await.unwrap(), committed);
        assert_eq!(auth.0.load(Ordering::SeqCst), 5);
        let queue_runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards(
            store.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Arc::new(tokeira_runtime::NoopNexusHttpClient),
            Default::default(),
            1,
            "queue-owner".into(),
            false,
        ));
        let queue_service = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
            Arc::new(crate::grpc::runtime_adapter::RuntimeAdapter::new(
                queue_runtime.clone(),
            )),
            Arc::new(crate::InMemoryExecutionResolver::new()),
            Arc::new(crate::EmptyVisibilityApi),
            store.clone(),
            Arc::new(crate::InMemoryOperatorApi::new("test", "test")),
            namespaces,
            interceptors,
            crate::PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            queue_runtime.buffered_queries(),
            queue_runtime.broker(),
            crate::LongPollGate::new(crate::LongPollConfig::default()),
            Arc::new(crate::CacheBackedRouter::new(cache, IncarnationId::new())),
            crate::HistoryWaitRegistry::default(),
        );
        let queue_service = crate::grpc::workflow_service::WorkflowServiceGrpc::new(queue_service);
        let completion = owner_completion(token, query_id, started.token.started_event_id - 1);
        let error = queue_service
            .respond_workflow_task_completed(Request::new(completion.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated);
        assert_eq!(store.snapshot().await.unwrap(), committed);
        let mut looping = authorized_reply(completion.clone());
        looping
            .metadata_mut()
            .insert("x-tokeira-workflow-reply-forwarded", "1".parse().unwrap());
        let error = queue_service
            .respond_workflow_task_completed(looping)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unavailable);
        assert_eq!(store.snapshot().await.unwrap(), committed);
        let response = queue_service
            .respond_workflow_task_completed(authorized_reply(completion.clone()))
            .await
            .unwrap();
        assert!(!response.metadata().contains_key("grpc-encoding"));
        assert!(!response.metadata().contains_key("content-length"));
        let local_stale = owner_reply_service
            .respond_workflow_task_completed(authorized_reply(completion.clone()))
            .await
            .unwrap_err();
        let stale = queue_service
            .respond_workflow_task_completed(authorized_reply(completion))
            .await
            .unwrap_err();
        assert_eq!(stale.code(), Code::NotFound);
        assert_eq!(stale.code(), local_stale.code());
        assert_eq!(stale.message(), local_stale.message());
        assert_eq!(stale.message(), "Workflow task not found.");
        assert!(!stale.metadata().contains_key("grpc-encoding"));
        assert!(matches!(
            query_reply.await.unwrap(),
            tokeira_runtime::QueryResult::Completed { .. }
        ));
        let update = update_reply.await.unwrap();
        assert_eq!(
            update.stage,
            tokeira_runtime::UpdateLifecycleStage::Completed
        );
        assert!(matches!(
            update.outcome,
            Some(tokeira_runtime::UpdateOutcome::Completed { .. })
        ));
        let history = store.read_history_to_end(key, 0).await.unwrap();
        assert!(
            history.iter().any(|event| matches!(
                event.kind,
                tokeira_kernel::HistoryEventKind::WorkflowExecutionUpdateCompletedV2 { .. }
            )),
            "owner history: {history:#?}"
        );
        let mut future_dto = start_dto();
        future_dto.workflow_id = "future-query-barrier".into();
        let future_request = crate::translate::to_internal::start_request(future_dto, &context);
        let future_key = future_request.run_key;
        runtime.start_workflow(future_request).await.unwrap();
        let _future_reply = buffer_query(&runtime, future_key, i64::MAX, Vec::new());
        let mut future_offer = offer.clone();
        future_offer.run_key = future_key;
        let future_started = FORWARDED_POLL
            .scope(
                forwarding.clone(),
                starter.start(ShardId(0), future_offer, WorkerIdentity("worker".into())),
            )
            .await
            .unwrap();
        assert!(
            forwarding
                .decorations
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .queries
                .is_empty()
        );
        assert!(
            runtime.buffered_queries().has_buffered(future_key),
            "future barrier stays on owner"
        );
        queue_service
            .respond_workflow_task_failed(authorized_reply(RespondWorkflowTaskFailedRequest {
                namespace: "default".into(),
                task_token: crate::task_token::encode(
                    future_started.token,
                    offer.queue.namespace_id,
                )
                .unwrap(),
                cause: WorkflowTaskFailedCause::UnhandledCommand as i32,
                identity: "worker".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let history = store.read_history_to_end(future_key, 0).await.unwrap();
        assert!(history.iter().any(|event| matches!(
            event.kind,
            tokeira_kernel::HistoryEventKind::WorkflowTaskFailed { .. }
        )));
        let mut large_dto = start_dto();
        large_dto.workflow_id = "oversized-attachments".into();
        let large_request = crate::translate::to_internal::start_request(large_dto, &context);
        let large_key = large_request.run_key;
        runtime.start_workflow(large_request).await.unwrap();
        let _large_reply = buffer_query(&runtime, large_key, 0, vec![255; 1_100_000]);
        let mut large_offer = offer;
        large_offer.run_key = large_key;
        let too_large = FORWARDED_POLL
            .scope(
                forwarding.clone(),
                starter.start(ShardId(0), large_offer, WorkerIdentity("worker".into())),
            )
            .await
            .unwrap_err();
        let oversized_status = too_large.downcast_ref::<Status>().unwrap();
        assert_eq!(oversized_status.code(), Code::ResourceExhausted);
        let public_status: Status = crate::EdgeError::from(too_large).into();
        assert_eq!(public_status.code(), Code::ResourceExhausted);
        assert!(public_status.message().contains("4 MiB"));
        assert!(forwarding.decorations.lock().unwrap().is_none());
        let tokeira_kernel::LoadedRun::Existing(state) = store.load_run(large_key).await.unwrap()
        else {
            panic!("started run missing")
        };
        assert!(
            state.pending_workflow_task.unwrap().started_at.is_some(),
            "response refusal must not undo a committed start"
        );
        queue_runtime.shutdown_handle().begin_shutdown();
        queue_runtime
            .shutdown_handle()
            .wait(std::time::Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
        stop.cancel();
        server.await.unwrap();
        accept.await.unwrap();
        runtime.shutdown_handle().begin_shutdown();
        runtime
            .shutdown_handle()
            .wait(std::time::Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
    }

    fn authorized_reply<T>(value: T) -> Request<T> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", "Bearer synthetic-worker".parse().unwrap());
        request
    }

    fn owner_completion(
        token: Vec<u8>,
        query_id: String,
        sequencing: i64,
    ) -> RespondWorkflowTaskCompletedRequest {
        let bodies = [
            (
                "acceptance",
                "Acceptance",
                Acceptance {
                    accepted_request_message_id: "owner-update/request".into(),
                    accepted_request_sequencing_event_id: sequencing,
                    accepted_request: None,
                }
                .encode_to_vec(),
            ),
            (
                "response",
                "Response",
                UpdateResponse {
                    meta: None,
                    outcome: Some(Outcome {
                        value: Some(outcome::Value::Success(Default::default())),
                    }),
                }
                .encode_to_vec(),
            ),
        ];
        let commands = bodies
            .iter()
            .map(|(suffix, _, _)| Command {
                command_type: CommandType::ProtocolMessage as i32,
                attributes: Some(Attributes::ProtocolMessageCommandAttributes(
                    ProtocolMessageCommandAttributes {
                        message_id: format!("owner-update/{suffix}"),
                    },
                )),
                ..Default::default()
            })
            .collect();
        let messages = bodies
            .into_iter()
            .map(|(suffix, kind, value)| ProtocolMessage {
                id: format!("owner-update/{suffix}"),
                protocol_instance_id: "owner-update".into(),
                body: Some(prost_types::Any {
                    type_url: format!("type.googleapis.com/temporal.api.update.v1.{kind}"),
                    value,
                }),
                sequencing_id: None,
            })
            .collect();
        RespondWorkflowTaskCompletedRequest {
            namespace: "default".into(),
            task_token: token,
            identity: "worker".into(),
            commands,
            messages,
            query_results: BTreeMap::from([(
                query_id,
                WorkflowQueryResult {
                    result_type: QueryResultType::Answered as i32,
                    answer: Some(Default::default()),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        }
    }

    fn buffer_query(
        runtime: &TokeiraRuntime<InMemoryStore>,
        key: RunKey,
        barrier: i64,
        data: Vec<u8>,
    ) -> tokio::sync::oneshot::Receiver<tokeira_runtime::QueryResult> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        runtime
            .buffered_queries()
            .buffer(
                key,
                tokeira_runtime::BufferedQuery {
                    query_id: uuid::Uuid::new_v4().to_string(),
                    query_type: "query-handler".into(),
                    query_args: tokeira_types::Payloads(vec![tokeira_types::Payload::new(data)]),
                    required_barrier: barrier,
                    enqueued_at: std::time::Instant::now(),
                    deadline: time::OffsetDateTime::now_utc() + time::Duration::minutes(1),
                    response_tx,
                },
            )
            .unwrap();
        response_rx
    }

    fn start_dto() -> crate::translate::StartWorkflowExecutionRequest {
        crate::translate::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            workflow_type: "workflow-type".to_string(),
            task_queue: "queue".to_string(),
            input: tokeira_types::Payloads::default(),
            request_id: None,
            memo: tokeira_types::Memo::default(),
            search_attributes: tokeira_types::SearchAttributes::default(),
            identity: None,
            request_eager_execution: false,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            completion_callback_limit: usize::MAX,
            user_metadata: None,
            links: Vec::new(),
            eager_worker_deployment_options: None,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: None,
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            header: None,
            versioning_override: None,
            on_conflict_options: None,
            priority: None,
            cron_schedule: None,
            run_key: None,
            run_id: None,
            now: Some(time::OffsetDateTime::now_utc()),
        }
    }
}
