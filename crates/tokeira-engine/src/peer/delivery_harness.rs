//! Shared real-socket nodes, generated credentials and transport barriers.

#[path = "delivery_tests/repository.rs"]
pub(super) mod repository;

use anyhow::Result;
use http_body_util::BodyExt;
use prost::Message;
use std::sync::Mutex;
use tokeira_edge::grpc::{
    PeerConnector,
    activity_dispatch::{
        ActivityTaskDispatchGrpc, RemoteActivityTaskStarter, offer::PeerActivityOfferSink,
    },
};
use tokeira_runtime::{
    ActivityOfferError, ActivityOfferOutcome, ActivityQueueOffer, RemoteActivityOfferSink,
};

use async_trait::async_trait;
use http::{HeaderMap, Request as HttpRequest, Response as HttpResponse};
pub(super) use repository::Repository;
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use time::OffsetDateTime;
use tokeira_auth::{AuthPrincipal, Claims};
use tokeira_clock::Clock;
use tokeira_edge::{
    Action, EdgeContext, EdgeError, EdgeInterceptors, EdgeResult, InMemoryNamespaceCache,
    NamespaceCache, ResolvedNamespace, RoutingCache, WorkflowService,
    grpc::{
        runtime_adapter::RuntimeAdapter,
        workflow_dispatch::{RemoteWorkflowTaskStarter, WorkflowTaskDispatchGrpc},
        workflow_offer::{OFFER_PATH, PeerOfferEndpoint, delivery::PeerWorkflowOfferSink},
        workflow_service::WorkflowServiceGrpc,
    },
    interceptors::Authenticator,
};
use tokeira_kernel::StartRequest;
use tokeira_proto::{
    internal::runtime::{self as wire, workflow_task_dispatch_client::WorkflowTaskDispatchClient},
    public::temporal::api::taskqueue::v1::TaskQueue,
    workflowservice::{
        PollWorkflowTaskQueueRequest, workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, LaneConfig, NexusCompletionDeps, NexusEndpointRegistry,
    NexusTimeoutScannerConfig, NoopNexusHttpClient, RemoteWorkflowOfferSink, TimerScannerConfig,
    TokeiraRuntime, WorkflowOffer, WorkflowOfferError, WorkflowOfferOutcome,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{
    BundleOwner, IncarnationId, NodeEndpoint, PlacementConfig, QueueKey, RoutingSnapshot, RunKey,
    ShardEpoch, ShardId, TaskKind, bundle_for_partition, execution_home_bundle,
    workflow_queue_partition,
};
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Status, body::Body, server::NamedService};
use tower::Service;

pub(super) const ACTIVITY_OFFER_PATH: &str =
    "/tokeira.internal.runtime.v1.ActivityTaskDispatch/OfferActivityTask";
pub(super) const ACTIVITY_START_PATH: &str =
    "/tokeira.internal.runtime.v1.ActivityTaskDispatch/StartActivityTask";

#[derive(Debug)]
pub(super) struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(self.0.load(Ordering::SeqCst)).unwrap()
    }
}
impl TestClock {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(2_000_000_000)))
    }
    #[cfg(feature = "dsql-integration")]
    pub(super) fn at(now: OffsetDateTime) -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(now.unix_timestamp())))
    }
    pub(super) fn advance(&self, seconds: i64) {
        self.0.fetch_add(seconds, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
pub(super) struct WorkerPolicy(pub(super) AtomicUsize);
#[async_trait]
impl Authenticator for WorkerPolicy {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        if headers.get("authorization").and_then(|v| v.to_str().ok())
            != Some("Bearer integration-worker")
        {
            return Err(EdgeError::Unauthorized("worker credential required".into()));
        }
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
    async fn authorize(
        &self,
        _: Option<&Claims>,
        _: Action,
        namespace: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        if namespace != Some("default") {
            return Err(EdgeError::Unauthorized("wrong namespace".into()));
        }
        Ok(None)
    }
}

#[derive(Debug, Default)]
pub(super) struct Faults {
    // Transport loss is outside the gated service; successful delivery always
    // traverses the real decoder and peer/worker authorization paths.
    pub(super) mode: AtomicU8,
    pub(super) calls: AtomicUsize,
    pub(super) held: AtomicUsize,
    pub(super) reached: Notify,
    pub(super) release: Notify,
    inside: Mutex<WireCalls>,
    drained: Notify,
}

#[derive(Debug, Default)]
struct WireCalls {
    generation: usize,
    active: BTreeMap<usize, usize>,
}

#[derive(Debug)]
struct WireCall {
    faults: Arc<Faults>,
    generation: usize,
}

impl Drop for WireCall {
    fn drop(&mut self) {
        let mut inside = self.faults.inside.lock().unwrap();
        let count = inside.active.get_mut(&self.generation).unwrap();
        *count -= 1;
        if *count == 0 {
            inside.active.remove(&self.generation);
        }
        self.faults.drained.notify_waiters();
    }
}

impl Faults {
    fn enter(self: &Arc<Self>) -> (WireCall, u8) {
        let mut inside = self.inside.lock().unwrap();
        let generation = inside.generation;
        // Registration and mode selection share the barrier's lock: no call
        // can read the old mode without being included in its drain.
        *inside.active.entry(generation).or_default() += 1;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = self
            .mode
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |mode| {
                Some(if mode == 6 { 0 } else { mode })
            })
            .unwrap();
        (
            WireCall {
                faults: self.clone(),
                generation,
            },
            mode,
        )
    }

    /// Switch modes and wait until every call that selected an earlier mode
    /// completes or is dropped. Later calls cannot prolong this barrier.
    ///
    /// Use only with modes 0, 1, 2 and 5. Held modes 3 and 4 never drain until
    /// explicitly released, so their schedules must use the release wakeup.
    pub(super) async fn set_and_drain(&self, mode: u8) {
        assert!(matches!(mode, 0 | 1 | 2 | 5), "barrier mode: {mode}");
        let generation = {
            let mut inside = self.inside.lock().unwrap();
            self.mode.store(mode, Ordering::SeqCst);
            inside.generation += 1;
            inside.generation
        };
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self
                .inside
                .lock()
                .unwrap()
                .active
                .range(..generation)
                .next()
                .is_none()
            {
                return;
            }
            drained.await;
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct FaultWire<S> {
    pub(super) inner: S,
    pub(super) faults: Arc<Faults>,
    pub(super) path: &'static str,
}
impl<S: NamedService> NamedService for FaultWire<S> {
    const NAME: &'static str = S::NAME;
}
impl<S, B> Service<HttpRequest<B>> for FaultWire<S>
where
    S: Service<HttpRequest<B>, Response = HttpResponse<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = HttpResponse<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: HttpRequest<B>) -> Self::Future {
        if request.uri().path() != self.path {
            return Box::pin(self.inner.call(request));
        }
        let (call, mode) = self.faults.enter();
        let faults = self.faults.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            // Capture before polling so even an unpolled, cancelled request
            // releases its registration when its future is dropped.
            let _call = call;
            if mode == 5 {
                faults.reached.notify_one();
                return Ok(Status::unimplemented("unknown method").into_http());
            }
            if mode == 1 {
                faults.reached.notify_one();
                return Ok(Status::unavailable("dropped offer").into_http());
            }
            if mode == 3 {
                faults.reached.notify_one();
                faults.release.notified().await;
            }
            let response = inner.call(request);
            if mode == 4 {
                // The gate acquires its authenticated permit synchronously.
                // Hold its future before polling the decoder so overload must
                // be refused by the real gate, independently of broker limits.
                let release = faults.release.notified();
                tokio::pin!(release);
                release.as_mut().enable();
                faults.held.fetch_add(1, Ordering::SeqCst);
                faults.reached.notify_one();
                release.await;
            }
            let response = response.await?;
            if mode == 2 || mode == 6 {
                faults.reached.notify_one();
                Ok(Status::unavailable("dropped acknowledgement").into_http())
            } else {
                Ok(response)
            }
        })
    }
}

#[derive(Clone, Debug)]
struct ObserveStarts<S> {
    inner: S,
    requests: Arc<Mutex<Vec<wire::StartActivityTaskRequest>>>,
}
impl<S: NamedService> NamedService for ObserveStarts<S> {
    const NAME: &'static str = S::NAME;
}
impl<S> Service<HttpRequest<Body>> for ObserveStarts<S>
where
    S: Service<HttpRequest<Body>, Response = HttpResponse<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = HttpResponse<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        if request.uri().path().ends_with("/PollActivityTaskQueue") {
            return Box::pin(repository::START_READS.scope((), self.inner.call(request)));
        }
        if request.uri().path() != ACTIVITY_START_PATH {
            return Box::pin(self.inner.call(request));
        }
        let mut inner = self.inner.clone();
        let requests = self.requests.clone();
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let bytes = body.collect().await.expect("collect start wire").to_bytes();
            let decoded =
                wire::StartActivityTaskRequest::decode(&bytes[5..]).expect("observe start wire");
            requests.lock().unwrap().push(decoded);
            repository::START_READS
                .scope(
                    (),
                    inner.call(HttpRequest::from_parts(
                        parts,
                        Body::new(http_body_util::Full::new(bytes)),
                    )),
                )
                .await
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct ActivityReceipt {
    pub(super) offer: ActivityQueueOffer,
    pub(super) destination: ShardId,
    pub(super) refresh: bool,
    pub(super) outcome: Result<ActivityOfferOutcome, ActivityOfferError>,
}
#[derive(Debug)]
struct ObservedActivitySink {
    inner: PeerActivityOfferSink,
    receipts: mpsc::UnboundedSender<ActivityReceipt>,
}
#[async_trait]
impl RemoteActivityOfferSink for ObservedActivitySink {
    async fn offer(
        &self,
        offer: &ActivityQueueOffer,
        destination: ShardId,
        refresh: bool,
    ) -> Result<ActivityOfferOutcome, ActivityOfferError> {
        let outcome = self.inner.offer(offer, destination, refresh).await;
        let _ = self.receipts.send(ActivityReceipt {
            offer: offer.clone(),
            destination,
            refresh,
            outcome,
        });
        outcome
    }
}

#[derive(Clone, Debug)]
pub(super) struct Receipt {
    pub(super) offer: WorkflowOffer,
    pub(super) destination: ShardId,
    pub(super) outcome: Result<WorkflowOfferOutcome, WorkflowOfferError>,
}
#[derive(Debug)]
struct ObservedSink {
    inner: PeerWorkflowOfferSink,
    receipts: mpsc::UnboundedSender<Receipt>,
}
#[async_trait]
impl RemoteWorkflowOfferSink for ObservedSink {
    async fn offer(
        &self,
        offer: &WorkflowOffer,
        destination: ShardId,
        refresh: bool,
    ) -> Result<WorkflowOfferOutcome, WorkflowOfferError> {
        let outcome = self.inner.offer(offer, destination, refresh).await;
        let _ = self.receipts.send(Receipt {
            offer: offer.clone(),
            destination,
            outcome,
        });
        outcome
    }
}

pub(super) struct Node<R: RunRepository + 'static> {
    pub(super) runtime: Arc<TokeiraRuntime<Repository<R>>>,
    pub(super) repo: Arc<Repository<R>>,
    pub(super) peer: Arc<PeerOfferEndpoint>,
    pub(super) cache: Arc<RoutingCache>,
    pub(super) faults: Arc<Faults>,
    pub(super) policy: Arc<WorkerPolicy>,
    pub(super) address: std::net::SocketAddr,
    pub(super) cancel: CancellationToken,
    pub(super) server: tokio::task::JoinHandle<()>,
    pub(super) receipts: mpsc::UnboundedReceiver<Receipt>,
    pub(super) activity_faults: Arc<Faults>,
    pub(super) start_faults: Arc<Faults>,
    pub(super) starts: Arc<Mutex<Vec<wire::StartActivityTaskRequest>>>,
    pub(super) activity_receipts: mpsc::UnboundedReceiver<ActivityReceipt>,
}
impl<R: RunRepository + 'static> fmt::Debug for Node<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

pub(super) fn placement() -> PlacementConfig {
    PlacementConfig {
        shard_count: 8,
        bundle_count: 8,
        partition_count: 8,
        hash_version: 1,
    }
}
pub(super) fn queue_home(queue: &QueueKey) -> ShardId {
    bundle_for_partition(
        workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    )
}

pub(super) async fn node<R: RunRepository + 'static>(
    store: Arc<R>,
    identity: &str,
    config: &tokeira_config::peer::PeerConfig,
    clock: Arc<TestClock>,
) -> Node<R> {
    node_with_activity(store, identity, config, clock, None, true).await
}

pub(super) async fn activity_node<R: RunRepository + 'static>(
    store: Arc<R>,
    identity: &str,
    config: &tokeira_config::peer::PeerConfig,
    clock: Arc<TestClock>,
    controller: &str,
) -> Node<R> {
    node_with_activity(store, identity, config, clock, Some(controller), true).await
}

pub(super) async fn activity_node_with_scanner<R: RunRepository + 'static>(
    store: Arc<R>,
    identity: &str,
    config: &tokeira_config::peer::PeerConfig,
    clock: Arc<TestClock>,
    controller: &str,
    scan_activity: bool,
) -> Node<R> {
    node_with_activity(
        store,
        identity,
        config,
        clock,
        Some(controller),
        scan_activity,
    )
    .await
}

async fn node_with_activity<R: RunRepository + 'static>(
    store: Arc<R>,
    identity: &str,
    config: &tokeira_config::peer::PeerConfig,
    clock: Arc<TestClock>,
    controller: Option<&str>,
    scan_activity: bool,
) -> Node<R> {
    let repo = Arc::new(Repository::new(store));
    if controller.is_none() {
        repo.activity.release();
    }
    let mut runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        identity.into(),
        "127.0.0.1:0".into(),
        false,
        None,
        clock.clone(),
    )
    .with_worker_deployment_repository(Arc::new(InMemoryStore::default().with_clock(clock)));
    if !scan_activity {
        // Retirement isolates publication from due eligibility so no unrelated
        // hint can cross the deliberate interval with no active queue owner.
        runtime.shutdown_activity_timeout_scanner().await.unwrap();
    }
    let runtime = Arc::new(runtime);
    let peer = Arc::new(
        super::bootstrap(Some(config))
            .unwrap()
            .with_clock(runtime.clock().clone()),
    );
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await
        .unwrap();
    let policy = Arc::new(WorkerPolicy::default());
    let interceptors = Arc::new(
        EdgeInterceptors::configured(namespaces.clone(), policy.clone(), false)
            .with_clock(runtime.clock().clone()),
    );
    let workflow = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
        Arc::new(RuntimeAdapter::new(runtime.clone())),
        Arc::new(tokeira_edge::InMemoryExecutionResolver::new()),
        Arc::new(tokeira_edge::EmptyVisibilityApi),
        repo.clone(),
        Arc::new(tokeira_edge::InMemoryOperatorApi::new("test", "test")),
        namespaces,
        interceptors.clone(),
        tokeira_edge::PollerRegistry::default(),
        tokeira_edge::PendingQueryStore::default(),
        runtime.buffered_queries(),
        runtime.broker(),
        tokeira_edge::LongPollGate::new(tokeira_edge::LongPollConfig::default()),
        Arc::new(tokeira_edge::LocalOnlyRouter),
        tokeira_edge::HistoryWaitRegistry::default(),
    );
    let service =
        WorkflowTaskDispatchGrpc::new(runtime.clone(), interceptors.clone(), workflow.clone())
            .with_peer(Some(peer.clone()));
    let faults = Arc::new(Faults::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let gate = FaultWire {
        inner: service.into_service(),
        faults: faults.clone(),
        path: OFFER_PATH,
    };
    let activity_faults = Arc::new(Faults::default());
    let start_faults = Arc::new(Faults::default());
    let starts = Arc::new(Mutex::new(Vec::new()));
    let activity_gate = ObserveStarts {
        inner: FaultWire {
            inner: FaultWire {
                inner: ActivityTaskDispatchGrpc::new(
                    runtime.clone(),
                    interceptors,
                    Some(peer.clone()),
                )
                .into_service(),
                faults: activity_faults.clone(),
                path: ACTIVITY_OFFER_PATH,
            },
            faults: start_faults.clone(),
            path: ACTIVITY_START_PATH,
        },
        requests: starts.clone(),
    };
    let public_service = ObserveStarts {
        inner: WorkflowServiceGrpc::new(workflow).into_service(),
        requests: starts.clone(),
    };
    let activity_enabled = controller.is_some();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(gate)
            .add_optional_service(activity_enabled.then_some(activity_gate))
            .add_service(public_service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown.cancelled())
            .await
            .unwrap();
    });
    let cache = Arc::new(RoutingCache::new(placement()));
    runtime.set_workflow_task_starter(Arc::new(RemoteWorkflowTaskStarter::new(cache.clone())));
    let (receipts, receive) = mpsc::unbounded_channel();
    runtime.set_workflow_offer_sink(Arc::new(ObservedSink {
        inner: PeerWorkflowOfferSink::new(cache.clone(), "http://127.0.0.1:1".into(), peer.clone()),
        receipts,
    }));
    let (activity_send, activity_receipts) = mpsc::unbounded_channel();
    if let Some(controller) = controller {
        let connector: PeerConnector = Arc::new(tonic::transport::Endpoint::connect_lazy);
        runtime.set_activity_task_starter(Arc::new(
            RemoteActivityTaskStarter::new(cache.clone(), peer.clone())
                .with_peer_connector(connector.clone()),
        ));
        runtime.set_activity_offer_sink(Arc::new(ObservedActivitySink {
            inner: PeerActivityOfferSink::new(cache.clone(), controller.into(), peer.clone())
                .with_peer_connector(connector),
            receipts: activity_send,
        }));
    }
    Node {
        activity_faults,
        start_faults,
        starts,
        activity_receipts,
        runtime,
        repo,
        peer,
        cache,
        faults,
        policy,
        address,
        cancel,
        server,
        receipts: receive,
    }
}
impl<R: RunRepository + 'static> Node<R> {
    pub(super) async fn activity_receipt(&mut self) -> ActivityReceipt {
        tokio::time::timeout(Duration::from_secs(3), self.activity_receipts.recv())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "activity ACK: {error}; owner loads={}, starts={:?}",
                    self.repo.loads.load(Ordering::SeqCst),
                    self.starts.lock().unwrap()
                )
            })
            .expect("activity sender closed")
    }

    pub(super) async fn receipt(&mut self) -> Receipt {
        tokio::time::timeout(Duration::from_secs(3), self.receipts.recv())
            .await
            .unwrap()
            .unwrap()
    }
    pub(super) async fn stop(self) {
        self.cancel.cancel();
        self.server.await.unwrap();
        let shutdown = self.runtime.shutdown_handle();
        shutdown.begin_shutdown();
        shutdown
            .wait(std::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
    pub(super) async fn client(&self) -> WorkflowTaskDispatchClient<tonic::transport::Channel> {
        WorkflowTaskDispatchClient::connect(format!("http://{}", self.address))
            .await
            .unwrap()
    }
    pub(super) async fn poll(
        &self,
        queue: &QueueKey,
        sticky: Option<&QueueKey>,
    ) -> tokeira_proto::workflowservice::PollWorkflowTaskQueueResponse {
        self.poll_versioned(queue, sticky, None).await
    }
    pub(super) async fn poll_versioned(
        &self,
        queue: &QueueKey,
        sticky: Option<&QueueKey>,
        build: Option<&str>,
    ) -> tokeira_proto::workflowservice::PollWorkflowTaskQueueResponse {
        let mut client = WorkflowServiceClient::connect(format!("http://{}", self.address))
            .await
            .unwrap();
        let mut request = tonic::Request::new(PollWorkflowTaskQueueRequest {
            namespace: "default".into(),
            identity: "integration-worker".into(),
            task_queue: Some(TaskQueue {
                name: queue.task_queue.0.clone(),
                kind: if sticky.is_some() { 2 } else { 1 },
                normal_name: sticky.map(|q| q.task_queue.0.clone()).unwrap_or_default(),
            }),
            deployment_options: build.map(|build| {
                tokeira_proto::public::temporal::api::deployment::v1::WorkerDeploymentOptions {
                    deployment_name: "deployment".into(),
                    build_id: build.into(),
                    worker_versioning_mode: 2,
                }
            }),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "authorization",
            "Bearer integration-worker".parse().unwrap(),
        );
        tokio::time::timeout(
            Duration::from_secs(3),
            client.poll_workflow_task_queue(request),
        )
        .await
        .unwrap()
        .unwrap()
        .into_inner()
    }
}

pub(super) fn credential() -> (tempfile::TempDir, tokeira_config::peer::PeerConfig) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap().join("peer.json");
    let printed = super::generate(&path).unwrap();
    let config = toml::from_str::<crate::TokeiraConfig>(&printed)
        .unwrap()
        .infrastructure
        .peer
        .unwrap();
    (directory, config)
}

pub(super) fn request(index: u32, clock: &TestClock) -> StartRequest {
    let mut dto = start_dto();
    dto.task_queue = format!("normal-{index}");
    let context = EdgeContext {
        request_id: tokeira_edge::request_id::RequestId::new("integration"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: clock.now(),
        is_long_poll: false,
    };
    let mut request =
        tokeira_edge::translate::to_internal::start_request(dto, &context, clock.now());
    let queue = normal_queue(&request);
    request.workflow_id = (0..100)
        .map(|n| tokeira_types::WorkflowId(format!("integration-{index}-{n}")))
        .find(|id| {
            execution_home_bundle(request.namespace_id.0.as_bytes(), id.0.as_bytes(), 8)
                != queue_home(&queue)
        })
        .unwrap();
    request.run_key = RunKey::derive(request.namespace_id, &request.workflow_id, request.run_id);
    request
}
pub(super) fn normal_queue(request: &StartRequest) -> QueueKey {
    QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}
pub(super) fn execution(request: &StartRequest) -> ShardId {
    execution_home_bundle(
        request.namespace_id.0.as_bytes(),
        request.workflow_id.0.as_bytes(),
        8,
    )
}
pub(super) fn route<R: RunRepository + 'static>(
    nodes: &[&Node<R>],
    homes: &[(ShardId, usize, ShardEpoch)],
) {
    let mut snapshot = RoutingSnapshot::new(placement());
    for (i, node) in nodes.iter().enumerate() {
        snapshot.node_endpoints.insert(
            IncarnationId(uuid::Uuid::from_u128(i as u128 + 1)),
            NodeEndpoint {
                host: node.address.ip().to_string(),
                port: node.address.port(),
            },
        );
    }
    for (home, i, epoch) in homes {
        snapshot.execution_bundle_owners.insert(
            *home,
            BundleOwner {
                node_id: IncarnationId(uuid::Uuid::from_u128(*i as u128 + 1)),
                epoch: *epoch,
            },
        );
    }
    for node in nodes {
        node.cache.replace_snapshot(snapshot.clone());
    }
}

fn start_dto() -> tokeira_edge::translate::StartWorkflowExecutionRequest {
    tokeira_edge::translate::StartWorkflowExecutionRequest {
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
        now: None,
    }
}
pub(super) fn offer_wire(
    req: &StartRequest,
    destination: ShardId,
) -> wire::OfferWorkflowTaskRequest {
    wire::OfferWorkflowTaskRequest {
        run_key: req.run_key.0.as_bytes().to_vec(),
        logical_seq: 1,
        execution_home: execution(req).0,
        destination_home: destination.0,
        namespace_id: req.namespace_id.0.as_bytes().to_vec(),
        selected_queue: Some(wire::OfferQueue {
            name: req.task_queue.0.clone(),
            ..Default::default()
        }),
        queue_kind: wire::OfferQueueKind::Normal as i32,
        routing: Some(wire::OfferRouting {
            mode: wire::OfferRoutingMode::Live as i32,
            ..Default::default()
        }),
        version_directive: Some(wire::OfferVersionDirective {
            behavior: wire::OfferVersionBehavior::Unversioned as i32,
            ..Default::default()
        }),
        scheduled_at: Some(prost_types::Timestamp {
            seconds: req.now.unix_timestamp(),
            nanos: req.now.nanosecond() as i32,
        }),
        workflow_id: req.workflow_id.0.clone(),
        ..Default::default()
    }
}
pub(super) fn authorized_offer(
    peer: &PeerOfferEndpoint,
    offer: wire::OfferWorkflowTaskRequest,
) -> tonic::Request<wire::OfferWorkflowTaskRequest> {
    let token = peer
        .signer()
        .token_at(peer.unix_seconds().unwrap())
        .unwrap();
    let mut request = tonic::Request::new(offer);
    request
        .metadata_mut()
        .insert("authorization", token.authorization().parse().unwrap());
    request
}

#[cfg(test)]
mod fault_barrier_tests {
    //! Socket-free checks for mode selection, completion and cancellation at the wire boundary.

    use super::*;
    use std::{convert::Infallible, future::poll_fn};

    #[tokio::test]
    async fn fault_barrier_drains_earlier_calls_only() {
        for mode in [0, 1, 2, 5] {
            let faults = Arc::new(Faults::default());
            faults.mode.store(mode, Ordering::SeqCst);
            let mut wire = FaultWire {
                inner: tower::service_fn(|_: HttpRequest<()>| async {
                    Ok::<_, Infallible>(HttpResponse::new(Body::empty()))
                }),
                faults: faults.clone(),
                path: ACTIVITY_OFFER_PATH,
            };
            let earlier = wire.call(
                HttpRequest::builder()
                    .uri(ACTIVITY_OFFER_PATH)
                    .body(())
                    .unwrap(),
            );
            let mut barrier = Box::pin(faults.set_and_drain(1));
            let observed = poll_fn(|cx| Poll::Ready(barrier.as_mut().poll(cx))).await;
            assert!(
                observed.is_pending(),
                "barrier before completion: {observed:?}"
            );
            let later = wire.call(
                HttpRequest::builder()
                    .uri(ACTIVITY_OFFER_PATH)
                    .body(())
                    .unwrap(),
            );
            earlier.await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), barrier)
                .await
                .unwrap();
            let active = faults.inside.lock().unwrap().active.clone();
            assert_eq!(active, BTreeMap::from([(1, 1)]), "wire calls: {active:?}");
            drop(later);
            let active = faults.inside.lock().unwrap().active.clone();
            assert!(
                active.is_empty(),
                "wire calls after unpolled cancellation: {active:?}"
            );
        }
    }

    #[tokio::test]
    async fn fault_barrier_tracks_calls_across_cancelled_waiters() {
        let faults = Arc::new(Faults::default());
        let mut wire = FaultWire {
            inner: tower::service_fn(|_: HttpRequest<()>| {
                std::future::pending::<Result<HttpResponse<Body>, Infallible>>()
            }),
            faults: faults.clone(),
            path: ACTIVITY_OFFER_PATH,
        };
        let mut earlier = wire.call(
            HttpRequest::builder()
                .uri(ACTIVITY_OFFER_PATH)
                .body(())
                .unwrap(),
        );
        let observed = poll_fn(|cx| Poll::Ready(earlier.as_mut().poll(cx))).await;
        assert!(
            observed.is_pending(),
            "wire before cancellation: {observed:?}"
        );
        let mut cancelled = Box::pin(faults.set_and_drain(2));
        let observed = poll_fn(|cx| Poll::Ready(cancelled.as_mut().poll(cx))).await;
        assert!(observed.is_pending(), "first barrier: {observed:?}");
        drop(cancelled);
        let mut barrier = Box::pin(faults.set_and_drain(1));
        let observed = poll_fn(|cx| Poll::Ready(barrier.as_mut().poll(cx))).await;
        assert!(observed.is_pending(), "replacement barrier: {observed:?}");
        drop(earlier);
        tokio::time::timeout(Duration::from_secs(1), barrier)
            .await
            .unwrap();
        let active = faults.inside.lock().unwrap().active.clone();
        assert!(
            active.is_empty(),
            "wire calls after cancellation: {active:?}"
        );
    }
}
