//! Generated real-wire checks of owner admission and native activity replies.

mod limits;
mod read_counts;

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use proptest::prelude::*;
use tokeira_auth::{
    AuthPrincipal, Claims, WorkerCallTarget, WorkerScope, WorkerScopeDecision,
    peer::{PeerMaterial, PeerVerifier},
};
use tokeira_kernel::LoadedRun;
use tokeira_proto::{
    common,
    enums::CommandType,
    public::temporal::api::{
        command::v1::{Command, ScheduleActivityTaskCommandAttributes, command::Attributes},
        taskqueue::v1::TaskQueue,
    },
    workflowservice,
};
use tokeira_runtime::{LaneConfig, TimerScannerConfig, WorkflowTimeoutScannerConfig};
use tokeira_storage::InMemoryStore;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    EdgeError, EdgeResult, InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace,
    interceptors::Authenticator,
};

#[derive(Debug, Default)]
struct WorkerAuth(AtomicUsize);

#[async_trait]
impl Authenticator for WorkerAuth {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        let credential = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        if !matches!(
            credential,
            Some("Bearer synthetic-worker" | "Bearer scoped-worker")
        ) {
            return Err(EdgeError::Unauthorized("worker credential required".into()));
        }
        self.0.fetch_add(1, Ordering::SeqCst);
        if credential == Some("Bearer scoped-worker") {
            return Ok(Some(Claims {
                subject: "scoped-worker".into(),
                worker_scope: Some(
                    WorkerScope::try_new(
                        "default".into(),
                        vec!["activity-q-0".into()],
                        "deployment".into(),
                        "build".into(),
                    )
                    .unwrap(),
                ),
                ..Default::default()
            }));
        }
        Ok(None)
    }

    async fn authorize_worker(
        &self,
        claims: Option<&Claims>,
        action: Action,
        namespace: Option<&str>,
        worker: WorkerCallTarget<'_>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        if let Some(scope) = claims.and_then(|claims| claims.worker_scope.as_ref())
            && !matches!(
                scope.authorize(
                    worker.operation,
                    namespace.unwrap_or_default(),
                    worker.target
                ),
                WorkerScopeDecision::Allow
            )
        {
            return Err(EdgeError::Forbidden {
                action: "PollActivityTaskQueue",
                namespace: namespace.map(str::to_owned),
            });
        }
        self.authorize(claims, action, namespace).await
    }
    async fn authorize(
        &self,
        _claims: Option<&Claims>,
        action: Action,
        namespace: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        if action != Action::PollActivityTaskQueue || namespace != Some("default") {
            return Err(EdgeError::Forbidden {
                action: "PollActivityTaskQueue",
                namespace: namespace.map(str::to_owned),
            });
        }
        Ok(None)
    }
}

fn context() -> EdgeContext {
    EdgeContext {
        request_id: crate::request_id::RequestId::new("activity-wire"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: OffsetDateTime::now_utc(),
        is_long_poll: false,
    }
}

fn placed_runtime<R: RunRepository + 'static>(
    store: Arc<R>,
    homes: u32,
    owner: &str,
) -> Arc<TokeiraRuntime<R>> {
    Arc::new(TokeiraRuntime::new_with_nexus_and_shards(
        store,
        1,
        LaneConfig {
            controller_managed_placement: homes > 1,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Arc::new(tokeira_runtime::NoopNexusHttpClient),
        Default::default(),
        homes,
        owner.into(),
        homes == 1,
    ))
}

fn queue_home(namespace: NamespaceId, name: &str, homes: u32) -> ShardId {
    tokeira_types::bundle_for_partition(
        tokeira_types::workflow_queue_partition(namespace, name, homes),
        homes,
    )
}

struct Fixture<R = InMemoryStore> {
    store: Arc<R>,
    runtime: Arc<TokeiraRuntime<R>>,
    peer: Arc<super::super::workflow_offer::PeerOfferEndpoint>,
    request: wire::StartActivityTaskRequest,
    address: std::net::SocketAddr,
    homes: u32,
    client: ActivityTaskDispatchClient<tonic::transport::Channel>,
    cancel: CancellationToken,
    accept: tokio::task::JoinHandle<()>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(data: Vec<u8>, header: Vec<u8>, priority: i32) -> Self {
        Self::with_homes(data, header, priority, 1).await
    }

    async fn with_homes(data: Vec<u8>, header: Vec<u8>, priority: i32, homes: u32) -> Self {
        let store = Arc::new(InMemoryStore::with_shard_count(homes));
        Self::with_store(store, data, header, priority, homes).await
    }
}

impl<R: RunRepository + tokeira_storage::LeaseRepository + 'static> Fixture<R> {
    async fn with_store(
        store: Arc<R>,
        data: Vec<u8>,
        header: Vec<u8>,
        priority: i32,
        homes: u32,
    ) -> Self {
        let runtime = placed_runtime(store.clone(), homes, "wire-execution");
        if homes > 1 {
            for home in 0..homes {
                runtime.acquire_shard(ShardId(home)).await.unwrap();
            }
        }
        let start = super::super::translate::start_request_to_edge(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".into(),
                workflow_id: format!("activity-wire-{}", Uuid::new_v4()),
                request_id: Uuid::new_v4().to_string(),
                workflow_type: Some(common::WorkflowType {
                    name: "workflow-type".into(),
                }),
                task_queue: Some(TaskQueue {
                    name: "workflow-q".into(),
                    ..Default::default()
                }),
                workflow_task_timeout: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let start = crate::translate::to_internal::start_request(start, &context());
        let run = start.run_key;
        let namespace_id = start.namespace_id;
        let execution_home = tokeira_types::execution_home_bundle(
            namespace_id.0.as_bytes(),
            start.workflow_id.0.as_bytes(),
            homes,
        );
        let activity_queue = (0..128)
            .map(|n| format!("activity-q-{n}"))
            .find(|queue| homes == 1 || queue_home(namespace_id, queue, homes) != execution_home)
            .unwrap();
        let queue = QueueKey {
            namespace_id,
            task_queue: start.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        runtime.start_workflow(start).await.unwrap();
        let wft = runtime
            .poll_workflow_task(
                queue,
                WorkerIdentity("workflow-worker".into()),
                Duration::from_secs(10),
            )
            .await
            .unwrap()
            .unwrap();
        let payload = common::Payload {
            metadata: BTreeMap::from([("encoding".into(), b"binary/plain".to_vec())]),
            data,
            external_payloads: Vec::new(),
        };
        let header = common::Header {
            fields: BTreeMap::from([(
                "trace".into(),
                common::Payload {
                    metadata: BTreeMap::new(),
                    data: header,
                    external_payloads: Vec::new(),
                },
            )]),
        };
        let completion = super::super::translate::respond_completed_request_to_edge(
            workflowservice::RespondWorkflowTaskCompletedRequest {
                task_token: crate::task_token::encode(wft.token, namespace_id).unwrap(),
                namespace: "default".into(),
                identity: "workflow-worker".into(),
                commands: vec![Command {
                    command_type: CommandType::ScheduleActivityTask as i32,
                    attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            activity_id: "activity-1".into(),
                            activity_type: Some(common::ActivityType {
                                name: "activity-type".into(),
                            }),
                            task_queue: Some(TaskQueue {
                                name: activity_queue.clone(),
                                ..Default::default()
                            }),
                            input: Some(common::Payloads {
                                payloads: vec![payload],
                            }),
                            header: Some(header),
                            start_to_close_timeout: Some(prost_types::Duration {
                                seconds: 60,
                                nanos: 0,
                            }),
                            schedule_to_close_timeout: Some(prost_types::Duration {
                                seconds: 300,
                                nanos: 0,
                            }),
                            retry_policy: Some(common::RetryPolicy {
                                initial_interval: Some(prost_types::Duration {
                                    seconds: 1,
                                    nanos: 0,
                                }),
                                backoff_coefficient: 2.0,
                                maximum_attempts: 4,
                                ..Default::default()
                            }),
                            priority: Some(common::Priority {
                                priority_key: priority,
                                fairness_key: "fair".into(),
                                fairness_weight: 1.5,
                            }),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .unwrap();
        runtime
            .complete_workflow_task(
                crate::translate::to_internal::workflow_task_completed_request(
                    completion,
                    &context(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let LoadedRun::Existing(state) = store.load_run(run).await.unwrap() else {
            panic!("run");
        };
        let activity = &state.activities["activity-1"];
        let request = wire::StartActivityTaskRequest {
            run_key: run.0.as_bytes().to_vec(),
            activity_id: activity.activity_id.clone(),
            schedule_event_id: activity.schedule_event_id,
            attempt: 1,
            stamp: activity.stamp,
            request_id: Uuid::new_v4().as_bytes().to_vec(),
            namespace: "default".into(),
            task_queue: activity_queue,
            deployment: None,
            build_id: None,
            dispatch_revision: 0,
            worker_identity: "activity-worker".into(),
            scoped_versioned: false,
        };
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let interceptors = Arc::new(EdgeInterceptors::configured(
            namespaces,
            Arc::new(WorkerAuth::default()),
            false,
        ));
        let material = PeerMaterial::generate().unwrap();
        let peer = Arc::new(super::super::workflow_offer::PeerOfferEndpoint::new(
            PeerVerifier::new(material.profile().clone()).unwrap(),
            material.signer(material.profile()).unwrap(),
        ));
        let service =
            ActivityTaskDispatchGrpc::new(runtime.clone(), interceptors, Some(peer.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let accept_cancel = cancel.clone();
        let (send, receive) = tokio::sync::mpsc::channel(4);
        let accept = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_cancel.cancelled() => break,
                    socket = listener.accept() => if send.send(socket.map(|(socket, _)| socket)).await.is_err() { break; },
                }
            }
        });
        let server_cancel = cancel.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service.into_service())
                .serve_with_incoming_shutdown(
                    ReceiverStream::new(receive),
                    server_cancel.cancelled(),
                )
                .await
                .unwrap();
        });
        let client = ActivityTaskDispatchClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        Self {
            store,
            runtime,
            peer,
            request,
            address,
            homes,
            client,
            cancel,
            accept,
            server,
        }
    }

    fn authorized(&self) -> Request<wire::StartActivityTaskRequest> {
        let mut request = Request::new(self.request.clone());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        request.metadata_mut().insert(
            PEER_HEADER,
            self.peer
                .signer()
                .activity_start_token_at(now)
                .unwrap()
                .authorization()
                .parse()
                .unwrap(),
        );
        request
            .metadata_mut()
            .insert("authorization", "Bearer synthetic-worker".parse().unwrap());
        request
    }

    fn starter(&self, home: ShardId) -> RemoteActivityTaskStarter {
        let placement = tokeira_types::PlacementConfig {
            shard_count: self.homes,
            bundle_count: self.homes,
            partition_count: self.homes,
            hash_version: 1,
        };
        let cache = Arc::new(RoutingCache::new(placement));
        let mut snapshot = tokeira_types::RoutingSnapshot::new(placement);
        let node = tokeira_types::IncarnationId(Uuid::new_v4());
        snapshot.execution_bundle_owners.insert(
            home,
            tokeira_types::BundleOwner {
                node_id: node,
                epoch: tokeira_types::ShardEpoch::ZERO,
            },
        );
        snapshot.node_endpoints.insert(
            node,
            tokeira_types::NodeEndpoint {
                host: self.address.ip().to_string(),
                port: self.address.port(),
            },
        );
        cache.replace_snapshot(snapshot);
        RemoteActivityTaskStarter::new(cache, self.peer.clone())
    }

    async fn remote_replay(&self) -> ActivityStartOutcome {
        let starter = self.starter(ShardId(0));
        let request = ActivityTaskStartRequest {
            offered: DispatchableActivityTask {
                run_key: self.run(),
                queue: QueueKey {
                    namespace_id: crate::translate::to_internal::namespace_id_for("default"),
                    task_queue: TaskQueueName(self.request.task_queue.clone()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                },
                activity_id: self.request.activity_id.clone(),
                schedule_event_id: self.request.schedule_event_id,
                attempt: self.request.attempt,
                stamp: self.request.stamp,
                dispatch_revision: self.request.dispatch_revision,
                priority: None,
                order: None,
            },
            request_id: Uuid::from_slice(&self.request.request_id).unwrap(),
            worker: WorkerIdentity(self.request.worker_identity.clone()),
        };
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer synthetic-worker".parse().unwrap());
        FORWARDED_ACTIVITY_POLL
            .scope(
                ForwardedActivityPoll {
                    headers,
                    namespace: "default".into(),
                    scoped_versioned: false,
                },
                starter.start(
                    ShardId(0),
                    request,
                    tokio::time::Instant::now() + Duration::from_secs(10),
                ),
            )
            .await
            .unwrap()
    }

    fn run(&self) -> RunKey {
        RunKey(Uuid::from_slice(&self.request.run_key).unwrap())
    }

    async fn stop(self) {
        drop(self.client);
        self.cancel.cancel();
        self.accept.await.unwrap();
        self.server.await.unwrap();
        let shutdown = self.runtime.shutdown_handle();
        shutdown.begin_shutdown();
        shutdown
            .wait(std::time::Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 12: queue-home discovery and an independently leased owner preserve native payloads.
    #[test]
    fn distinct_activity_owners_deliver_over_wire(data in proptest::collection::vec(any::<u8>(), 0..4096), header in proptest::collection::vec(any::<u8>(), 0..256), priority in 1i32..6) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let fixture = Fixture::with_homes(data.clone(), header.clone(), priority, 8).await;
            let LoadedRun::Existing(before) = fixture.store.load_run(fixture.run()).await.unwrap() else { panic!("run"); };
            let namespace = before.namespace_id;
            let home = tokeira_types::execution_home_bundle(namespace.0.as_bytes(), before.workflow_id.0.as_bytes(), 8);
            let queue_home = queue_home(namespace, &fixture.request.task_queue, 8);
            prop_assert_ne!(home, queue_home);
            tokeira_runtime::membership::MembershipShardLifecycle::relinquish_shard(fixture.runtime.as_ref(), queue_home).await.unwrap();
            let queue_runtime = placed_runtime(fixture.store.clone(), 8, "wire-queue");
            queue_runtime.acquire_shard(queue_home).await.unwrap();
            prop_assert!(!queue_runtime.active_shards().contains(&home));
            prop_assert!(!fixture.runtime.active_shards().contains(&queue_home));
            queue_runtime.set_activity_task_starter(Arc::new(fixture.starter(home)));
            let queue = QueueKey { namespace_id: namespace, task_queue: TaskQueueName(fixture.request.task_queue.clone()), task_kind: TaskKind::Activity, deployment: None, build_id: None };
            let mut headers = HeaderMap::new();
            headers.insert("authorization", "Bearer synthetic-worker".parse().unwrap());
            let started = FORWARDED_ACTIVITY_POLL.scope(ForwardedActivityPoll {
                headers, namespace: "default".into(), scoped_versioned: false,
            }, queue_runtime.poll_activity_task(queue.clone(), WorkerIdentity("remote-worker".into()), Duration::from_secs(10))).await.unwrap().unwrap();
            prop_assert_eq!(&started.input.0[0].data, &data);
            prop_assert_eq!(&started.header.as_ref().unwrap().0["trace"].data, &header);
            prop_assert_eq!(started.priority.unwrap().priority_key, priority);
            prop_assert_eq!(started.workflow_type.as_str(), "workflow-type");
            let LoadedRun::Existing(after) = fixture.store.load_run(fixture.run()).await.unwrap() else { panic!("run"); };
            prop_assert_eq!(after.transition_seq, before.transition_seq.next());
            prop_assert_eq!(after.activity_start_receipts.len(), 1);
            prop_assert!(fixture.store.list_all_dispatchable_activity_tasks(&queue, 10).await.unwrap().is_empty());
            let shutdown = queue_runtime.shutdown_handle();
            shutdown.begin_shutdown();
            shutdown.wait(std::time::Instant::now() + Duration::from_secs(10)).await.unwrap();
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 12: binary payloads and optional replay fields survive real HTTP/2.
    #[test]
    fn native_activity_wire_preserves_binary_response(data in proptest::collection::vec(any::<u8>(), 0..2048), header in proptest::collection::vec(any::<u8>(), 0..128), priority in 1i32..6) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut fixture = Fixture::new(data.clone(), header.clone(), priority).await;
            let LoadedRun::Existing(before) = fixture.store.load_run(fixture.run()).await.unwrap() else { panic!("run"); };
            let request = fixture.authorized();
            let fresh = fixture.client.start_activity_task(request).await.unwrap().into_inner();
            prop_assert!(!fresh.replayed);
            let task = fresh.task.unwrap();
            prop_assert_eq!(&task.input.as_ref().unwrap().payloads[0].data, &data);
            prop_assert_eq!(&task.input.as_ref().unwrap().payloads[0].metadata["encoding"], b"binary/plain");
            prop_assert_eq!(&task.header.as_ref().unwrap().fields["trace"].data, &header);
            prop_assert_eq!(task.priority.as_ref().unwrap().priority_key, priority);
            prop_assert_eq!(task.workflow_type.as_ref().unwrap().name.as_str(), "workflow-type");
            prop_assert_eq!(task.workflow_namespace.as_str(), "default");
            prop_assert!(task.retry_policy.is_some());
            let LoadedRun::Existing(started) = fixture.store.load_run(fixture.run()).await.unwrap() else { panic!("run"); };
            prop_assert_eq!(started.transition_seq, before.transition_seq.next());
            let request = fixture.authorized();
            let replay = fixture.client.start_activity_task(request).await.unwrap().into_inner();
            prop_assert!(replay.replayed);
            let replay = replay.task.unwrap();
            prop_assert_eq!(replay.input, task.input);
            prop_assert_eq!(replay.header, task.header);
            prop_assert_eq!(replay.priority, task.priority);
            prop_assert_eq!(replay.started_time, task.started_time);
            prop_assert_eq!(replay.task_token, task.task_token);
            prop_assert_eq!(replay.attempt, 1);
            prop_assert!(replay.workflow_type.is_none() && replay.workflow_namespace.is_empty() && replay.retry_policy.is_none() && replay.heartbeat_details.is_none());
            let ActivityStartOutcome::Replayed(remote) = fixture.remote_replay().await else { panic!("remote replay"); };
            prop_assert_eq!(&remote.input.0[0].data, &data);
            prop_assert_eq!(&remote.header.as_ref().unwrap().0["trace"].data, &header);
            prop_assert_eq!(remote.priority.as_ref().unwrap().priority_key, priority);
            prop_assert_eq!(fixture.store.load_run(fixture.run()).await.unwrap(), LoadedRun::Existing(started));
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 9: invalid peer, worker, coordinates or budgets cannot mutate authority.
    #[test]
    fn activity_wire_admission_precedes_mutation(mutation in 0u8..19) {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut fixture = Fixture::new(vec![0, 255], vec![128], 1).await;
            let before = fixture.store.load_run(fixture.run()).await.unwrap();
            let mut request = fixture.authorized();
            match mutation {
                0 => { request.metadata_mut().remove(PEER_HEADER); }
                1 => { let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(); request.metadata_mut().insert(PEER_HEADER, fixture.peer.signer().token_at(now).unwrap().authorization().parse().unwrap()); }
                2 => { request.metadata_mut().remove("authorization"); }
                3 => request.get_mut().run_key.pop().map(|_| ()).unwrap(),
                4 => request.get_mut().attempt = 0,
                5 => request.get_mut().stamp = u64::MAX,
                6 => { let duplicate = request.metadata().get(PEER_HEADER).unwrap().clone(); request.metadata_mut().append(PEER_HEADER, duplicate); }
                7 => { request.metadata_mut().insert("x-large", "a".repeat(9 * 1024).parse().unwrap()); }
                8 => request.get_mut().dispatch_revision = -1,
                9 => request.get_mut().request_id = vec![0; 16],
                10 => { request.metadata_mut().insert("authorization", "Bearer wrong-worker".parse().unwrap()); }
                11 => { request.get_mut().activity_id = "a".repeat(MAX_REQUEST + 1); }
                12 => { request.get_mut().namespace = "foreign".into(); }
                13 => { let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(); request.metadata_mut().insert(PEER_HEADER, fixture.peer.signer().outbound_token_at(now).unwrap().authorization().parse().unwrap()); }
                14 => {
                    let material = PeerMaterial::generate().unwrap();
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                    request.metadata_mut().insert(PEER_HEADER, material.signer(material.profile()).unwrap().activity_start_token_at(now).unwrap().authorization().parse().unwrap());
                }
                _ => {
                    request.metadata_mut().insert("authorization", "Bearer scoped-worker".parse().unwrap());
                    request.get_mut().scoped_versioned = mutation != 15;
                    request.get_mut().deployment = Some(if mutation == 16 { "foreign" } else { "deployment" }.into());
                    request.get_mut().build_id = Some(if mutation == 17 { "foreign" } else { "build" }.into());
                    if mutation == 18 { request.get_mut().task_queue = "foreign".into(); }
                }
            }
            let result = fixture.client.start_activity_task(request).await;
            prop_assert!(result.is_err());
            prop_assert_eq!(fixture.store.load_run(fixture.run()).await.unwrap(), before);
            fixture.stop().await;
            Ok(())
        })?;
    }
}
