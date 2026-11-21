//! Hinted polling through the generated client and production authorization gate.
//! In-process gRPC isolates routing and commit faults from socket availability.

use std::sync::Mutex;

use tokeira_clock::Clock;
use tokeira_storage::read_probe::{ReadProbe, ReadReport};

use super::{
    queue_home_read_baseline::{measured_runtime, release, stop},
    *,
};

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }
}

fn peer() -> Arc<super::super::super::workflow_offer::PeerOfferEndpoint> {
    let material = PeerMaterial::generate().unwrap();
    Arc::new(
        super::super::super::workflow_offer::PeerOfferEndpoint::new(
            PeerVerifier::new(material.profile().clone()).unwrap(),
            material.signer(material.profile()).unwrap(),
        )
        .with_clock(Arc::new(FixedClock)),
    )
}

async fn interceptors(auth: Arc<WorkerAuth>) -> Arc<EdgeInterceptors> {
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await
        .unwrap();
    Arc::new(EdgeInterceptors::configured(namespaces, auth, false))
}

fn authorized(
    body: wire::StartActivityTaskRequest,
    peer: &super::super::super::workflow_offer::PeerOfferEndpoint,
) -> Request<wire::StartActivityTaskRequest> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        PEER_HEADER,
        peer.signer()
            .activity_start_token_at(peer.unix_seconds().unwrap())
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

#[derive(Debug)]
struct WireStarter {
    owner: Arc<TokeiraRuntime<InMemoryStore>>,
    wrong: Arc<TokeiraRuntime<InMemoryStore>>,
    home: ShardId,
    peer: Arc<super::super::super::workflow_offer::PeerOfferEndpoint>,
    auth: Arc<WorkerAuth>,
    requests: Mutex<Vec<(ShardId, Uuid)>>,
    lose_reply: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl ActivityTaskStarter for WireStarter {
    async fn start(
        &self,
        home: ShardId,
        request: ActivityTaskStartRequest,
        _: tokio::time::Instant,
    ) -> Result<ActivityStartOutcome> {
        self.requests
            .lock()
            .unwrap()
            .push((home, request.request_id));
        let node = if home == self.home {
            self.owner.clone()
        } else {
            self.wrong.clone()
        };
        let service = ActivityTaskDispatchGrpc::new(
            node,
            interceptors(self.auth.clone()).await,
            Some(self.peer.clone()),
        );
        let mut client = ActivityTaskDispatchClient::new(service.into_service());
        let task = &request.offered;
        let body = wire::StartActivityTaskRequest {
            run_key: task.run_key.0.as_bytes().to_vec(),
            activity_id: task.activity_id.clone(),
            schedule_event_id: task.schedule_event_id,
            attempt: task.attempt,
            stamp: task.stamp,
            request_id: request.request_id.as_bytes().to_vec(),
            namespace: "default".into(),
            task_queue: task.queue.task_queue.0.clone(),
            deployment: task.queue.deployment.as_ref().map(|v| v.0.clone()),
            build_id: task.queue.build_id.as_ref().map(|v| v.0.clone()),
            dispatch_revision: task.dispatch_revision,
            worker_identity: request.worker.0.clone(),
            scoped_versioned: false,
        };
        let response = match client
            .start_activity_task(authorized(body, &self.peer))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == Code::Unavailable => {
                return Err(RetryableActivityTaskStart(status.to_string()).into());
            }
            Err(status) => return Err(status.into()),
        };
        if response.task.is_some() && self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(RetryableActivityTaskStart("lost committed reply".into()).into());
        }
        decode_response(response, &request, "default")
    }
}

fn count(report: &ReadReport, phase: &'static str) -> usize {
    report.counts.get(&(phase, "logical")).copied().unwrap_or(0)
}

async fn admit(
    node: Arc<TokeiraRuntime<InMemoryStore>>,
    peer: Arc<super::super::super::workflow_offer::PeerOfferEndpoint>,
    offer: &tokeira_runtime::ActivityQueueOffer,
    home: ShardId,
) {
    let service = ActivityTaskDispatchGrpc::new(
        node,
        interceptors(Arc::new(WorkerAuth::default())).await,
        Some(peer.clone()),
    );
    let mut client = ActivityTaskDispatchClient::new(service.into_service());
    let mut request = Request::new(super::super::offer::encode(offer, home));
    request.metadata_mut().insert(
        "authorization",
        peer.signer()
            .token_at(peer.unix_seconds().unwrap())
            .unwrap()
            .authorization()
            .parse()
            .unwrap(),
    );
    assert_eq!(
        super::super::offer::outcome(client.offer_activity_task(request).await.unwrap().get_ref())
            .unwrap(),
        tokeira_runtime::ActivityOfferOutcome::Admitted
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))), ..ProptestConfig::default() })]

    // Scoped workers retain exact versioned origins, while no-peer assembly rejects the internal route before mutation.
    #[test]
    fn activity_scoped_start_and_no_peer_preservation(data in prop::collection::vec(any::<u8>(), 0..64)) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let clock: Arc<dyn Clock> = Arc::new(FixedClock);
            let repo = Arc::new(InMemoryStore::default().with_clock(clock.clone()));
            let owner = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
                repo.clone(), 1, LaneConfig::default(), Default::default(), Default::default(), Default::default(), Default::default(), Default::default(),
                Arc::new(tokeira_runtime::NoopNexusHttpClient), Default::default(), 1, "local".into(), "local".into(), true, None, clock,
            ));
            let mut body = seed(repo.as_ref(), &owner, data.clone(), data.clone(), 1, 1).await;
            body.scoped_versioned = true; body.deployment = Some("deployment".into()); body.build_id = Some("build".into());
            let run = RunKey(Uuid::from_slice(&body.run_key).unwrap());
            let before = repo.load_run(run).await.unwrap();
            let peer = peer();
            let auth = Arc::new(WorkerAuth::default());
            let mut absent = ActivityTaskDispatchClient::new(ActivityTaskDispatchGrpc::new(owner.clone(), interceptors(auth.clone()).await, None).into_service());
            prop_assert_eq!(absent.start_activity_task(authorized(body.clone(), &peer)).await.unwrap_err().code(), Code::Unimplemented);
            prop_assert_eq!(&repo.load_run(run).await.unwrap(), &before);
            let mut client = ActivityTaskDispatchClient::new(ActivityTaskDispatchGrpc::new(owner.clone(), interceptors(auth.clone()).await, Some(peer.clone())).into_service());
            for replay in [false, true] {
                let mut request = authorized(body.clone(), &peer);
                request.metadata_mut().insert("authorization", "Bearer scoped-worker".parse().unwrap());
                let response = client.start_activity_task(request).await.unwrap().into_inner();
                prop_assert_eq!(response.replayed, replay);
                let request = ActivityTaskStartRequest {
                    offered: DispatchableActivityTask { run_key: run, activity_id: body.activity_id.clone(), schedule_event_id: body.schedule_event_id, attempt: body.attempt, stamp: body.stamp,
                        queue: QueueKey { namespace_id: match &before { LoadedRun::Existing(state) => state.namespace_id, LoadedRun::Absent => panic!("run") }, task_queue: TaskQueueName(body.task_queue.clone()), task_kind: TaskKind::Activity, deployment: Some(DeploymentId("deployment".into())), build_id: Some(BuildId("build".into())) }, dispatch_revision: 0, priority: None, order: None },
                    request_id: Uuid::from_slice(&body.request_id).unwrap(), worker: WorkerIdentity(body.worker_identity.clone()),
                };
                let started = match decode_response(response, &request, "default").unwrap() { ActivityStartOutcome::Started(task) | ActivityStartOutcome::Replayed(task) => task, other => panic!("missing scoped start: {other:?}") };
                prop_assert_eq!(started.origin.deployment, DeploymentId("deployment".into()));
                prop_assert_eq!(started.origin.build_id, BuildId("build".into()));
                prop_assert_eq!(&started.input.0[0].data, &data);
                prop_assert_eq!(&started.header.as_ref().unwrap().0["trace"].data, &data);
            }
            prop_assert_eq!(auth.0.load(Ordering::SeqCst), 2);
            stop(&owner).await;
            Ok(())
        })?;
    }

    // A hint never replaces owner validation; every refusal, retry and replay read is charged.
    #[test]
    fn hinted_activity_start_reads_and_replay(warm in any::<bool>(), conflict in any::<bool>(), hint in 0u8..4, lost in any::<bool>(), data in prop::collection::vec(any::<u8>(), 0..64)) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let clock: Arc<dyn Clock> = Arc::new(FixedClock);
            let repo = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
            let seed_node = measured_runtime(repo.clone(), "seed", clock.clone());
            for home in 0..8 { seed_node.acquire_shard(ShardId(home)).await.unwrap(); }
            let body = seed(repo.as_ref(), &seed_node, data.clone(), data.clone(), 2, 8).await;
            let run = RunKey(Uuid::from_slice(&body.run_key).unwrap());
            let LoadedRun::Existing(before) = repo.load_run(run).await.unwrap() else { panic!("run") };
            let home = tokeira_types::execution_home_bundle(before.namespace_id.0.as_bytes(), before.workflow_id.0.as_bytes(), 8);
            let destination = queue_home(before.namespace_id, &body.task_queue, 8);
            for id in 0..8 { release(&repo, &seed_node, ShardId(id)).await; }
            stop(&seed_node).await;
            let owner = measured_runtime(repo.clone(), "owner", clock.clone());
            owner.acquire_shard(home).await.unwrap();
            let matching = measured_runtime(repo.clone(), "matching", clock.clone());
            matching.acquire_shard(destination).await.unwrap();
            if warm {
                owner.submit(run, tokeira_kernel::Command::Signal(tokeira_kernel::SignalRequest {
                    signal_name: "warm".into(), input: Default::default(), header: None, links: Vec::new(),
                    request: tokeira_types::RequestContext::unattributed(clock.now()), now: clock.now(),
                })).await.unwrap();
            }
            let peer = peer();
            let auth = Arc::new(WorkerAuth::default());
            let starter = Arc::new(WireStarter { owner: owner.clone(), wrong: matching.clone(), home, peer: peer.clone(), auth: auth.clone(), requests: Mutex::new(Vec::new()), lose_reply: std::sync::atomic::AtomicBool::new(lost) });
            matching.set_activity_task_starter(starter.clone());
            let offered_home = match hint {
                1 => destination,
                2 => (0..8).map(ShardId).find(|id| *id != home && *id != destination).unwrap(),
                _ => home,
            };
            let task = DispatchableActivityTask {
                run_key: run, queue: QueueKey { namespace_id: before.namespace_id, task_queue: TaskQueueName(body.task_queue.clone()), task_kind: TaskKind::Activity, deployment: None, build_id: None },
                activity_id: body.activity_id.clone(), schedule_event_id: body.schedule_event_id, attempt: body.attempt, stamp: body.stamp, dispatch_revision: 0, priority: None, order: None,
            };
            if hint == 3 { matching.activity_broker().publish_activity_task(task.clone(), None).await.unwrap(); }
            else { admit(matching.clone(), peer, &tokeira_runtime::ActivityQueueOffer { task: task.clone(), execution_home: offered_home, dispatch_at: clock.now(), expires_at: clock.now() + time::Duration::seconds(5) }, destination).await; }
            let probe = ReadProbe::start(run, "hinted-poll");
            if conflict { probe.inject_conflicts(1); }
            let started = matching.poll_activity_task(task.queue.clone(), WorkerIdentity("hint-worker".into()), Duration::from_secs(1)).await.unwrap().unwrap();
            let report = probe.report(); drop(probe);
            let refused = usize::from(hint == 1 || hint == 2);
            prop_assert_eq!(count(&report, "queue-home/activity-owner"), usize::from(hint != 0) + usize::from(lost));
            prop_assert_eq!(count(&report, "execution-owner/activity-start"), 1 + refused + usize::from(conflict) + usize::from(lost));
            prop_assert_eq!(count(&report, "execution-owner/activity-replay"), usize::from(lost));
            prop_assert!(report.counts.keys().all(|(_, op)| !op.starts_with("sql/")));
            let attempts = starter.requests.lock().unwrap().clone();
            prop_assert_eq!(attempts.len(), 1 + usize::from(hint == 2) + usize::from(lost));
            prop_assert!(attempts.iter().all(|(_, id)| *id == attempts[0].1));
            prop_assert_eq!(auth.0.load(Ordering::SeqCst), attempts.len());
            prop_assert_eq!(&started.input.0[0].data, &data);
            prop_assert_eq!(&started.header.as_ref().unwrap().0["trace"].data, &data);
            prop_assert_eq!(started.replayed, lost);
            let LoadedRun::Existing(after) = repo.load_run(run).await.unwrap() else { panic!("run") };
            prop_assert_eq!(after.transition_seq.0, before.transition_seq.0 + u64::from(warm) + 1);
            prop_assert_eq!(after.activity_start_receipts.len(), 1);
            stop(&matching).await; stop(&owner).await;
            Ok(())
        })?;
    }

    // Forged coordinates and credentials cannot consume a start; the winning receipt binds every replay coordinate.
    #[test]
    fn hinted_activity_wire_gates_and_competing_starts(delta in 1u32..100, data in prop::collection::vec(any::<u8>(), 0..64), conflict in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let clock: Arc<dyn Clock> = Arc::new(FixedClock);
            let repo = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
            let owner = measured_runtime(repo.clone(), "wire-owner", clock);
            for home in 0..8 { owner.acquire_shard(ShardId(home)).await.unwrap(); }
            let body = seed(repo.as_ref(), &owner, data.clone(), data.clone(), 1, 8).await;
            let run = RunKey(Uuid::from_slice(&body.run_key).unwrap());
            let before = repo.load_run(run).await.unwrap();
            let peer = peer();
            let service = ActivityTaskDispatchGrpc::new(owner.clone(), interceptors(Arc::new(WorkerAuth::default())).await, Some(peer.clone()));
            let mut client = ActivityTaskDispatchClient::new(service.into_service());
            let namespaces = Arc::new(InMemoryNamespaceCache::new());
            let mut foreign = ResolvedNamespace::active("default");
            foreign.namespace_id = Some(Uuid::new_v4().to_string());
            namespaces.insert(foreign).await.unwrap();
            let mut wrong_namespace = ActivityTaskDispatchClient::new(ActivityTaskDispatchGrpc::new(
                owner.clone(), Arc::new(EdgeInterceptors::configured(namespaces, Arc::new(WorkerAuth::default()), false)), Some(peer.clone()),
            ).into_service());
            let probe = ReadProbe::start(run, "owner-namespace-refusal");
            prop_assert!(wrong_namespace.start_activity_task(authorized(body.clone(), &peer)).await.unwrap().get_ref().task.is_none());
            let report = probe.report(); drop(probe);
            prop_assert_eq!(count(&report, "execution-owner/activity-start"), 1);
            prop_assert_eq!(repo.load_run(run).await.unwrap(), before.clone());
            for mutation in 0..12 {
                let mut request = authorized(body.clone(), &peer);
                match mutation {
                    0 => request.get_mut().schedule_event_id += i64::from(delta),
                    1 => request.get_mut().attempt += delta,
                    2 => request.get_mut().stamp += u64::from(delta),
                    3 => request.get_mut().task_queue.push_str("-forged"),
                    4 => request.get_mut().namespace = "foreign".into(),
                    5 => { request.metadata_mut().remove(PEER_HEADER); },
                    6 => { request.metadata_mut().remove("authorization"); },
                    7 => { request.metadata_mut().insert("padding", "x".repeat(8192 + delta as usize).parse().unwrap()); },
                    8 => request.get_mut().worker_identity = "x".repeat(MAX_REQUEST + delta as usize),
                    _ => {
                        request.metadata_mut().insert("authorization", "Bearer scoped-worker".parse().unwrap());
                        request.get_mut().scoped_versioned = true;
                        request.get_mut().deployment = Some(if mutation == 9 { "foreign" } else { "deployment" }.into());
                        request.get_mut().build_id = Some(if mutation == 10 { "foreign" } else { "build" }.into());
                        request.get_mut().task_queue = if mutation == 11 { "foreign" } else { "activity-q-0" }.into();
                    }
                }
                let probe = ReadProbe::start(run, format!("wire-refusal-{mutation}"));
                let response = client.start_activity_task(request).await;
                let report = probe.report(); drop(probe);
                if mutation < 4 { prop_assert!(response.unwrap().get_ref().task.is_none()); }
                else { prop_assert!(response.is_err()); }
                prop_assert_eq!(count(&report, "execution-owner/activity-start"), usize::from(mutation < 4));
                prop_assert_eq!(count(&report, "queue-home/activity-owner"), 0);
                prop_assert_eq!(repo.load_run(run).await.unwrap(), before.clone());
            }
            let mut competitor = body.clone(); competitor.request_id = Uuid::new_v4().as_bytes().to_vec();
            let mut other = client.clone();
            let probe = ReadProbe::start(run, "wire-competition");
            if conflict { probe.inject_conflicts(1); }
            let (left, right) = tokio::join!(client.start_activity_task(authorized(body.clone(), &peer)), other.start_activity_task(authorized(competitor.clone(), &peer)));
            let left = left.unwrap().into_inner(); let right = right.unwrap().into_inner();
            prop_assert_eq!(usize::from(left.task.is_some()) + usize::from(right.task.is_some()), 1);
            let report = probe.report(); drop(probe);
            prop_assert_eq!(count(&report, "execution-owner/activity-start"), 2 + usize::from(conflict));
            let winner = if left.task.is_some() { body } else { competitor };
            let committed = repo.load_run(run).await.unwrap();
            let probe = ReadProbe::start(run, "wire-replay");
            let replay = client.start_activity_task(authorized(winner.clone(), &peer)).await.unwrap().into_inner();
            let report = probe.report(); drop(probe);
            prop_assert!(replay.replayed);
            prop_assert_eq!(count(&report, "execution-owner/activity-start"), 1);
            prop_assert_eq!(count(&report, "execution-owner/activity-replay"), 1);
            let reply = replay.task.unwrap();
            prop_assert_eq!(&reply.input.unwrap().payloads[0].data, &data);
            prop_assert_eq!(&reply.header.unwrap().fields["trace"].data, &data);
            prop_assert!(reply.workflow_type.is_none() && reply.retry_policy.is_none() && reply.heartbeat_details.is_none());
            for mutation in 0..5 {
                let mut forged = winner.clone();
                match mutation {
                    0 => forged.deployment = Some("foreign".into()),
                    1 => forged.build_id = Some("foreign".into()),
                    2 => forged.dispatch_revision += i64::from(delta),
                    3 => forged.worker_identity.push('x'),
                    _ => forged.stamp += u64::from(delta),
                }
                let probe = ReadProbe::start(run, "forged-replay");
                prop_assert!(client.start_activity_task(authorized(forged, &peer)).await.unwrap().get_ref().task.is_none());
                let report = probe.report(); drop(probe);
                prop_assert_eq!(count(&report, "execution-owner/activity-start"), 1);
                prop_assert_eq!(count(&report, "execution-owner/activity-replay"), 0);
            }
            prop_assert_eq!(&repo.load_run(run).await.unwrap(), &committed);
            let LoadedRun::Existing(after) = committed else { panic!("run") };
            let LoadedRun::Existing(before) = before else { panic!("run") };
            prop_assert_eq!(after.transition_seq, before.transition_seq.next());
            prop_assert_eq!(after.activity_start_receipts.len(), 1);
            stop(&owner).await;
            Ok(())
        })?;
    }
}
