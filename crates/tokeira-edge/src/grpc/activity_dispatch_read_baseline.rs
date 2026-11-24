//! Activity run-load budgets from dequeue through an authenticated remote commit.
//!
//! Both nodes share the in-memory repository, so physical SQL is explicitly empty.
//! The existing per-run observer follows the real gRPC call and distinguishes
//! routing, authoritative start, discovery and unscoped/background work.

use proptest::test_runner::{
    Config, FileFailurePersistence, RngAlgorithm, TestError, TestRng, TestRunner,
};
use tokeira_clock::Clock;
use tokeira_kernel::{Command as KernelCommand, SignalRequest};
use tokeira_storage::{
    LeaseRepository,
    read_probe::{ReadProbe, ReadReport},
};
use tokeira_types::RequestContext;

use super::*;

#[derive(Debug)]
struct StartClock(OffsetDateTime);

impl Clock for StartClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

pub(super) fn measured_runtime(
    repo: Arc<InMemoryStore>,
    identity: &str,
    clock: Arc<dyn Clock>,
) -> Arc<TokeiraRuntime<InMemoryStore>> {
    Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo,
        1,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Arc::new(tokeira_runtime::NoopNexusHttpClient),
        Default::default(),
        8,
        identity.into(),
        identity.into(),
        false,
        None,
        clock,
    ))
}

pub(super) async fn release(
    repo: &InMemoryStore,
    runtime: &TokeiraRuntime<InMemoryStore>,
    home: ShardId,
) {
    tokeira_runtime::membership::MembershipShardLifecycle::relinquish_shard(runtime, home)
        .await
        .unwrap();
    if let Some(lease) = repo
        .list_bundle_leases()
        .await
        .unwrap()
        .into_iter()
        .find(|lease| lease.bundle_id == home && lease.owner_node_id.is_some())
    {
        repo.relinquish_bundle(home, lease.owner_node_id.unwrap(), lease.epoch)
            .await
            .unwrap();
    }
}

pub(super) async fn stop(runtime: &TokeiraRuntime<InMemoryStore>) {
    let shutdown = runtime.shutdown_handle();
    shutdown.begin_shutdown();
    shutdown
        .wait(std::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

async fn case(warm: bool, conflict: bool, value: u8, hinted: bool) -> ReadReport {
    let fixture = Fixture::with_homes(vec![value, 0, 255], vec![255, value], 3, 8).await;
    let repo = fixture.store.clone();
    let run = fixture.run();
    let LoadedRun::Existing(before) = repo.load_run(run).await.unwrap() else {
        panic!("run")
    };
    let execution = tokeira_types::execution_home_bundle(
        before.namespace_id.0.as_bytes(),
        before.workflow_id.0.as_bytes(),
        8,
    );
    let queue = QueueKey {
        namespace_id: before.namespace_id,
        task_queue: TaskQueueName(fixture.request.task_queue.clone()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
    let queue_home = queue_home(queue.namespace_id, &queue.task_queue.0, 8);
    assert_ne!(execution, queue_home);
    // Hold eligibility and timeout decisions fixed throughout transport and OCC
    // retries; cache warmth is the only time-independent difference in this series.
    let clock: Arc<dyn Clock> = Arc::new(StartClock(
        before.activities["activity-1"]
            .current_attempt_scheduled_at
            .unwrap()
            + time::Duration::milliseconds(1),
    ));
    // Use a newly constructed execution runtime in both series. Only the warm
    // series admits a lane command before measurement; acquisition hydration is
    // completed before the observer starts and cannot be disguised as a start.
    release(&repo, &fixture.runtime, execution).await;
    release(&repo, &fixture.runtime, queue_home).await;
    let owner = measured_runtime(repo.clone(), "activity-read-owner", clock.clone());
    owner.acquire_shard(execution).await.unwrap();
    if warm {
        let now = clock.now();
        owner
            .submit(
                run,
                KernelCommand::Signal(SignalRequest {
                    signal_name: "prime-cache".into(),
                    input: Default::default(),
                    header: None,
                    links: Vec::new(),
                    request: RequestContext::unattributed(now),
                    now,
                }),
            )
            .await
            .unwrap();
    }
    let auth = Arc::new(WorkerAuth::default());
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await
        .unwrap();
    let service = ActivityTaskDispatchGrpc::new(
        owner.clone(),
        Arc::new(EdgeInterceptors::configured(
            namespaces,
            auth.clone(),
            false,
        )),
        Some(fixture.peer.clone()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("activity read baseline requires local gRPC sockets");
    let address = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let accepting = cancel.clone();
    let (send, receive) = tokio::sync::mpsc::channel(4);
    let accept = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = accepting.cancelled() => break,
                socket = listener.accept() => if send.send(socket.map(|(socket, _)| socket)).await.is_err() { break; },
            }
        }
    });
    let stopping = cancel.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_service())
            .serve_with_incoming_shutdown(ReceiverStream::new(receive), stopping.cancelled())
            .await
            .unwrap();
    });
    let queue_runtime = measured_runtime(repo.clone(), "activity-read-queue", clock.clone());
    queue_runtime.acquire_shard(queue_home).await.unwrap();
    let placement = tokeira_types::PlacementConfig {
        shard_count: 8,
        bundle_count: 8,
        partition_count: 8,
        hash_version: 1,
    };
    let cache = Arc::new(RoutingCache::new(placement));
    let mut snapshot = tokeira_types::RoutingSnapshot::new(placement);
    let node = tokeira_types::IncarnationId(Uuid::new_v4());
    snapshot.execution_bundle_owners.insert(
        execution,
        tokeira_types::BundleOwner {
            node_id: node,
            epoch: tokeira_types::ShardEpoch::ZERO,
        },
    );
    snapshot.node_endpoints.insert(
        node,
        tokeira_types::NodeEndpoint {
            host: address.ip().to_string(),
            port: address.port(),
        },
    );
    cache.replace_snapshot(snapshot);
    queue_runtime.set_activity_task_starter(Arc::new(RemoteActivityTaskStarter::new(
        cache,
        fixture.peer.clone(),
    )));
    let activity = &before.activities["activity-1"];
    let task = DispatchableActivityTask {
        run_key: run,
        queue: queue.clone(),
        activity_id: activity.activity_id.clone(),
        schedule_event_id: activity.schedule_event_id,
        attempt: activity.attempt,
        stamp: activity.stamp,
        dispatch_revision: 0,
        priority: None,
        order: None,
    };
    let publication = ReadProbe::start(run, "activity/publication");
    if hinted {
        let service = ActivityTaskDispatchGrpc::new(
            queue_runtime.clone(),
            Arc::new(EdgeInterceptors::permissive(Arc::new(
                InMemoryNamespaceCache::new(),
            ))),
            Some(fixture.peer.clone()),
        );
        let mut client = ActivityTaskDispatchClient::new(service.into_service());
        let offer = tokeira_runtime::ActivityQueueOffer {
            task,
            execution_home: execution,
            dispatch_at: clock.now(),
            expires_at: clock.now() + time::Duration::seconds(5),
        };
        let mut request = Request::new(super::super::offer::encode(&offer, queue_home));
        request.metadata_mut().insert(
            "authorization",
            fixture
                .peer
                .signer()
                .token_at(fixture.peer.unix_seconds().unwrap())
                .unwrap()
                .authorization()
                .parse()
                .unwrap(),
        );
        assert_eq!(
            super::super::offer::outcome(
                client.offer_activity_task(request).await.unwrap().get_ref()
            )
            .unwrap(),
            tokeira_runtime::ActivityOfferOutcome::Admitted,
        );
    } else {
        queue_runtime
            .activity_broker()
            .publish_activity_task(task, None)
            .await
            .unwrap();
    }
    assert!(
        publication.report().counts.is_empty(),
        "offer admission must not hydrate the run: {:?}",
        publication.report()
    );
    drop(publication);
    let probe = ReadProbe::start(
        run,
        format!("activity/warm={warm}/conflict={conflict}/value={value}"),
    );
    if conflict {
        probe.inject_conflicts(1);
    }
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer synthetic-worker".parse().unwrap());
    let started = FORWARDED_ACTIVITY_POLL
        .scope(
            ForwardedActivityPoll {
                headers,
                namespace: "default".into(),
                scoped_versioned: false,
            },
            queue_runtime.poll_activity_task(
                queue,
                WorkerIdentity("read-worker".into()),
                Duration::from_secs(2),
            ),
        )
        .await
        .unwrap()
        .unwrap();
    let report = probe.report();
    drop(probe);
    assert_eq!(
        report
            .counts
            .iter()
            .filter(|((_, kind), _)| *kind == "injected-conflict")
            .map(|(_, count)| count)
            .sum::<usize>(),
        usize::from(conflict),
        "the conflict series must actually reject one commit"
    );
    assert_eq!(
        auth.0.load(Ordering::SeqCst),
        1,
        "remote owner must repeat worker authorization"
    );
    assert_eq!(started.input.0[0].data, [value, 0, 255]);
    // Start remains authoritative and request-bound even when the routing load
    // is removed: service/history/api/recordactivitytaskstarted/api.go @ v1.31.0.
    let LoadedRun::Existing(after) = repo.load_run(run).await.unwrap() else {
        panic!("started run")
    };
    assert!(after.activities["activity-1"].started_at.is_some());
    assert_eq!(after.activity_start_receipts.len(), 1);
    assert_eq!(
        after.activity_start_receipts["activity-1"]
            .worker_identity
            .0,
        "read-worker"
    );
    cancel.cancel();
    accept.await.unwrap();
    server.await.unwrap();
    stop(&queue_runtime).await;
    stop(&owner).await;
    fixture.stop().await;
    report
}

fn loads(report: &ReadReport) -> (usize, usize) {
    let count = |node| {
        report
            .counts
            .iter()
            .filter(|((caller, kind), _)| caller.starts_with(node) && *kind == "logical")
            .map(|(_, count)| count)
            .sum()
    };
    (count("queue-home/"), count("execution-owner/"))
}

fn explore(warm: bool, conflict: bool) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Infrastructure refusal is not a shrunk behavioral counterexample. Fail
    // before generating cases if this environment cannot host the real wire.
    drop(rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("activity read baseline requires local gRPC sockets; run outside the restricted sandbox"));
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[37; 32]),
    );
    let checked = std::cell::Cell::new(0);
    let failures = std::cell::Cell::new(0);
    let latest = std::cell::RefCell::new(None);
    runner
        .run(&any::<u8>(), |value| {
            let report = rt.block_on(case(warm, conflict, value, true));
            let observed = loads(&report);
            if observed != (0, 1 + usize::from(conflict)) {
                failures.set(failures.get() + 1);
            }
            assert!(
                report
                    .counts
                    .keys()
                    .all(|(_, kind)| !kind.starts_with("sql/")),
                "in-memory fixtures execute no physical SQL"
            );
            checked.set(checked.get() + 1);
            latest.replace(Some(report));
            Ok(())
        })
        .unwrap();
    assert!(checked.get() >= 100);
    if failures.get() == 0 {
        return;
    }
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[37; 32]),
    );
    let failure = runner.run(&any::<u8>(), |value| {
        let report = rt.block_on(case(warm, conflict, value, true));
        let (queue, owner) = loads(&report);
        if (queue, owner) == (0, 1 + usize::from(conflict)) { Ok(()) }
        else { Err(TestCaseError::fail(format!("queue-home route_activity_start/load_run={queue}, owner start_activity_task_inner/load_run={owner}; physical SQL=[] (in-memory); {report:?}"))) }
    });
    let Err(TestError::Fail(reason, minimal)) = failure else {
        panic!("missing counterexample: {failure:?}")
    };
    assert_eq!(minimal, 0);
    panic!(
        "{} generated cases; minimal value={minimal}; warm={warm}, conflict={conflict}: {reason}; last report={:?}",
        checked.get(),
        latest.borrow()
    );
}

// Feature: activity-queue-home-delivery, Property 5: One authoritative start with fewer reads.
#[test]
fn activity_queue_home_read_warm() {
    explore(true, false);
}
// Feature: activity-queue-home-delivery, Property 5: One authoritative start with fewer reads.
#[test]
fn activity_queue_home_read_cold() {
    explore(false, false);
}
// Feature: activity-queue-home-delivery, Property 5: One authoritative start with fewer reads.
#[test]
fn activity_queue_home_read_warm_conflict() {
    explore(true, true);
}
// Feature: activity-queue-home-delivery, Property 5: One authoritative start with fewer reads.
#[test]
fn activity_queue_home_read_cold_conflict() {
    explore(false, true);
}

#[test]
fn activity_queue_home_read_legacy_unhinted() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    drop(rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("legacy read control requires local gRPC sockets; run outside the restricted sandbox"));
    let mut runner = TestRunner::new(Config {
        cases: 100,
        source_file: Some(file!()),
        ..Config::default()
    });
    runner
        .run(
            &(any::<bool>(), any::<bool>(), any::<u8>()),
            |(warm, conflict, value)| {
                let report = rt.block_on(case(warm, conflict, value, false));
                prop_assert_eq!(loads(&report), (1, 1 + usize::from(conflict)));
                Ok(())
            },
        )
        .unwrap();
}
