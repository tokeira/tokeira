//! Exact per-phase activity reads across pushed/discovered starts and receipt replay.
//! Publication, acquisition and discovery remain visible outside the start budgets.

use super::*;
#[cfg(feature = "dsql-integration")]
use anyhow::Context;
use tokeira_clock::Clock;
use tokeira_kernel::{
    BasicKernel, Command as KernelCommand, Kernel, SignalRequest, TerminateRequest,
};
use tokeira_storage::{
    CommitResult, ExecutionHomeFence, LeaseOutcome, LeaseRepository, WriteFence,
    read_probe::{ReadProbe, ReadReport},
};
use tokeira_types::{Payload, Payloads, RequestContext};

// Each command carries its own request id, so none is deduplicated as a
// retry of another.
fn fresh_request(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: tokeira_types::RequestId(Uuid::new_v4().to_string()),
        ..RequestContext::unattributed(now)
    }
}

#[derive(Debug)]
struct FixedClock(OffsetDateTime);
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

fn runtime<R: RunRepository + 'static>(
    repo: Arc<R>,
    identity: &str,
    clock: Arc<dyn Clock>,
) -> Arc<TokeiraRuntime<R>> {
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

async fn release<R: RunRepository + LeaseRepository + 'static>(
    repo: &R,
    runtime: &TokeiraRuntime<R>,
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
        assert!(matches!(
            repo.relinquish_bundle(home, lease.owner_node_id.unwrap(), lease.epoch)
                .await
                .unwrap(),
            LeaseOutcome::Acquired { .. }
        ));
    }
}

async fn serving<R: RunRepository + LeaseRepository + 'static>(
    store: Arc<R>,
    runtime: Arc<TokeiraRuntime<R>>,
    request: wire::StartActivityTaskRequest,
    peer: Arc<super::super::super::workflow_offer::PeerOfferEndpoint>,
) -> Fixture<R> {
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
    let service = ActivityTaskDispatchGrpc::new(runtime.clone(), interceptors, Some(peer.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
    let client = ActivityTaskDispatchClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    Fixture {
        store,
        runtime,
        peer,
        request,
        address,
        homes: 8,
        client,
        cancel,
        accept,
        server,
    }
}

fn assert_reads(report: &ReadReport, replay: bool, conflict: bool, split: bool, sql: bool) {
    let starts = 1 + usize::from(conflict);
    let mut expected = BTreeMap::from([(("execution-owner/activity-start", "logical"), starts)]);
    if replay {
        expected.insert(("execution-owner/activity-replay", "logical"), 1);
    }
    if sql {
        expected.insert(
            ("execution-owner/activity-start", "sql/hot-state-fast"),
            starts,
        );
        if split {
            for operation in [
                "sql/begin-read-snapshot",
                "sql/hot-state-snapshot",
                "sql/value-parts",
                "sql/commit-read-snapshot",
            ] {
                expected.insert(("execution-owner/activity-start", operation), starts);
            }
        }
        if replay {
            for operation in [
                "sql/begin-activity-observation",
                "sql/activity-observation-fence",
                "sql/activity-observation-state",
                "sql/commit-activity-observation",
            ] {
                expected.insert(("execution-owner/activity-replay", operation), 1);
            }
            if split {
                expected.insert(("execution-owner/activity-replay", "sql/value-parts"), 1);
            }
        }
    }
    // Count every phase except the explicitly named background scopes. In
    // particular an unexpected queue-home load cannot cancel an owner omission.
    let scoped = report
        .counts
        .iter()
        .filter(|((phase, kind), _)| {
            *kind != "injected-conflict"
                && !matches!(
                    *phase,
                    "outside-start"
                        | "activity-discovery/resolve"
                        | "normal-discovery"
                        | "publication-fallback"
                        | "sticky-recovery"
                        | "sticky-recovery-fallback"
                )
        })
        .map(|(key, count)| (*key, *count))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(scoped, expected, "{report:?}");
    assert_eq!(
        report
            .counts
            .iter()
            .filter(|((_, kind), _)| *kind == "injected-conflict")
            .map(|(_, count)| count)
            .sum::<usize>(),
        usize::from(conflict),
        "{report:?}"
    );
}

async fn case<R: RunRepository + LeaseRepository + 'static>(
    repo: Arc<R>,
    sql: bool,
    pushed: bool,
    warm: bool,
    split: bool,
    conflict: bool,
) -> Vec<ReadReport> {
    let seeding = placed_runtime(repo.clone(), 8, "wire-execution");
    for home in 0..8 {
        seeding.acquire_shard(ShardId(home)).await.unwrap();
    }
    let (request, mut publication) =
        seed_with_reads(repo.as_ref(), &seeding, vec![0, 255, 17], vec![9, 0], 3, 8).await;
    let material = PeerMaterial::generate().unwrap();
    let peer = Arc::new(super::super::super::workflow_offer::PeerOfferEndpoint::new(
        PeerVerifier::new(material.profile().clone()).unwrap(),
        material.signer(material.profile()).unwrap(),
    ));
    let seeded = serving(repo.clone(), seeding, request, peer).await;
    let run = seeded.run();
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("state")
    };
    let home = tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        8,
    );
    let queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: TaskQueueName(seeded.request.task_queue.clone()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
    let qhome = queue_home(queue.namespace_id, &queue.task_queue.0, 8);
    let now = state.activities["activity-1"]
        .current_attempt_scheduled_at
        .unwrap()
        + time::Duration::milliseconds(1);
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(now));
    let signal = || {
        KernelCommand::Signal(SignalRequest {
            signal_name: "prime-cache".into(),
            input: Payloads::default(),
            header: None,
            links: Vec::new(),
            request: fresh_request(now),
            now,
        })
    };
    if split {
        let lease = repo
            .list_bundle_leases()
            .await
            .unwrap()
            .into_iter()
            .find(|lease| lease.bundle_id == home)
            .unwrap();
        let mut transition = BasicKernel
            .apply(LoadedRun::Existing(state), signal())
            .unwrap();
        transition.next_state.last_completion_result = Some(Payloads(vec![Payload::new(vec![
                42;
                tokeira_storage::value_parts::PART_BYTES
                    + 1
            ])]));
        assert!(matches!(
            repo.commit_transition_for_bundle(
                run,
                home,
                transition,
                WriteFence::Lease(ExecutionHomeFence {
                    home,
                    owner: lease.owner_node_id.unwrap(),
                    epoch: lease.epoch
                })
            )
            .await
            .unwrap(),
            CommitResult::Applied { .. }
        ));
    }
    let request = seeded.request.clone();
    let peer = seeded.peer.clone();
    for shard in seeded.runtime.active_shards() {
        release(repo.as_ref(), &seeded.runtime, shard).await;
    }
    seeded.stop().await;
    let label = format!("pushed={pushed}/warm={warm}/split={split}/conflict={conflict}");
    publication.attempt = format!("{label}/publication");
    let probe = ReadProbe::start(run, format!("{label}/acquisition-and-offer"));
    let owner = runtime(repo.clone(), "read-owner", clock.clone());
    owner.acquire_shard(home).await.unwrap();
    if warm {
        owner.submit(run, signal()).await.unwrap();
    }
    let mut fixture = serving(repo.clone(), owner, request, peer.clone()).await;
    let queue_runtime = runtime(repo.clone(), "read-queue", clock.clone());
    queue_runtime.acquire_shard(qhome).await.unwrap();
    queue_runtime.set_activity_task_starter(Arc::new(fixture.starter(home)));
    let mut queue_service = serving(
        repo.clone(),
        queue_runtime.clone(),
        fixture.request.clone(),
        peer.clone(),
    )
    .await;
    if pushed {
        let activity = &match repo.load_run(run).await.unwrap() {
            LoadedRun::Existing(state) => state,
            _ => panic!("state"),
        }
        .activities["activity-1"];
        let offer = tokeira_runtime::ActivityQueueOffer {
            task: DispatchableActivityTask {
                run_key: run,
                queue: queue.clone(),
                activity_id: activity.activity_id.clone(),
                schedule_event_id: activity.schedule_event_id,
                attempt: activity.attempt,
                stamp: activity.stamp,
                dispatch_revision: 0,
                priority: None,
                order: None,
            },
            execution_home: home,
            dispatch_at: now,
            expires_at: now + time::Duration::seconds(30),
        };
        let mut request = Request::new(super::super::offer::encode(&offer, qhome));
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
            queue_service
                .client
                .offer_activity_task(request)
                .await
                .unwrap()
                .get_ref()
                .outcome,
            wire::ActivityOfferOutcome::Admitted as i32
        );
    }
    let recovery = probe.report();
    drop(probe);
    let probe = ReadProbe::start(run, format!("{label}/fresh"));
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
                Duration::from_secs(30),
            ),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.input.0[0].data, [0, 255, 17]);
    let fresh = probe.report();
    drop(probe);
    assert_reads(&fresh, false, conflict, split, sql);
    if !pushed {
        assert!(
            fresh
                .counts
                .get(&("activity-discovery/resolve", "logical"))
                .copied()
                .unwrap_or(0)
                >= 1
        );
    }
    let LoadedRun::Existing(started_state) = repo.load_run(run).await.unwrap() else {
        panic!("started")
    };
    let receipt = &started_state.activity_start_receipts["activity-1"];
    fixture.request.request_id = receipt.request_id.as_bytes().to_vec();
    fixture.request.worker_identity = receipt.worker_identity.0.clone();
    let probe = ReadProbe::start(run, format!("{label}/same-request-replay"));
    let request = fixture.authorized();
    let reply = fixture
        .client
        .start_activity_task(request)
        .await
        .unwrap()
        .into_inner();
    assert!(reply.task.is_some());
    let replay = probe.report();
    drop(probe);
    assert_reads(&replay, true, false, split, sql);
    release(repo.as_ref(), &fixture.runtime, home).await;
    let replacement = runtime(repo.clone(), "read-replacement", clock);
    replacement.acquire_shard(home).await.unwrap();
    let mut replacement = serving(repo.clone(), replacement, fixture.request.clone(), peer).await;
    let probe = ReadProbe::start(run, format!("{label}/handoff-replay"));
    let request = replacement.authorized();
    assert!(
        replacement
            .client
            .start_activity_task(request)
            .await
            .unwrap()
            .get_ref()
            .task
            .is_some()
    );
    let handoff = probe.report();
    drop(probe);
    assert_reads(&handoff, true, false, split, sql);
    let LoadedRun::Existing(after) = repo.load_run(run).await.unwrap() else {
        panic!("after")
    };
    // Same-request replay preserves the committed start and event sequence:
    // service/history/api/recordactivitytaskstarted/api.go @ v1.31.0.
    assert_eq!(after.transition_seq, started_state.transition_seq);
    assert_eq!(
        after.activities["activity-1"].started_at,
        started_state.activities["activity-1"].started_at
    );
    replacement
        .runtime
        .submit(
            run,
            KernelCommand::Terminate(TerminateRequest {
                reason: "fixture cleanup".into(),
                details: None,
                identity: "fixture".into(),
                links: Vec::new(),
                request: fresh_request(now),
                now,
            }),
        )
        .await
        .unwrap();
    let LoadedRun::Existing(closed) = repo.load_run(run).await.unwrap() else {
        panic!("closed fixture")
    };
    let lease = repo
        .list_bundle_leases()
        .await
        .unwrap()
        .into_iter()
        .find(|lease| lease.bundle_id == home)
        .unwrap();
    assert!(matches!(
        repo.delete_run_for_bundle(
            run,
            home,
            tokeira_storage::DeleteRunRequest {
                expected_seq: closed.transition_seq,
                deleted_at: now
            },
            WriteFence::Lease(ExecutionHomeFence {
                home,
                owner: lease.owner_node_id.unwrap(),
                epoch: lease.epoch
            }),
        )
        .await
        .unwrap(),
        tokeira_storage::DeleteRunResult::Deleted { .. }
    ));
    repo.purge_run(run).await.unwrap();
    for service in [replacement, queue_service, fixture] {
        for shard in service.runtime.active_shards() {
            release(repo.as_ref(), &service.runtime, shard).await;
        }
        service.stop().await;
    }
    vec![publication, recovery, fresh, replay, handoff]
}

async fn matrix<R: RunRepository + LeaseRepository + 'static>(
    repo: Arc<R>,
    sql: bool,
) -> Vec<ReadReport> {
    let mut reports = Vec::new();
    for pushed in [false, true] {
        for warm in [false, true] {
            for split in [false, true] {
                for conflict in [false, true] {
                    reports.extend(case(repo.clone(), sql, pushed, warm, split, conflict).await);
                }
            }
        }
    }
    assert_eq!(reports.len(), 80);
    reports
}

#[tokio::test]
async fn activity_start_read_counts() {
    matrix(Arc::new(InMemoryStore::with_shard_count(8)), false).await;
}

#[cfg(feature = "dsql-integration")]
#[tokio::test]
// Isolated empty database, eight placement homes. Schema and placement are prepared
// before the first lease; run serially before other fixtures use this database.
#[ignore = "requires an isolated empty DSQL database and local gRPC sockets"]
async fn activity_dispatch_live_read_counts() -> Result<()> {
    let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .context("explicit activity read verification requires an isolated DSQL database")?;
    let store = tokeira_storage::dsql::DsqlStore::from_database_url_for_tests(
        url,
        tokeira_storage::dsql::DsqlPoolConfig {
            migration: tokeira_storage::dsql::MigrationConfig {
                migrations_dir: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: tokeira_storage::dsql::ReservoirConfig {
                target_ready: 5,
                inflight_limit: 2,
                ..Default::default()
            },
            shard_count: 8,
            ..Default::default()
        },
    )
    .await?;
    {
        let mut permit = tokeira_storage::ConnectionDirector::acquire(
            store.connection_director(),
            tokeira_storage::DbClass::Maintenance,
        )
        .await?;
        store
            .migration_runner()
            .apply_connection(permit.connection()?)
            .await?;
    }
    tokeira_storage::prepare_execution_placement(store.run_repository()).await?;
    let (director, repo, _, _, _) = store.into_parts();
    let reports = matrix(Arc::new(repo), true).await;
    if let Some(path) = std::env::var_os("TOKEIRA_ACTIVITY_READ_REPORT") {
        std::fs::write(path, format!("{reports:#?}"))?;
    }
    director.shutdown().await
}
