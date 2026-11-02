//! Isolated state-read attribution across discovery, authenticated owner start,
//! same-request replay and a new execution owner's replay observation.

use super::*;
#[cfg(feature = "dsql-integration")]
use anyhow::Context;
use tokeira_storage::{
    LeaseOutcome, LeaseRepository,
    read_probe::{ReadProbe, ReadReport},
};

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

fn assert_reads(report: &ReadReport, replay: bool, sql: bool) {
    let mut expected = BTreeMap::from([(("execution-owner/activity-start", "logical"), 1)]);
    if replay {
        expected.insert(("execution-owner/activity-replay", "logical"), 1);
    } else {
        expected.insert(("queue-home/activity-owner", "logical"), 1);
    }
    let scoped = report
        .counts
        .iter()
        .filter(|((phase, _), _)| {
            *phase != "outside-start" && *phase != "activity-discovery/resolve"
        })
        .map(|(key, value)| (*key, *value))
        .collect::<BTreeMap<_, _>>();
    if sql {
        expected.insert(("execution-owner/activity-start", "sql/hot-state-fast"), 1);
        if replay {
            for operation in [
                "sql/begin-activity-observation",
                "sql/activity-observation-fence",
                "sql/activity-observation-state",
                "sql/commit-activity-observation",
            ] {
                expected.insert(("execution-owner/activity-replay", operation), 1);
            }
        } else {
            expected.insert(("queue-home/activity-owner", "sql/hot-state-fast"), 1);
        }
    }
    assert_eq!(scoped, expected, "{report:?}");
}

async fn run<R: RunRepository + LeaseRepository + 'static>(
    repo: Arc<R>,
    sql: bool,
) -> Vec<ReadReport> {
    let mut fixture = Fixture::with_store(repo.clone(), vec![0, 255, 17], vec![9, 0], 3, 8).await;
    let run = fixture.run();
    let LoadedRun::Existing(state) = repo.load_run(run).await.unwrap() else {
        panic!("state")
    };
    let home = tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        8,
    );
    let queue_home = queue_home(state.namespace_id, &fixture.request.task_queue, 8);
    release(repo.as_ref(), fixture.runtime.as_ref(), queue_home).await;
    let queue_runtime = placed_runtime(repo.clone(), 8, "read-queue");
    queue_runtime.acquire_shard(queue_home).await.unwrap();
    queue_runtime.set_activity_task_starter(Arc::new(fixture.starter(home)));
    let probe = ReadProbe::start(run, "activity-fresh-start");
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer synthetic-worker".parse().unwrap());
    let queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: TaskQueueName(fixture.request.task_queue.clone()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
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
    assert_reads(&fresh, false, sql);
    assert!(
        fresh
            .counts
            .get(&("activity-discovery/resolve", "logical"))
            .copied()
            .unwrap_or(0)
            >= 1
    );
    drop(probe);
    let LoadedRun::Existing(started_state) = repo.load_run(run).await.unwrap() else {
        panic!("started")
    };
    let receipt = &started_state.activity_start_receipts["activity-1"];
    fixture.request.request_id = receipt.request_id.as_bytes().to_vec();
    fixture.request.worker_identity = receipt.worker_identity.0.clone();
    let probe = ReadProbe::start(run, "activity-same-request-replay");
    let request = fixture.authorized();
    let reply = fixture
        .client
        .start_activity_task(request)
        .await
        .unwrap()
        .into_inner();
    assert!(reply.task.is_some());
    let replay = probe.report();
    assert_reads(&replay, true, sql);
    drop(probe);

    release(repo.as_ref(), fixture.runtime.as_ref(), home).await;
    let replacement = placed_runtime(repo.clone(), 8, "read-replacement");
    replacement.acquire_shard(home).await.unwrap();
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
    let service = ActivityTaskDispatchGrpc::new(
        replacement.clone(),
        interceptors,
        Some(fixture.peer.clone()),
    );
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
    let mut client = ActivityTaskDispatchClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let probe = ReadProbe::start(run, "activity-handoff-replay");
    let reply = client
        .start_activity_task(fixture.authorized())
        .await
        .unwrap()
        .into_inner();
    assert!(reply.task.is_some());
    let handoff = probe.report();
    assert_reads(&handoff, true, sql);
    drop(probe);
    let LoadedRun::Existing(after) = repo.load_run(run).await.unwrap() else {
        panic!("after")
    };
    assert_eq!(after.transition_seq, started_state.transition_seq);
    assert_eq!(
        after.activities["activity-1"].started_at,
        started_state.activities["activity-1"].started_at
    );
    drop(client);
    cancel.cancel();
    accept.await.unwrap();
    server.await.unwrap();
    for runtime in [&replacement, &queue_runtime, &fixture.runtime] {
        let homes = runtime.active_shards();
        for home in homes {
            release(repo.as_ref(), runtime.as_ref(), home).await;
        }
        let shutdown = runtime.shutdown_handle();
        shutdown.begin_shutdown();
        shutdown
            .wait(std::time::Instant::now() + Duration::from_secs(10))
            .await
            .unwrap();
    }
    fixture.stop().await;
    vec![fresh, replay, handoff]
}

#[tokio::test]
async fn activity_start_read_counts() {
    run(Arc::new(InMemoryStore::with_shard_count(8)), false).await;
}

#[cfg(feature = "dsql-integration")]
#[tokio::test]
#[ignore = "requires an isolated ephemeral DSQL database; never run with other fixtures"]
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
                target_ready: 4,
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
    let reports = run(Arc::new(repo), true).await;
    if let Some(path) = std::env::var_os("TOKEIRA_ACTIVITY_READ_REPORT") {
        std::fs::write(path, format!("{reports:#?}"))?;
    }
    director.shutdown().await
}
