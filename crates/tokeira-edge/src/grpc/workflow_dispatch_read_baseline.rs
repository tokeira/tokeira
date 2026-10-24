//! Real remote-start read attribution before preparation is consolidated.
//!
//! The offer is already at the queue home: publication and recovery are outside
//! this measurement. Both runtimes and the actual authenticated gRPC endpoint
//! participate; repository observation includes their independently spawned lanes.

use super::*;
use crate::{EdgeContext, InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace};
use std::{collections::BTreeMap, sync::atomic::AtomicUsize};
use tokeira_kernel::{BasicKernel, Command, Kernel, LoadedRun, SignalRequest, TerminateRequest};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusTimeoutScannerConfig, NoopNexusHttpClient, TimerScannerConfig,
    WorkflowTimeoutScannerConfig,
};
#[cfg(feature = "dsql-integration")]
use tokeira_storage::{
    ConnectionDirector,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
};
use tokeira_storage::{
    InMemoryStore, LeaseRepository, WriteFence,
    read_probe::{ReadProbe, ReadReport},
};
use tokeira_types::{
    BundleOwner, IncarnationId, NodeEndpoint, PlacementConfig, RequestContext, RoutingSnapshot,
    StickyAffinity, bundle_for_partition, execution_home_bundle, workflow_queue_partition,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

fn runtime<R: RunRepository + 'static>(repo: Arc<R>, identity: &str) -> Arc<TokeiraRuntime<R>> {
    Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo,
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
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
    ))
}

async fn stop<R: RunRepository + 'static>(runtime: &TokeiraRuntime<R>) {
    let shutdown = runtime.shutdown_handle();
    shutdown.begin_shutdown();
    shutdown
        .wait(std::time::Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
}

async fn case<R: RunRepository + LeaseRepository + 'static>(
    repo: Arc<R>,
    sticky: bool,
    warm: bool,
    conflict: bool,
    split: bool,
) -> ReadReport {
    let context = EdgeContext {
        request_id: crate::request_id::RequestId::new("read-baseline"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: time::OffsetDateTime::now_utc(),
        is_long_poll: false,
    };
    let mut dto = super::tests::start_dto();
    dto.task_queue = "read-baseline".into();
    let mut request = crate::translate::to_internal::start_request(dto, &context);
    let normal = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let selected = if sticky {
        QueueKey {
            task_queue: TaskQueueName("read-baseline-sticky".into()),
            ..normal.clone()
        }
    } else {
        normal.clone()
    };
    let queue_home = bundle_for_partition(
        workflow_queue_partition(selected.namespace_id, &selected.task_queue.0, 8),
        8,
    );
    let execution_home = (0..100)
        .find_map(|index| {
            request.workflow_id.0 = format!("read-baseline-{}-{index}", request.run_key.0);
            let home = execution_home_bundle(
                request.namespace_id.0.as_bytes(),
                request.workflow_id.0.as_bytes(),
                8,
            );
            (home != queue_home).then_some(home)
        })
        .unwrap();
    let key = request.run_key;
    let mut initial = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(request))
        .unwrap();
    if split {
        initial.next_state.last_completion_result =
            Some(tokeira_types::Payloads(vec![tokeira_types::Payload::new(
                vec![42; 1024 * 1024],
            )]));
    }
    let worker = WorkerIdentity("read-worker".into());
    let deadline = time::OffsetDateTime::now_utc() + time::Duration::minutes(5);
    if sticky {
        initial.next_state.sticky = Some(StickyAffinity {
            worker_identity: worker.clone(),
            sticky_queue: selected.task_queue.clone(),
            schedule_to_start_timeout: time::Duration::minutes(5),
        });
        initial
            .next_state
            .pending_workflow_task
            .as_mut()
            .unwrap()
            .schedule_to_start_deadline = Some(deadline);
    }
    repo.commit_transition(key, initial, WriteFence::Unfenced)
        .await
        .unwrap();
    let owner = runtime(repo.clone(), "read-owner");
    let queue_runtime = runtime(repo.clone(), "read-queue");
    let epoch = owner.acquire_shard(execution_home).await.unwrap();
    let queue_epoch = queue_runtime.acquire_shard(queue_home).await.unwrap();
    if warm {
        let now = time::OffsetDateTime::now_utc();
        owner
            .submit(
                key,
                Command::Signal(SignalRequest {
                    signal_name: "prime-cache".into(),
                    input: Default::default(),
                    header: None,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: tokeira_types::RequestId("baseline-prime".into()),
                        ..RequestContext::unattributed(now)
                    },
                    now,
                }),
            )
            .await
            .unwrap();
    }

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await
        .unwrap();
    let interceptors = Arc::new(EdgeInterceptors::configured(
        namespaces.clone(),
        Arc::new(super::tests::WorkerAuth(AtomicUsize::new(0))),
        false,
    ));
    let workflow = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
        Arc::new(crate::grpc::runtime_adapter::RuntimeAdapter::new(
            owner.clone(),
        )),
        Arc::new(crate::InMemoryExecutionResolver::new()),
        Arc::new(crate::EmptyVisibilityApi),
        repo.clone(),
        Arc::new(crate::InMemoryOperatorApi::new("test", "test")),
        namespaces,
        interceptors.clone(),
        crate::PollerRegistry::default(),
        crate::PendingQueryStore::default(),
        owner.buffered_queries(),
        owner.broker(),
        crate::LongPollGate::new(crate::LongPollConfig::default()),
        Arc::new(crate::LocalOnlyRouter),
        crate::HistoryWaitRegistry::default(),
    );
    let service = WorkflowTaskDispatchGrpc::new(owner.clone(), interceptors, workflow);
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
            .serve_with_incoming_shutdown(ReceiverStream::new(receive), server_cancel.cancelled())
            .await
            .unwrap();
    });
    let placement = PlacementConfig {
        shard_count: 8,
        bundle_count: 8,
        partition_count: 8,
        hash_version: 1,
    };
    let cache = Arc::new(RoutingCache::new(placement));
    let mut snapshot = RoutingSnapshot::new(placement);
    let node = IncarnationId(uuid::Uuid::new_v4());
    snapshot.execution_bundle_owners.insert(
        execution_home,
        BundleOwner {
            node_id: node,
            epoch,
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
    queue_runtime.set_workflow_task_starter(Arc::new(RemoteWorkflowTaskStarter::new(cache)));
    let broker = queue_runtime.broker();
    broker
        .poll_workflow_task(&selected, &worker, Duration::ZERO)
        .await
        .unwrap();
    broker
        .publish_workflow_task(
            DispatchableWorkflowTask {
                run_key: key,
                logical_seq: LogicalTaskSeq::ONE,
                queue: selected.clone(),
                sticky_preferred: sticky.then(|| worker.clone()),
                normal_queue: sticky.then(|| normal.clone()),
                sticky_deadline: sticky.then_some(deadline),
                priority: None,
                order: None,
            },
            None,
        )
        .await;
    let attempt = format!("sticky={sticky},warm={warm},conflict={conflict}");
    let probe = ReadProbe::start(key, attempt);
    if conflict {
        probe.inject_conflicts(1);
    }
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer synthetic-worker".parse().unwrap());
    let forwarded = ForwardedPoll {
        headers,
        namespace: "default".into(),
        sticky,
        normal_task_queue: sticky.then(|| normal.task_queue.0.clone()),
        scoped_versioned: false,
        decorations: Arc::new(Mutex::new(None)),
    };
    let started = FORWARDED_POLL
        .scope(
            forwarded,
            queue_runtime.poll_workflow_task(selected, worker, Duration::from_secs(5)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.run_key, key);
    let report = probe.report();
    drop(probe);
    let now = time::OffsetDateTime::now_utc();
    owner
        .submit(
            key,
            Command::Terminate(TerminateRequest {
                reason: "baseline fixture cleanup".into(),
                details: None,
                identity: "read-baseline".into(),
                links: Vec::new(),
                request: RequestContext {
                    request_id: tokeira_types::RequestId("baseline-cleanup".into()),
                    ..RequestContext::unattributed(now)
                },
                now,
            }),
        )
        .await
        .unwrap();
    cancel.cancel();
    accept.await.unwrap();
    server.await.unwrap();
    stop(&queue_runtime).await;
    stop(&owner).await;
    let LoadedRun::Existing(state) = repo.load_run(key).await.unwrap() else {
        panic!("started run missing")
    };
    assert!(matches!(
        repo.delete_run_for_bundle(
            key,
            execution_home,
            tokeira_storage::DeleteRunRequest {
                expected_seq: state.transition_seq,
                deleted_at: time::OffsetDateTime::now_utc(),
            },
            WriteFence::Unfenced
        )
        .await
        .unwrap(),
        tokeira_storage::DeleteRunResult::Deleted { .. }
    ));
    repo.relinquish_bundle(execution_home, "read-owner".into(), epoch)
        .await
        .unwrap();
    repo.relinquish_bundle(queue_home, "read-queue".into(), queue_epoch)
        .await
        .unwrap();
    report
}

// Feature: queue-home-delivery, Property 6: Fewer reads without weakened start checks.
#[tokio::test]
async fn queue_home_delivery_exploration_read_baseline() {
    let subscriber = tracing_subscriber::fmt().with_test_writer().finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    for sticky in [false, true] {
        for warm in [false, true] {
            for conflict in [false, true] {
                let report = case(
                    Arc::new(InMemoryStore::with_shard_count(8)),
                    sticky,
                    warm,
                    conflict,
                    false,
                )
                .await;
                let mut expected = BTreeMap::from([
                    (("queue-home/resolve", "logical"), 1),
                    (("execution-owner/metadata", "logical"), 1),
                    (("execution-owner/resolve", "logical"), 1),
                ]);
                for (phase, present) in [
                    ("execution-owner/sticky-binding", sticky),
                    ("execution-owner/admission-cache", !warm),
                    ("execution-owner/lane-cache-or-retry", conflict),
                ] {
                    if present {
                        expected.insert((phase, "logical"), 1);
                    }
                }
                assert_eq!(
                    report
                        .counts
                        .iter()
                        .filter(|((_, kind), _)| *kind == "logical")
                        .map(|(key, count)| (*key, *count))
                        .collect::<BTreeMap<_, _>>(),
                    expected,
                    "{report:?}"
                );
                let owner_loads: usize = report
                    .counts
                    .iter()
                    .filter(|((phase, kind), _)| {
                        phase.starts_with("execution-owner/") && *kind == "logical"
                    })
                    .map(|(_, count)| *count)
                    .sum();
                for (met, failure) in [
                    (
                        report.counts[&("queue-home/resolve", "logical")] == 0,
                        "queue-home must not load run state",
                    ),
                    (
                        owner_loads <= 1 + usize::from(!warm) + usize::from(conflict),
                        "owner exceeds the preparation and required reload target",
                    ),
                ] {
                    let desired = if met {
                        Ok(())
                    } else {
                        Err(proptest::test_runner::TestCaseError::fail(failure))
                    };
                    assert!(
                        matches!(desired, Err(proptest::test_runner::TestCaseError::Fail(ref reason)) if reason.to_string() == failure),
                        "expected {failure}: {report:?}"
                    );
                }
                tracing::info!(?report, "queue-home delivery read baseline");
            }
        }
    }
}

// Feature: queue-home-delivery, Property 6: Fewer reads without weakened start checks.
#[cfg(feature = "dsql-integration")]
#[tokio::test]
async fn queue_home_delivery_live_read_baseline() -> Result<()> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(());
    };
    let subscriber = tracing_subscriber::fmt()
        .with_test_writer()
        .without_time()
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..ReservoirConfig::default()
            },
            shard_count: 8,
            ..DsqlPoolConfig::default()
        },
    )
    .await?;
    {
        let mut permit = ConnectionDirector::acquire(
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
    let repo = Arc::new(repo);
    let mut executed = 0;
    for split in [false, true] {
        for sticky in [false, true] {
            for warm in [false, true] {
                for conflict in [false, true] {
                    let report = case(repo.clone(), sticky, warm, conflict, split).await;
                    let logical: usize = report
                        .counts
                        .iter()
                        .filter(|((_, kind), _)| *kind == "logical")
                        .map(|(_, n)| *n)
                        .sum();
                    assert_eq!(
                        logical,
                        3 + usize::from(sticky) + usize::from(!warm) + usize::from(conflict),
                        "{report:?}"
                    );
                    for ((purpose, kind), count) in &report.counts {
                        if *kind != "logical" {
                            continue;
                        }
                        assert_eq!(
                            report.counts.get(&(*purpose, "sql/hot-state-fast")),
                            Some(count),
                            "{report:?}"
                        );
                        for operation in [
                            "sql/begin-read-snapshot",
                            "sql/hot-state-snapshot",
                            "sql/value-parts",
                            "sql/commit-read-snapshot",
                        ] {
                            assert_eq!(
                                report
                                    .counts
                                    .get(&(*purpose, operation))
                                    .copied()
                                    .unwrap_or(0),
                                if split { *count } else { 0 },
                                "{report:?}"
                            );
                        }
                    }
                    tracing::info!(split, ?report, "executed DSQL remote-start read baseline");
                    executed += 1;
                }
            }
        }
    }
    assert_eq!(executed, 16);
    director.shutdown().await?;
    Ok(())
}
