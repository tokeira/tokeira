//! Real internal gRPC exercises publication, recovery and fenced starts on shared stores.
//! Each node loads the daemon-generated file and shares one controllable wall clock.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[cfg(feature = "dsql-integration")]
use anyhow::Result;
use proptest::prelude::*;
use tokeira_clock::Clock;
use tokeira_edge::grpc::workflow_offer::{MAX_OFFER_HANDLERS, delivery::PeerWorkflowOfferSink};
use tokeira_kernel::{Command, LoadedRun, StartRequest, TerminateRequest};
use tokeira_proto::{
    internal::runtime::{self as wire, workflow_task_dispatch_client::WorkflowTaskDispatchClient},
    public::temporal::api::taskqueue::v1::TaskQueue,
    workflowservice::workflow_service_client::WorkflowServiceClient,
};
use tokeira_runtime::{RemoteWorkflowOfferSink, WorkflowOfferError, WorkflowOfferOutcome};
#[cfg(feature = "dsql-integration")]
use tokeira_storage::dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig};
use tokeira_storage::{InMemoryStore, LeaseRepository, RunRepository};
use tokeira_types::{
    QueueKey, RequestContext, RunKey, TaskQueueName, WorkerIdentity, execution_home_bundle,
};

use super::delivery_harness::*;

#[tokio::test]
async fn grpc_normal_push_remote_start_and_renewal() {
    let clock = TestClock::new();
    let store = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
    let (_directory, config) = credential();
    let before = tokeira_config::peer::load_peer_file(std::path::Path::new(&config.path)).unwrap();
    let mut owner = node(store.clone(), "execution", &config, clock.clone()).await;
    let matching = node(store.clone(), "queue", &config, clock.clone()).await;
    let first_token = owner
        .peer
        .signer()
        .token_at(owner.peer.unix_seconds().unwrap())
        .unwrap();
    let template = request(1, &clock);
    let home = execution(&template);
    let queue = normal_queue(&template);
    let qhome = queue_home(&queue);
    let epoch = owner.runtime.acquire_shard(home).await.unwrap();
    let qepoch = matching.runtime.acquire_shard(qhome).await.unwrap();
    route(
        &[&owner, &matching],
        &[(home, 0, epoch), (qhome, 1, qepoch)],
    );
    for round in 0..2 {
        if round == 1 {
            clock.advance(331);
        }
        let mut req = template.clone();
        req.run_id = tokeira_types::RunId::new();
        req.run_key = RunKey::derive(req.namespace_id, &req.workflow_id, req.run_id);
        req.workflow_id.0.push_str(&format!("-round-{round}"));
        // Keep distinct runs on the acquired execution home.
        req.workflow_id = (0..100)
            .map(|i| tokeira_types::WorkflowId(format!("round-{round}-{i}")))
            .find(|id| {
                execution_home_bundle(req.namespace_id.0.as_bytes(), id.0.as_bytes(), 8) == home
            })
            .unwrap();
        req.run_key = RunKey::derive(req.namespace_id, &req.workflow_id, req.run_id);
        req.now = clock.now();
        req.request = RequestContext::unattributed(req.now);
        let key = req.run_key;
        owner.runtime.start_workflow(req).await.unwrap();
        let receipt = owner.receipt().await;
        assert_eq!(receipt.outcome, Ok(WorkflowOfferOutcome::Admitted));
        assert_eq!(receipt.destination, qhome);
        assert_eq!(matching.repo.normal.reads.load(Ordering::SeqCst), 0);
        assert_eq!(owner.repo.sticky.reads.load(Ordering::SeqCst), 0);
        let owner_before = owner.repo.loads.load(Ordering::SeqCst);
        let queue_before = matching.repo.loads.load(Ordering::SeqCst);
        let response = matching.poll(&queue, None).await;
        assert!(!response.task_token.is_empty());
        assert_eq!(owner.repo.loads.load(Ordering::SeqCst) - owner_before, 1);
        assert_eq!(matching.repo.loads.load(Ordering::SeqCst), queue_before);
        let LoadedRun::Existing(state) = store.load_run(key).await.unwrap() else {
            panic!("run missing")
        };
        assert!(
            state
                .pending_workflow_task
                .unwrap()
                .started_event_id
                .is_some()
        );
        owner
            .runtime
            .submit(
                key,
                Command::Terminate(TerminateRequest {
                    reason: "fixture cleanup".into(),
                    details: None,
                    identity: "integration".into(),
                    links: Vec::new(),
                    request: RequestContext::unattributed(clock.now()),
                    now: clock.now(),
                }),
            )
            .await
            .unwrap();
        if round == 1 {
            let mut old = tonic::Request::new(wire::OfferWorkflowTaskRequest::default());
            old.metadata_mut().insert(
                "authorization",
                first_token.authorization().parse().unwrap(),
            );
            assert_eq!(
                matching
                    .client()
                    .await
                    .offer_workflow_task(old)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
        }
    }
    assert!(matching.policy.0.load(Ordering::SeqCst) >= 2);
    assert!(owner.policy.0.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        tokeira_config::peer::load_peer_file(std::path::Path::new(&config.path)).unwrap(),
        before
    );
    matching.stop().await;
    owner.stop().await;
}
fn start_wire(
    request: &StartRequest,
    logical_seq: u64,
) -> tonic::Request<wire::StartWorkflowTaskRequest> {
    let mut rpc = tonic::Request::new(wire::StartWorkflowTaskRequest {
        run_key: request.run_key.0.as_bytes().to_vec(),
        logical_seq,
        namespace: "default".into(),
        task_queue: request.task_queue.0.clone(),
        worker_identity: "integration-worker".into(),
        ..Default::default()
    });
    rpc.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    rpc
}

async fn fencing_case<R: RunRepository + LeaseRepository + 'static>(
    store: Arc<R>,
    clock: Arc<TestClock>,
    index: u32,
    during: bool,
    duplicates: usize,
) {
    let (_directory, config) = credential();
    let mut old = node(store.clone(), "former-writer", &config, clock.clone()).await;
    let current = node(store.clone(), "current-writer", &config, clock.clone()).await;
    let req = request(index, &clock);
    let home = execution(&req);
    let queue = normal_queue(&req);
    let qhome = queue_home(&queue);
    let epoch = old.runtime.acquire_shard(home).await.unwrap();
    let qepoch = current.runtime.acquire_shard(qhome).await.unwrap();
    route(&[&old, &current], &[(home, 0, epoch), (qhome, 1, qepoch)]);
    old.runtime.start_workflow(req.clone()).await.unwrap();
    let hint = old.receipt().await;
    assert_eq!(hint.outcome, Ok(WorkflowOfferOutcome::Admitted));
    let mut client = old.client().await;
    let pending = if during {
        let (entered, resume) = old.repo.pause_commit(req.run_key);
        let rpc = start_wire(&req, 1);
        let attempt = tokio::spawn(async move { client.start_workflow_task(rpc).await });
        tokio::time::timeout(Duration::from_secs(3), entered)
            .await
            .unwrap()
            .unwrap();
        Some((attempt, resume))
    } else {
        None
    };
    store
        .relinquish_bundle(home, "former-writer".into(), epoch)
        .await
        .unwrap();
    let next = current.runtime.acquire_shard(home).await.unwrap();
    assert!(next.0 > epoch.0);
    if let Some((attempt, resume)) = pending {
        resume.send(()).unwrap();
        assert!(attempt.await.unwrap().is_err());
    } else {
        assert!(
            old.client()
                .await
                .start_workflow_task(start_wire(&req, 1))
                .await
                .is_err()
        );
    }
    let LoadedRun::Existing(before) = store.load_run(req.run_key).await.unwrap() else {
        panic!("run missing")
    };
    assert!(
        before
            .pending_workflow_task
            .as_ref()
            .unwrap()
            .started_event_id
            .is_none()
    );
    old.runtime.relinquish_shard(home).await;
    route(&[&old, &current], &[(home, 1, next), (qhome, 1, qepoch)]);
    // The hint was captured while the former writer owned execution. The
    // receiver must not infer authority or obsolescence from its publisher.
    let sink = PeerWorkflowOfferSink::new(old.cache.clone(), "unused".into(), old.peer.clone());
    for _ in 0..duplicates {
        assert_eq!(
            sink.offer(&hint.offer, qhome, false).await.unwrap(),
            WorkflowOfferOutcome::Known
        );
    }
    let mut current_client = current.client().await;
    for obsolete in 2..2 + duplicates as u64 {
        let loads = current.repo.loads.load(Ordering::SeqCst);
        assert_eq!(
            current_client
                .start_workflow_task(start_wire(&req, obsolete))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            current.repo.loads.load(Ordering::SeqCst) - loads,
            if obsolete == 2 { 2 } else { 1 }
        );
    }
    let loads = current.repo.loads.load(Ordering::SeqCst);
    let started = current_client
        .start_workflow_task(start_wire(&req, 1))
        .await
        .unwrap()
        .into_inner();
    let started: tokeira_runtime::StartedWorkflowTask =
        serde_json::from_slice(&started.started_task_json).unwrap();
    assert_eq!(started.token.shard_epoch, next);
    assert_eq!(
        current.repo.loads.load(Ordering::SeqCst) - loads,
        1,
        "obsolete attempts already warmed the owner cache"
    );
    for _ in 0..duplicates {
        assert_eq!(
            current_client
                .start_workflow_task(start_wire(&req, 1))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
    let history = store.read_history_to_end(req.run_key, 0).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(
                event.kind,
                tokeira_kernel::HistoryEventKind::WorkflowTaskStarted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(current.repo.normal.reads.load(Ordering::SeqCst), 0);
    let mut failure = tonic::Request::new(
        tokeira_proto::workflowservice::RespondWorkflowTaskFailedRequest {
            namespace: "default".into(),
            task_token: serde_json::to_vec(&started.token).unwrap(),
            identity: "integration-worker".into(),
            cause: tokeira_proto::enums::WorkflowTaskFailedCause::UnhandledCommand as i32,
            ..Default::default()
        },
    );
    failure.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    WorkflowServiceClient::connect(format!("http://{}", current.address))
        .await
        .unwrap()
        .respond_workflow_task_failed(failure)
        .await
        .unwrap();
    let LoadedRun::Existing(successor) = store.load_run(req.run_key).await.unwrap() else {
        panic!("successor")
    };
    let next_seq = successor
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .logical_seq
        .0;
    assert!(next_seq > 1);
    for _ in 0..duplicates {
        assert_eq!(
            current_client
                .start_workflow_task(start_wire(&req, 1))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
    let retried = current_client
        .start_workflow_task(start_wire(&req, next_seq))
        .await
        .unwrap()
        .into_inner();
    let retried: tokeira_runtime::StartedWorkflowTask =
        serde_json::from_slice(&retried.started_task_json).unwrap();
    assert_eq!(retried.token.logical_seq.0, next_seq);
    let mut complete = tonic::Request::new(
        tokeira_proto::workflowservice::RespondWorkflowTaskCompletedRequest {
            namespace: "default".into(),
            task_token: serde_json::to_vec(&retried.token).unwrap(),
            identity: "integration-worker".into(),
            ..Default::default()
        },
    );
    complete.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    WorkflowServiceClient::connect(format!("http://{}", current.address))
        .await
        .unwrap()
        .respond_workflow_task_completed(complete)
        .await
        .unwrap();
    let history = store.read_history_to_end(req.run_key, 0).await.unwrap();
    let scheduled = history
        .iter()
        .filter_map(|event| match event.kind {
            tokeira_kernel::HistoryEventKind::WorkflowTaskStarted {
                scheduled_event_id, ..
            } => Some(scheduled_event_id),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(scheduled.len(), 2);
    cleanup_run(&current, &req).await;
    current.runtime.relinquish_shard(home).await;
    current.runtime.relinquish_shard(qhome).await;
    store
        .relinquish_bundle(home, "current-writer".into(), next)
        .await
        .unwrap();
    store
        .relinquish_bundle(qhome, "current-writer".into(), qepoch)
        .await
        .unwrap();
    current.stop().await;
    old.stop().await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(concat!(env!("CARGO_MANIFEST_DIR"), "/src/peer/delivery_tests.proptest-regressions")))), ..ProptestConfig::default() })]
    // Feature: queue-home-delivery, Property 3: only the current writer commits once; a former publisher's current hint survives takeover.
    #[test]
    fn property_3_fenced_starts_and_delayed_hints(index in 0u32..1000, during in any::<bool>(), duplicates in 1usize..5) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let clock = TestClock::new();
            fencing_case(Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone())), clock, index, during, duplicates).await;
        });
    }

    // Feature: queue-home-delivery, Property 3: a paused push cannot admit into a retired queue acquisition, and its current hint can be retried at the replacement.
    #[test]
    fn property_3_queue_movement_mid_push(index in 0u32..1000) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let clock = TestClock::new(); let store = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
            movement_case(store, clock, index).await;
        });
    }
}

async fn movement_case<R: RunRepository + LeaseRepository + 'static>(
    store: Arc<R>,
    clock: Arc<TestClock>,
    index: u32,
) {
    let (_directory, config) = credential();
    let mut owner = node(store.clone(), "publisher", &config, clock.clone()).await;
    let old_queue = node(store.clone(), "former-queue", &config, clock.clone()).await;
    let req = request(index, &clock);
    let home = execution(&req);
    let queue = normal_queue(&req);
    let qhome = queue_home(&queue);
    let epoch = owner.runtime.acquire_shard(home).await.unwrap();
    let qepoch = old_queue.runtime.acquire_shard(qhome).await.unwrap();
    route(
        &[&owner, &old_queue],
        &[(home, 0, epoch), (qhome, 1, qepoch)],
    );
    old_queue.faults.mode.store(3, Ordering::SeqCst);
    owner.runtime.start_workflow(req.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), old_queue.faults.reached.notified())
        .await
        .unwrap();
    old_queue.runtime.relinquish_shard(qhome).await;
    store
        .relinquish_bundle(qhome, "former-queue".into(), qepoch)
        .await
        .unwrap();
    let next = owner.runtime.acquire_shard(qhome).await.unwrap();
    old_queue.faults.mode.store(0, Ordering::SeqCst);
    old_queue.faults.release.notify_one();
    let receipt = owner.receipt().await;
    assert_eq!(receipt.outcome, Ok(WorkflowOfferOutcome::NotHome));
    assert!(
        old_queue
            .runtime
            .broker()
            .try_claim_workflow_task(&queue, req.run_key)
            .await
            .is_none()
    );
    route(&[&owner, &old_queue], &[(home, 0, epoch), (qhome, 0, next)]);
    let sink = PeerWorkflowOfferSink::new(
        old_queue.cache.clone(),
        "unused".into(),
        old_queue.peer.clone(),
    );
    assert!(matches!(
        sink.offer(&receipt.offer, qhome, false).await.unwrap(),
        WorkflowOfferOutcome::Admitted | WorkflowOfferOutcome::Known
    ));
    assert!(!owner.poll(&queue, None).await.task_token.is_empty());
    let history = store.read_history_to_end(req.run_key, 0).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(
                event.kind,
                tokeira_kernel::HistoryEventKind::WorkflowTaskStarted { .. }
            ))
            .count(),
        1
    );
    assert_eq!(old_queue.repo.normal.reads.load(Ordering::SeqCst), 0);
    cleanup_run(&owner, &req).await;
    owner.runtime.relinquish_shard(home).await;
    owner.runtime.relinquish_shard(qhome).await;
    store
        .relinquish_bundle(home, "publisher".into(), epoch)
        .await
        .unwrap();
    store
        .relinquish_bundle(qhome, "publisher".into(), next)
        .await
        .unwrap();
    old_queue.stop().await;
    owner.stop().await;
}

#[cfg(feature = "dsql-integration")]
#[tokio::test]
async fn queue_home_delivery_live_fenced_starts() -> Result<()> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(());
    };
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
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
    let repo = Arc::new(repo);
    for index in 0..100 {
        let clock = Arc::new(TestClock(AtomicI64::new(
            OffsetDateTime::now_utc().unix_timestamp(),
        )));
        fencing_case(
            repo.clone(),
            clock.clone(),
            index,
            index % 2 == 0,
            1 + index as usize % 4,
        )
        .await;
        movement_case(repo.clone(), clock, index).await;
    }
    director.shutdown().await?;
    Ok(())
}

async fn set_version<R: RunRepository + 'static>(node: &Node<R>, queue: &QueueKey, build: &str) {
    let registry = node.runtime.deployment_registry().unwrap();
    for version in ["v1", "v2"] {
        registry
            .register_polled_deployment(
                tokeira_runtime::deployment_registry::RegisterPolledDeployment {
                    namespace_id: queue.namespace_id,
                    deployment_name: tokeira_storage::DeploymentName("deployment".into()),
                    build_id: tokeira_storage::BuildId(version.into()),
                    task_queue: queue.task_queue.0.clone(),
                    task_queue_type: tokeira_storage::DeploymentTaskQueueType::Workflow,
                    identity: "integration-worker".into(),
                },
            )
            .await
            .unwrap();
    }
    registry
        .set_current_version(tokeira_runtime::deployment_registry::SetCurrent {
            namespace_id: queue.namespace_id,
            deployment_name: tokeira_storage::DeploymentName("deployment".into()),
            build_id: Some(tokeira_storage::BuildId(build.into())),
            conflict_token: None,
            identity: "operator".into(),
            allow_no_pollers: false,
            ignore_missing_task_queues: true,
        })
        .await
        .unwrap();
}

async fn sticky_case(changed: bool, loss: u8) {
    let clock = TestClock::new();
    let store = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
    let (_directory, config) = credential();
    let mut owner = node(store.clone(), "sticky-execution", &config, clock.clone()).await;
    let sticky_node = node(store.clone(), "sticky-queue", &config, clock.clone()).await;
    let normal_node = node(store.clone(), "normal-queue", &config, clock.clone()).await;
    let req = request(19, &clock);
    let home = execution(&req);
    let normal = normal_queue(&req);
    let normal_home = queue_home(&normal);
    let sticky = (0..100)
        .map(|i| QueueKey {
            task_queue: TaskQueueName(format!("sticky-{i}")),
            ..normal.clone()
        })
        .find(|q| queue_home(q) != home && queue_home(q) != normal_home)
        .unwrap();
    let sticky_home = queue_home(&sticky);
    let epoch = owner.runtime.acquire_shard(home).await.unwrap();
    let sticky_epoch = sticky_node
        .runtime
        .acquire_shard(sticky_home)
        .await
        .unwrap();
    let normal_epoch = normal_node
        .runtime
        .acquire_shard(normal_home)
        .await
        .unwrap();
    route(
        &[&owner, &sticky_node, &normal_node],
        &[
            (home, 0, epoch),
            (sticky_home, 1, sticky_epoch),
            (normal_home, 2, normal_epoch),
        ],
    );
    owner.runtime.start_workflow(req.clone()).await.unwrap();
    assert_eq!(
        owner.receipt().await.outcome,
        Ok(WorkflowOfferOutcome::Admitted)
    );
    let first = normal_node.poll(&normal, None).await;
    if changed {
        for node in [&owner, &sticky_node, &normal_node] {
            set_version(node, &normal, "v1").await;
        }
        sticky_node
            .runtime
            .broker()
            .poll_workflow_task(
                &sticky,
                &WorkerIdentity("integration-worker".into()),
                Duration::ZERO,
            )
            .await
            .unwrap();
        sticky_node.faults.mode.store(3, Ordering::SeqCst);
    } else if loss == 1 {
        sticky_node
            .runtime
            .broker()
            .poll_workflow_task(
                &sticky,
                &WorkerIdentity("integration-worker".into()),
                Duration::ZERO,
            )
            .await
            .unwrap();
        sticky_node.faults.mode.store(1, Ordering::SeqCst);
    } else if loss == 2 {
        normal_node.faults.mode.store(1, Ordering::SeqCst);
    } else if loss == 3 {
        normal_node.faults.mode.store(2, Ordering::SeqCst);
    }
    let mut complete = tonic::Request::new(
        tokeira_proto::workflowservice::RespondWorkflowTaskCompletedRequest {
            namespace: "default".into(),
            task_token: first.task_token,
            identity: "integration-worker".into(),
            force_create_new_workflow_task: true,
            sticky_attributes: Some(
                tokeira_proto::public::temporal::api::taskqueue::v1::StickyExecutionAttributes {
                    worker_task_queue: Some(TaskQueue {
                        name: sticky.task_queue.0.clone(),
                        kind: 2,
                        normal_name: normal.task_queue.0.clone(),
                    }),
                    schedule_to_start_timeout: Some(prost_types::Duration {
                        seconds: 600,
                        nanos: 0,
                    }),
                },
            ),
            versioning_behavior: if changed { 2 } else { 0 },
            deployment_options: changed.then(|| {
                tokeira_proto::public::temporal::api::deployment::v1::WorkerDeploymentOptions {
                    deployment_name: "deployment".into(),
                    build_id: "v1".into(),
                    worker_versioning_mode: 2,
                }
            }),
            ..Default::default()
        },
    );
    complete.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    WorkflowServiceClient::connect(format!("http://{}", owner.address))
        .await
        .unwrap()
        .respond_workflow_task_completed(complete)
        .await
        .unwrap();
    if changed {
        tokio::time::timeout(
            Duration::from_secs(3),
            sticky_node.faults.reached.notified(),
        )
        .await
        .unwrap();
        for node in [&owner, &sticky_node, &normal_node] {
            set_version(node, &normal, "v2").await;
        }
        sticky_node.faults.mode.store(0, Ordering::SeqCst);
        sticky_node.faults.release.notify_one();
    }
    let LoadedRun::Existing(pending) = store.load_run(req.run_key).await.unwrap() else {
        panic!("pending")
    };
    let deadline = pending
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .schedule_to_start_deadline
        .unwrap();
    let affinity = pending.sticky.clone();
    if loss == 1 {
        for _ in 0..3 {
            let result = owner.receipt().await;
            assert_eq!(result.destination, sticky_home);
            assert_eq!(result.outcome, Err(WorkflowOfferError::Transport));
        }
    } else {
        let unavailable = owner.receipt().await;
        assert_eq!(unavailable.destination, sticky_home);
        assert_eq!(
            unavailable.outcome,
            Ok(WorkflowOfferOutcome::StickyUnavailable(if changed {
                tokeira_runtime::StickyUnavailableReason::DeploymentChanged
            } else {
                tokeira_runtime::StickyUnavailableReason::NoRecentPoller
            }))
        );
        for _ in 0..if loss == 0 { 1 } else { 2 } {
            let result = owner.receipt().await;
            assert_eq!(result.destination, normal_home);
            assert_eq!(result.offer.hint.task.sticky_deadline, Some(deadline));
            assert_eq!(
                result.outcome,
                if loss == 0 {
                    Ok(WorkflowOfferOutcome::Admitted)
                } else {
                    Err(WorkflowOfferError::Transport)
                }
            );
        }
    }
    assert_eq!(owner.repo.sticky.reads.load(Ordering::SeqCst), 0);
    assert_eq!(normal_node.repo.normal.reads.load(Ordering::SeqCst), 0);
    if loss != 0 {
        if loss == 3 {
            normal_node.runtime.broker().remove_run(req.run_key).await;
        }
        sticky_node.faults.mode.store(0, Ordering::SeqCst);
        normal_node.faults.mode.store(0, Ordering::SeqCst);
        clock.advance(331);
        // Only the execution-home sticky reader is released. Normal discovery
        // cannot recover this durable sticky row, including its normal retarget.
        owner.repo.sticky.release();
        let recovered = owner.receipt().await;
        assert_eq!(recovered.destination, sticky_home);
        if loss == 1 {
            assert_eq!(recovered.outcome, Ok(WorkflowOfferOutcome::Admitted));
        } else {
            assert_eq!(
                recovered.outcome,
                Ok(WorkflowOfferOutcome::StickyUnavailable(
                    tokeira_runtime::StickyUnavailableReason::NoRecentPoller
                ))
            );
            assert_eq!(
                owner.receipt().await.outcome,
                Ok(WorkflowOfferOutcome::Admitted)
            );
        }
        assert!(owner.repo.sticky.reads.load(Ordering::SeqCst) > 0);
    }
    let LoadedRun::Existing(after) = store.load_run(req.run_key).await.unwrap() else {
        panic!("after")
    };
    assert_eq!(after.sticky, affinity);
    assert_eq!(
        after
            .pending_workflow_task
            .as_ref()
            .unwrap()
            .schedule_to_start_deadline,
        Some(deadline)
    );
    if changed {
        assert!(
            !normal_node
                .poll_versioned(&normal, None, Some("v2"))
                .await
                .task_token
                .is_empty()
        );
    } else if loss == 1 {
        assert!(
            !sticky_node
                .poll(&sticky, Some(&normal))
                .await
                .task_token
                .is_empty()
        );
    } else {
        assert!(!normal_node.poll(&normal, None).await.task_token.is_empty());
    }
    assert_eq!(normal_node.repo.normal.reads.load(Ordering::SeqCst), 0);
    normal_node.stop().await;
    sticky_node.stop().await;
    owner.stop().await;
}

#[tokio::test]
async fn grpc_three_homes_no_recent_poller() {
    sticky_case(false, 0).await;
}
#[tokio::test]
async fn grpc_three_homes_deployment_changed() {
    sticky_case(true, 0).await;
}
#[tokio::test]
async fn grpc_dropped_sticky_push_recovered_by_sweep() {
    sticky_case(false, 1).await;
}
#[tokio::test]
async fn grpc_dropped_fallback_push_recovered_by_sweep() {
    sticky_case(false, 2).await;
}
#[tokio::test]
async fn grpc_dropped_acknowledgement_recovered_by_sweep() {
    sticky_case(false, 3).await;
}

#[tokio::test]
async fn grpc_peer_policy_capacity_statuses_and_retirement() {
    let clock = TestClock::new();
    let store = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
    let (_directory, config) = credential();
    let node = node(store.clone(), "policy", &config, clock.clone()).await;
    let req = request(81, &clock);
    let q = normal_queue(&req);
    let home = queue_home(&q);
    let epoch = node.runtime.acquire_shard(home).await.unwrap();
    let before = store.snapshot().await.unwrap();
    let mut client = node.client().await;
    let offer = offer_wire(&req, home);
    assert_eq!(
        client
            .offer_workflow_task(offer.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let mut worker = tonic::Request::new(offer.clone());
    worker.metadata_mut().insert(
        "authorization",
        "Bearer integration-worker".parse().unwrap(),
    );
    assert_eq!(
        client.offer_workflow_task(worker).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let token = node
        .peer
        .signer()
        .outbound_token_at(node.peer.unix_seconds().unwrap())
        .unwrap();
    let mut wrong_purpose = tonic::Request::new(offer.clone());
    wrong_purpose
        .metadata_mut()
        .insert("authorization", token.authorization().parse().unwrap());
    assert_eq!(
        client
            .offer_workflow_task(wrong_purpose)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut malformed = offer.clone();
    malformed.logical_seq = 0;
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, malformed))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut metadata = authorized_offer(&node.peer, offer.clone());
    metadata
        .metadata_mut()
        .insert("x-padding", "x".repeat(8192).parse().unwrap());
    assert_eq!(
        client
            .offer_workflow_task(metadata)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let mut misplaced = offer.clone();
    misplaced.destination_home = (home.0 + 1) % 8;
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, misplaced))
            .await
            .unwrap()
            .get_ref()
            .outcome,
        wire::OfferOutcome::NotHome as i32
    );
    let mut expired = offer.clone();
    expired.sticky_origin = Some(wire::OfferStickyOrigin {
        sticky_queue: Some(wire::OfferQueue {
            name: "expired-sticky".into(),
            ..Default::default()
        }),
        normal_queue: expired.selected_queue.clone(),
        worker_identity: "integration-worker".into(),
        deadline: expired.scheduled_at,
    });
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, expired))
            .await
            .unwrap()
            .get_ref()
            .outcome,
        wire::OfferOutcome::Expired as i32
    );

    node.faults.mode.store(4, Ordering::SeqCst);
    let mut held = Vec::new();
    for _ in 0..MAX_OFFER_HANDLERS {
        let address = node.address;
        let request = authorized_offer(&node.peer, offer.clone());
        held.push(tokio::spawn(async move {
            let mut client = WorkflowTaskDispatchClient::connect(format!("http://{address}"))
                .await
                .unwrap();
            client.offer_workflow_task(request).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while node.faults.held.load(Ordering::SeqCst) < MAX_OFFER_HANDLERS {
            node.faults.reached.notified().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "held {} of {MAX_OFFER_HANDLERS} wire requests",
            node.faults.held.load(Ordering::SeqCst)
        )
    });
    node.faults.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, offer.clone()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    node.faults.release.notify_waiters();
    for response in held {
        assert!(matches!(
            wire::OfferOutcome::try_from(response.await.unwrap().unwrap().get_ref().outcome)
                .unwrap(),
            wire::OfferOutcome::Admitted | wire::OfferOutcome::Known
        ));
    }
    node.runtime.broker().remove_run(req.run_key).await;

    for index in 0..257 {
        let mut value = offer.clone();
        value.run_key = uuid::Uuid::from_u128(index + 1).as_bytes().to_vec();
        let outcome = client
            .offer_workflow_task(authorized_offer(&node.peer, value))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            outcome.outcome,
            if index < 256 {
                wire::OfferOutcome::Admitted
            } else {
                wire::OfferOutcome::Full
            } as i32
        );
    }
    let mut retained = offer.clone();
    retained.run_key = uuid::Uuid::from_u128(1).as_bytes().to_vec();
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, retained.clone()))
            .await
            .unwrap()
            .get_ref()
            .outcome,
        wire::OfferOutcome::Known as i32
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
    node.runtime.relinquish_shard(home).await;
    assert!(
        node.runtime
            .broker()
            .poll_workflow_task(&q, &WorkerIdentity("worker".into()), Duration::ZERO)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, retained.clone()))
            .await
            .unwrap()
            .get_ref()
            .outcome,
        wire::OfferOutcome::NotHome as i32
    );
    store
        .relinquish_bundle(home, "policy".into(), epoch)
        .await
        .unwrap();
    node.runtime.acquire_shard(home).await.unwrap();
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, retained.clone()))
            .await
            .unwrap()
            .get_ref()
            .outcome,
        wire::OfferOutcome::Admitted as i32
    );
    assert_eq!(
        client
            .offer_workflow_task(authorized_offer(&node.peer, retained))
            .await
            .unwrap()
            .get_ref()
            .outcome,
        wire::OfferOutcome::Known as i32
    );
    assert!(
        node.runtime
            .broker()
            .try_claim_workflow_task(&q, RunKey(uuid::Uuid::from_u128(1)))
            .await
            .is_some()
    );
    assert_eq!(node.repo.normal.reads.load(Ordering::SeqCst), 0);
    node.stop().await;
}

#[tokio::test]
async fn grpc_dropped_normal_push_recovered_by_discovery() {
    let clock = TestClock::new();
    let store = Arc::new(InMemoryStore::with_shard_count(8).with_clock(clock.clone()));
    let (_directory, config) = credential();
    let mut owner = node(store.clone(), "normal-execution", &config, clock.clone()).await;
    let matching = node(store.clone(), "normal-matching", &config, clock.clone()).await;
    let req = request(24, &clock);
    let home = execution(&req);
    let q = normal_queue(&req);
    let qhome = queue_home(&q);
    let epoch = owner.runtime.acquire_shard(home).await.unwrap();
    let qepoch = matching.runtime.acquire_shard(qhome).await.unwrap();
    route(
        &[&owner, &matching],
        &[(home, 0, epoch), (qhome, 1, qepoch)],
    );
    matching.faults.mode.store(1, Ordering::SeqCst);
    owner.runtime.start_workflow(req.clone()).await.unwrap();
    for _ in 0..3 {
        assert_eq!(
            owner.receipt().await.outcome,
            Err(WorkflowOfferError::Transport)
        );
    }
    assert!(!matching.runtime.broker().has_runnable_backlog(&q).await);
    matching.faults.mode.store(0, Ordering::SeqCst);
    matching.repo.normal.release();
    assert!(!matching.poll(&q, None).await.task_token.is_empty());
    assert!(matching.repo.normal.reads.load(Ordering::SeqCst) > 0);
    assert_eq!(owner.repo.sticky.reads.load(Ordering::SeqCst), 0);
    matching.stop().await;
    owner.stop().await;
}

async fn cleanup_run<R: RunRepository + 'static>(owner: &Node<R>, req: &StartRequest) {
    let now = owner.runtime.clock().now();
    owner
        .runtime
        .submit(
            req.run_key,
            Command::Terminate(TerminateRequest {
                reason: "fixture cleanup".into(),
                details: None,
                identity: "integration".into(),
                links: Vec::new(),
                request: RequestContext::unattributed(now),
                now,
            }),
        )
        .await
        .unwrap();
    owner.repo.inner.purge_run(req.run_key).await.unwrap();
}
