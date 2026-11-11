//! Prepared starts revalidate lane state using routing captured before submission.
//! A closed deployment-read gate must not stall this lane or its other runs.

use super::*;
use crate::deployment_registry::{RegisterPolledDeployment, SetCurrent};
use async_trait::async_trait;
use proptest::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokeira_kernel::{Kernel, ResetStickyRequest};
use tokeira_storage::{
    ConflictToken, DeploymentCasResult, DeploymentTaskQueueType, InMemoryStore,
    StoredWorkerDeployment, WriteFence,
};
use tokio::sync::Notify;

struct Fixture {
    repo: Arc<InMemoryStore>,
    runtime: TokeiraRuntime<InMemoryStore>,
    registry: DeploymentRegistry,
    offered: DispatchableWorkflowTask,
}

async fn fixture(
    index: u32,
    sticky: bool,
    repo: Arc<InMemoryStore>,
    deployments: Arc<dyn WorkerDeploymentRepository>,
) -> Fixture {
    let runtime =
        super::super::discovery_tests::runtime(repo.clone(), crate::discovery::tests::home(0))
            .with_worker_deployment_repository(deployments);
    let registry = runtime.deployment_registry().unwrap();
    let queue = QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName(format!("version-{index}")),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let mut initial = crate::discovery::tests::transition(&queue, index as usize);
    initial.next_state.worker_deployment_name = Some("deployment".into());
    initial.next_state.versioning_info = Some(WorkflowVersioningInfo {
        behavior: VersioningBehavior::AutoUpgrade,
        deployment_version: Some(version("v1")),
        ..WorkflowVersioningInfo::default()
    });
    if sticky {
        initial.next_state.sticky = Some(tokeira_types::StickyAffinity {
            worker_identity: WorkerIdentity("worker".into()),
            sticky_queue: TaskQueueName(format!("sticky-{index}")),
            schedule_to_start_timeout: time::Duration::minutes(5),
        });
    }
    let key = initial.next_state.run_key;
    let selected = initial
        .next_state
        .sticky
        .as_ref()
        .map(|s| s.sticky_queue.clone())
        .unwrap_or_else(|| queue.task_queue.clone());
    let sequence = initial
        .next_state
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .logical_seq;
    repo.commit_transition(key, initial, WriteFence::Unfenced)
        .await
        .unwrap();
    for build in ["v1", "v2"] {
        registry
            .register_polled_deployment(RegisterPolledDeployment {
                namespace_id: queue.namespace_id,
                deployment_name: DeploymentName("deployment".into()),
                build_id: tokeira_storage::BuildId(build.into()),
                task_queue: queue.task_queue.0.clone(),
                task_queue_type: DeploymentTaskQueueType::Workflow,
                identity: "worker".into(),
            })
            .await
            .unwrap();
    }
    registry
        .set_current_version(current(queue.namespace_id, "v1"))
        .await
        .unwrap();
    let offered = DispatchableWorkflowTask {
        run_key: key,
        logical_seq: sequence,
        queue: QueueKey {
            task_queue: selected,
            deployment: Some(DeploymentId("deployment".into())),
            build_id: Some(BuildId("v1".into())),
            ..queue
        },
        sticky_preferred: sticky.then(|| WorkerIdentity("worker".into())),
        normal_queue: None,
        sticky_deadline: None,
        priority: None,
        order: None,
    };
    Fixture {
        repo,
        runtime,
        registry,
        offered,
    }
}

fn version(build: &str) -> WorkerDeploymentVersionRef {
    WorkerDeploymentVersionRef {
        deployment_name: "deployment".into(),
        build_id: build.into(),
    }
}

fn current(namespace_id: NamespaceId, build: &str) -> SetCurrent {
    SetCurrent {
        namespace_id,
        deployment_name: DeploymentName("deployment".into()),
        build_id: Some(tokeira_storage::BuildId(build.into())),
        conflict_token: None,
        identity: "operator".into(),
        allow_no_pollers: false,
        ignore_missing_task_queues: true,
    }
}

fn signal() -> Command {
    let now = OffsetDateTime::now_utc();
    Command::Signal(SignalRequest {
        signal_name: "unrelated".into(),
        input: Default::default(),
        header: None,
        links: Vec::new(),
        request: RequestContext {
            request_id: RequestId(Uuid::new_v4().to_string()),
            ..RequestContext::unattributed(now)
        },
        now,
    })
}

async fn prepare(f: &Fixture) -> (PolledWorkflowTaskTarget, WorkflowStartCheck) {
    let prepared = f
        .runtime
        .prepare_workflow_start(f.offered.run_key)
        .await
        .unwrap();
    let (target, routing) = f
        .runtime
        .resolve_prepared_start(&prepared, &f.offered, true)
        .await
        .unwrap();
    let check = WorkflowStartCheck {
        run_id: prepared.state.run_id,
        normal_queue: prepared.state.task_queue,
        offered: f.offered.clone(),
        target: target.clone(),
        routing,
    };
    (target, check)
}

async fn start(
    f: &Fixture,
    target: PolledWorkflowTaskTarget,
    check: WorkflowStartCheck,
) -> Result<StartedWorkflowTask> {
    f.runtime
        .start_polled_workflow_task_inner(
            f.offered.clone(),
            tokio::time::Instant::now(),
            WorkerIdentity("worker".into()),
            target,
            Some(check),
        )
        .await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: queue-home-delivery, Property 6. Routing changes after preparation retain the captured policy.
    #[test]
    fn property_6_prepared_lane_uses_captured_routing(index in 0u32..10000, sticky in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8));
            let f = fixture(index, sticky, repo.clone(), repo).await;
            let (target, check) = prepare(&f).await;
            f.registry.set_current_version(current(f.offered.queue.namespace_id, "v2")).await.unwrap();
            start(&f, target, check).await.unwrap();
            super::super::discovery_tests::stop(&f.runtime).await;
        });
    }

    // Feature: queue-home-delivery, Property 6. Changed sticky/version decisions are refused, including after OCC reload.
    #[test]
    fn property_6_prepared_lane_refuses_changed_decision(index in 0u32..10000, sticky in any::<bool>(), warm in any::<bool>(), external in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::with_shard_count(8));
            let f = fixture(index, sticky, repo.clone(), repo).await;
            let key = f.offered.run_key;
            if warm { f.runtime.submit(key, signal()).await.unwrap(); }
            let (target, check) = prepare(&f).await;
            let now = OffsetDateTime::now_utc();
            let command = if sticky {
                Command::ResetSticky(ResetStickyRequest { now })
            } else {
                Command::UpdateExecutionOptions(UpdateExecutionOptionsRequest {
                    versioning_override: VersioningOverrideChange::Set(VersioningOverride::Pinned { version: version("v2") }),
                    completion_callbacks: FieldChange::Unchanged, attached_completion_callbacks: Vec::new(), attached_links: Vec::new(),
                    attached_request_id: None, request: RequestContext::unattributed(now), now,
                    priority: FieldChange::Unchanged, completion_callback_limit: None,
                })
            };
            if external {
                let transition = BasicKernel.apply(f.repo.load_run(key).await.unwrap(), command).unwrap();
                f.repo.commit_transition(key, transition, WriteFence::Unfenced).await.unwrap();
            } else {
                f.runtime.submit(key, command).await.unwrap();
            }
            let before = f.repo.load_run(key).await.unwrap();
            let error = start(&f, target, check).await.unwrap_err();
            assert!(error.is::<crate::WorkflowOfferRejected>(), "{error:?}");
            assert_eq!(f.repo.load_run(key).await.unwrap(), before);
            super::super::discovery_tests::stop(&f.runtime).await;
        });
    }
}

struct GatedDeployments {
    repo: Arc<InMemoryStore>,
    closed: AtomicBool,
    blocked_reads: AtomicUsize,
    entered: Notify,
    release: Notify,
}

impl GatedDeployments {
    async fn read_gate(&self) {
        if self.closed.load(Ordering::SeqCst) {
            self.blocked_reads.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[async_trait]
impl WorkerDeploymentRepository for GatedDeployments {
    async fn load_deployment(&self, key: &DeploymentKey) -> Result<Option<StoredWorkerDeployment>> {
        self.read_gate().await;
        self.repo.load_deployment(key).await
    }

    async fn put_deployment(
        &self,
        record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult> {
        self.repo.put_deployment(record, expected).await
    }

    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult> {
        self.repo.delete_deployment(key, expected).await
    }

    async fn list_deployments(
        &self,
        namespace_id: NamespaceId,
        after: Option<&DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        self.read_gate().await;
        self.repo.list_deployments(namespace_id, after, limit).await
    }

    async fn list_all_for_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        self.read_gate().await;
        self.repo.list_all_for_namespace(namespace_id).await
    }
}

#[tokio::test]
async fn prepared_start_does_not_read_deployments_in_lane() {
    for sticky in [false, true] {
        let repo = Arc::new(InMemoryStore::with_shard_count(8));
        let gated = Arc::new(GatedDeployments {
            repo: repo.clone(),
            closed: AtomicBool::new(false),
            blocked_reads: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let f = fixture(0, sticky, repo, gated.clone()).await;
        let (target, check) = prepare(&f).await;
        let other = crate::discovery::tests::transition(
            &QueueKey {
                deployment: None,
                build_id: None,
                task_queue: TaskQueueName("other".into()),
                ..f.offered.queue.clone()
            },
            1,
        );
        let other_key = other.next_state.run_key;
        let other_seq = other.next_state.transition_seq;
        f.repo
            .commit_transition(other_key, other, WriteFence::Unfenced)
            .await
            .unwrap();
        gated.closed.store(true, Ordering::SeqCst);
        let reader = gated.clone();
        let key = DeploymentKey {
            namespace_id: f.offered.queue.namespace_id,
            deployment_name: DeploymentName("deployment".into()),
        };
        let blocked = tokio::spawn(async move { reader.load_deployment(&key).await });
        gated.entered.notified().await;
        assert!(!blocked.is_finished());
        let work = async {
            let (started, other) = tokio::join!(
                start(&f, target, check),
                f.runtime.submit(other_key, signal())
            );
            started.unwrap();
            other.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                biased;
                () = gated.entered.notified() => panic!("start read deployment storage inside the lane"),
                () = work => {}
            }
        }).await.unwrap();
        assert_eq!(gated.blocked_reads.load(Ordering::SeqCst), 1);
        assert!(!blocked.is_finished());
        let LoadedRun::Existing(other) = f.repo.load_run(other_key).await.unwrap() else {
            panic!("other run missing")
        };
        assert_eq!(other.transition_seq, other_seq.next());
        gated.closed.store(false, Ordering::SeqCst);
        gated.release.notify_one();
        blocked.await.unwrap().unwrap();
        super::super::discovery_tests::stop(&f.runtime).await;
    }
}
