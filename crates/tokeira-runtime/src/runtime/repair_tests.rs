//! Fault and synchronization fixtures exercise the actual acquisition lifecycle.

use super::*;
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokeira_kernel::*;
use tokeira_storage::*;
use tokeira_types::*;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Point {
    Candidates,
    Repair,
    Home,
    Commit,
}

#[derive(Debug)]
struct Pause {
    point: Point,
    entered: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[derive(Debug, Default)]
struct Repo {
    inner: InMemoryStore,
    fail: Mutex<Option<Point>>,
    conflicts: AtomicUsize,
    repair_attempts: AtomicUsize,
    pause: Mutex<Option<Pause>>,
}

impl Repo {
    fn pause(&self, point: Point) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (resume, waiting) = oneshot::channel();
        *self.pause.lock().unwrap() = Some(Pause {
            point,
            entered,
            resume: waiting,
        });
        (observed, resume)
    }
    async fn hit(&self, point: Point) -> Result<()> {
        if point == Point::Repair {
            self.repair_attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(WorkflowDispatchRepairConflict.into());
            }
        }
        let fail = {
            let mut fail = self.fail.lock().unwrap();
            if *fail == Some(point) {
                fail.take();
                true
            } else {
                false
            }
        };
        if fail {
            anyhow::bail!("injected {point:?} failure");
        }
        let pause = {
            let mut pause = self.pause.lock().unwrap();
            if pause.as_ref().is_some_and(|p| p.point == point) {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            pause
                .resume
                .await
                .map_err(|_| anyhow!("injected interrupted {point:?}"))?;
        }
        Ok(())
    }
}

fn runtime(repo: Arc<Repo>) -> Arc<TokeiraRuntime<Repo>> {
    let runtime = Arc::new(TokeiraRuntime::new(
        repo,
        1,
        LaneConfig::default(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(86400),
            ..Default::default()
        },
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));
    runtime.shard_owner.write().unwrap().enable_reconciliation();
    runtime
}

async fn seed(repo: &Repo) -> WorkflowState {
    let queue = QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("repair".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let transition = crate::discovery::tests::transition(&queue, 0);
    let CommitResult::Applied { new_state } = repo
        .inner
        .commit_transition(transition.next_state.run_key, transition, ShardEpoch::ZERO)
        .await
        .unwrap()
    else {
        panic!("seed failed")
    };
    new_state
}

async fn stop(runtime: &TokeiraRuntime<Repo>) {
    runtime.runtime_shutdown.begin_shutdown();
    runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test]
async fn failures_in_either_walk_remain_non_serving_and_restart_from_head() {
    for point in [Point::Candidates, Point::Repair, Point::Home] {
        let repo = Arc::new(Repo::default());
        seed(&repo).await;
        let before = repo.inner.snapshot().await.unwrap();
        let runtime = runtime(repo.clone());
        *repo.fail.lock().unwrap() = Some(point);
        assert!(
            runtime
                .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
                .await
                .is_err()
        );
        assert!(runtime.active_shards().is_empty());
        assert!(runtime.workflow_timeout_tracking.snapshot().is_empty());
        assert!(runtime.activity_tracking.snapshot().is_empty());
        assert!(runtime.nexus_timeout_tracking.snapshot().is_empty());
        assert!(runtime.completion_callback_tracking.snapshot().is_empty());
        runtime
            .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
            .await
            .unwrap();
        assert_eq!(runtime.active_shards(), vec![ShardId(0)]);
        assert_eq!(repo.inner.snapshot().await.unwrap(), before);
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn repair_retries_whole_calls_and_exhaustion_never_serves() {
    for conflicts in [2, 100] {
        let repo = Arc::new(Repo::default());
        seed(&repo).await;
        repo.conflicts.store(conflicts, Ordering::SeqCst);
        let runtime = runtime(repo.clone());
        let result = runtime
            .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
            .await;
        if conflicts == 2 {
            assert!(result.is_ok());
            assert!(repo.repair_attempts.load(Ordering::SeqCst) >= 4);
        } else {
            assert!(result.is_err());
            assert!(runtime.active_shards().is_empty());
            assert_eq!(
                repo.repair_attempts.load(Ordering::SeqCst),
                runtime.config.max_occ_retries as usize + 1
            );
        }
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn interrupted_home_walk_cannot_publish_active() {
    let repo = Arc::new(Repo::default());
    seed(&repo).await;
    let runtime = runtime(repo.clone());
    let (entered, resume) = repo.pause(Point::Home);
    let acquiring = runtime.clone();
    let task = tokio::spawn(async move {
        acquiring
            .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
            .await
    });
    entered.await.unwrap();
    assert!(runtime.active_shards().is_empty());
    drop(resume);
    assert!(task.await.unwrap().is_err());
    assert!(runtime.active_shards().is_empty());
    runtime
        .recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO)
        .await
        .unwrap();
    assert_eq!(runtime.active_shards(), vec![ShardId(0)]);
    stop(&runtime).await;
}

#[tokio::test]
async fn acquisition_drains_an_admitted_lane_commit_before_reading_repair_pages() {
    let repo = Arc::new(Repo {
        inner: InMemoryStore::with_shard_count(8),
        ..Default::default()
    });
    let (state, home) = loop {
        let state = seed(&repo).await;
        let home = execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            8,
        );
        if home != crate::shard::shard_for(state.run_key, 8) {
            break (state, home);
        }
    };
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo.clone(),
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
        "repair-home-test".into(),
        "127.0.0.1:0".into(),
        false,
        None,
    ));
    {
        let mut owner = runtime.shard_owner.write().unwrap();
        owner.enable_reconciliation();
        owner.record_acquired(home, ShardEpoch::ZERO);
        owner.mark_active(home);
    }
    let old = runtime
        .shard_owner
        .read()
        .unwrap()
        .acquisition(home)
        .unwrap();
    let (committing, finish_commit) = repo.pause(Point::Commit);
    let lane = runtime.lanes[0].clone();
    let run_key = state.run_key;
    let command = Command::Signal(SignalRequest {
        signal_name: "before-acquisition".into(),
        input: Payloads::default(),
        header: None,
        links: vec![],
        request: RequestContext {
            request_id: RequestId("repair-test-request".into()),
            caller_identity: None,
            principal: None,
            received_at: state.started_at,
        },
        now: state.started_at,
    });
    let writer = tokio::spawn(async move { lane.submit(run_key, command).await });
    committing.await.unwrap();
    let (reading, finish_read) = repo.pause(Point::Candidates);
    let acquiring = runtime.clone();
    let recovery = tokio::spawn(async move {
        acquiring
            .recover_self_assigned_shard(home, ShardEpoch::ZERO)
            .await
    });
    old.cancel.cancelled().await;
    assert!(runtime.active_shards().is_empty());
    assert!(
        runtime
            .shard_owner
            .read()
            .unwrap()
            .write_barrier(home)
            .try_write()
            .is_err()
    );
    assert_eq!(
        repo.inner.load_run(run_key).await.unwrap(),
        LoadedRun::Existing(state.clone())
    );
    finish_commit.send(()).unwrap();
    assert!(matches!(
        writer.await.unwrap().unwrap(),
        CommitResult::Applied { .. }
    ));
    reading.await.unwrap();
    let before_repair = repo.inner.snapshot().await.unwrap();
    assert!(
        crate::serving_gate::admit(&runtime.shard_owner, home)
            .await
            .is_err()
    );
    finish_read.send(()).unwrap();
    recovery.await.unwrap().unwrap();
    assert_eq!(repo.inner.snapshot().await.unwrap(), before_repair);
    stop(&runtime).await;
}

#[async_trait]
impl RunRepository for Repo {
    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        self.inner.resolve_execution(execution).await
    }

    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        self.inner.find_latest_run(namespace_id, workflow_id).await
    }

    async fn list_runs_for_namespace(&self, namespace_id: NamespaceId) -> Result<Vec<RunKey>> {
        self.inner.list_runs_for_namespace(namespace_id).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        self.inner.load_run(run_key).await
    }

    async fn load_run_with_stats(&self, run_key: RunKey) -> Result<(LoadedRun, RunHistoryStats)> {
        self.inner.load_run_with_stats(run_key).await
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        self.inner
            .read_history(run_key, after_event_id, limit)
            .await
    }

    async fn read_attributed_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<AttributedHistoryEvent>> {
        self.inner
            .read_attributed_history(run_key, after_event_id, limit)
            .await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        self.inner
            .lookup_request_dedupe(execution, request_id)
            .await
    }

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        self.inner.read_transition_audit(run_key).await
    }

    async fn has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        self.inner
            .has_open_pinned_workflows(namespace_id, version)
            .await
    }

    async fn create_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule: WorkflowRuleRecord,
        max_rules: usize,
    ) -> Result<WorkflowRuleCreateResult> {
        self.inner
            .create_workflow_rule(namespace_id, rule, max_rules)
            .await
    }

    async fn get_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<Option<WorkflowRuleRecord>> {
        self.inner.get_workflow_rule(namespace_id, rule_id).await
    }

    async fn delete_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<WorkflowRuleDeleteResult> {
        self.inner.delete_workflow_rule(namespace_id, rule_id).await
    }

    async fn list_workflow_rules(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRuleRecord>> {
        self.inner.list_workflow_rules(namespace_id).await
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        self.inner
            .commit_transition(run_key, transition, epoch)
            .await
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        self.hit(Point::Commit).await?;
        self.inner
            .commit_transition_for_bundle(run_key, execution_home_bundle, transition, epoch)
            .await
    }

    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult> {
        self.inner
            .delete_run_for_bundle(run_key, execution_home_bundle, request, epoch)
            .await
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        self.inner
            .materialize_reset_successor(base_run_key, fork_event_id, successor_run_id)
            .await
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        self.inner
            .list_dispatchable_workflow_tasks(queue, limit)
            .await
    }

    async fn reconcile_workflow_dispatch_run(&self, home: ShardId, run_key: RunKey) -> Result<()> {
        self.hit(Point::Repair).await?;
        self.inner
            .reconcile_workflow_dispatch_run(home, run_key)
            .await
    }

    async fn list_workflow_dispatch_page(
        &self,
        range: &WorkflowDiscoveryRange,
        after: Option<WorkflowDispatchPosition>,
        limit: std::num::NonZeroU32,
    ) -> Result<WorkflowDispatchPage> {
        self.inner
            .list_workflow_dispatch_page(range, after, limit)
            .await
    }

    async fn list_workflow_dispatch_for_home(
        &self,
        home: ShardId,
        after: Option<RunKey>,
        limit: std::num::NonZeroU32,
    ) -> Result<Vec<RunKey>> {
        self.hit(Point::Home).await?;
        self.inner
            .list_workflow_dispatch_for_home(home, after, limit)
            .await
    }

    async fn list_due_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.inner
            .list_due_dispatchable_activity_tasks(queue, now, limit)
            .await
    }

    async fn list_all_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.inner
            .list_all_dispatchable_activity_tasks(queue, limit)
            .await
    }

    async fn delete_activity_dispatch_if_matches(
        &self,
        candidate: &ActivityDispatchIdentity,
    ) -> Result<bool> {
        self.inner
            .delete_activity_dispatch_if_matches(candidate)
            .await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        self.inner.persist_to_backlog(entries).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        self.inner.drain_backlog(queue, limit).await
    }

    async fn backlog_stats_by_priority(
        &self,
        queue: &QueueKey,
    ) -> Result<BTreeMap<i16, BacklogBandStats>> {
        self.inner.backlog_stats_by_priority(queue).await
    }

    async fn list_versioned_backlog_queue_keys(&self) -> Result<Vec<QueueKey>> {
        self.inner.list_versioned_backlog_queue_keys().await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        self.inner.list_due_timers(now, limit).await
    }

    async fn list_recovery_candidates_for_shard(
        &self,
        shard_id: ShardId,
        cursor: Option<&RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage> {
        self.hit(Point::Candidates).await?;
        self.inner
            .list_recovery_candidates_for_shard(shard_id, cursor, limit)
            .await
    }

    async fn list_due_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        after: Option<&DueActivityDispatch>,
        limit: usize,
    ) -> Result<Vec<DueActivityDispatch>> {
        self.inner
            .list_due_dispatchable_activity_tasks_for_shard(shard_id, now, after, limit)
            .await
    }

    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        after: Option<&DueTimer>,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        self.inner
            .list_due_timers_for_shard(shard_id, now, after, limit)
            .await
    }

    async fn delete_due_timer_if_matches(&self, timer: &DueTimer) -> Result<bool> {
        self.inner.delete_due_timer_if_matches(timer).await
    }
}
