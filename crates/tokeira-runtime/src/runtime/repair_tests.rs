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
    Load,
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
    loads: AtomicUsize,
    preparation_retries: AtomicUsize,
    preparation_attempts: AtomicUsize,
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
    fn placement_ready(&self) -> bool {
        self.inner.placement_ready()
    }
    async fn prepare_placement_page(&self) -> Result<PlacementPage> {
        self.preparation_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .preparation_retries
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Ok(PlacementPage::Retry);
        }
        self.inner.prepare_placement_page().await
    }

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
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.hit(Point::Load).await?;
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
        expected_current: Option<RunKey>,
    ) -> Result<()> {
        self.inner
            .materialize_reset_successor(
                base_run_key,
                fork_event_id,
                successor_run_id,
                expected_current,
            )
            .await
    }

    async fn abandon_materialization(&self, run_key: RunKey) -> Result<()> {
        self.inner.abandon_materialization(run_key).await
    }

    async fn purge_run(&self, run_key: RunKey) -> Result<()> {
        self.inner.purge_run(run_key).await
    }

    async fn list_run_bulk_writes(
        &self,
        shard_id: ShardId,
        after: Option<RunKey>,
        limit: usize,
    ) -> Result<Vec<tokeira_storage::RunBulkWrite>> {
        self.inner
            .list_run_bulk_writes(shard_id, after, limit)
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

    async fn persist_to_backlog(
        &self,
        entries: Vec<BacklogEntry>,
    ) -> std::result::Result<(), tokeira_storage::BacklogPersistError> {
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

    async fn delete_due_timer_if_matches(
        &self,
        timer: &DueTimer,
        reason: tokeira_storage::StaleTimer,
    ) -> Result<bool> {
        self.inner.delete_due_timer_if_matches(timer, reason).await
    }
}

fn timeout_runtime(repo: Arc<Repo>, home: ShardId) -> Arc<TokeiraRuntime<Repo>> {
    home_runtime(repo, home, true)
}

pub(super) fn home_runtime<R: RunRepository + 'static>(
    repo: Arc<R>,
    home: ShardId,
    repair: bool,
) -> Arc<TokeiraRuntime<R>> {
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        repo,
        1,
        LaneConfig::default(),
        TimerScannerConfig {
            scan_interval: std::time::Duration::from_secs(86400),
            ..Default::default()
        },
        WorkflowTimeoutScannerConfig {
            scan_interval: std::time::Duration::from_secs(86400),
            ..Default::default()
        },
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        "timeout-home-test".into(),
        "127.0.0.1:0".into(),
        false,
        None,
    ));
    {
        let mut owner = runtime.shard_owner.write().unwrap();
        if repair {
            owner.enable_reconciliation();
        }
        owner.record_acquired(home, ShardEpoch::ZERO);
        owner.mark_active(home);
    }
    runtime
}

fn timeout_start() -> (StartRequest, ShardId) {
    let mut request =
        super::tests::sample_start_request(Some(Duration::hours(1)), Some(Duration::seconds(2)));
    request.namespace_id = NamespaceId(uuid::Uuid::from_u128(1));
    let home = execution_home_bundle(
        request.namespace_id.0.as_bytes(),
        request.workflow_id.0.as_bytes(),
        8,
    );
    request.run_key = (1..100)
        .map(|i| RunKey(uuid::Uuid::from_u128(i)))
        .find(|key| crate::shard::shard_for(*key, 8) != home)
        .unwrap();
    (request, home)
}

async fn expire_tracked_run(runtime: &TokeiraRuntime<Repo>, run_key: RunKey, home: ShardId) {
    let entry = runtime
        .workflow_timeout_tracking
        .snapshot_for_shard(home)
        .into_iter()
        .find(|entry| entry.run_key == run_key)
        .expect("every committed successor must have its own timeout");
    let now = entry.started_at
        + entry.workflow_start_delay.unwrap_or(Duration::ZERO)
        + entry.workflow_run_timeout.unwrap()
        + Duration::milliseconds(1);
    crate::timeout::scan_workflow_timeouts_once(
        &runtime.workflow_timeout_tracking,
        Some(home),
        &WorkflowTimeoutScannerConfig::default(),
        now,
        |entry, violation, now| {
            crate::timeout::submit_workflow_timeout(
                runtime.repo.clone(),
                runtime.lanes.clone(),
                runtime.lanes.len(),
                entry,
                violation,
                now,
            )
        },
    )
    .await;
    let LoadedRun::Existing(state) = runtime.repo.load_run(run_key).await.unwrap() else {
        panic!("expired run disappeared")
    };
    assert_eq!(state.status, ExecutionStatus::TimedOut);
    assert!(
        runtime
            .repo
            .read_history(run_key, 0, 128)
            .await
            .unwrap()
            .iter()
            .any(|event| {
                matches!(
                    event.kind,
                    HistoryEventKind::WorkflowExecutionTimedOut {
                        timeout_type: WorkflowTimeoutType::RunTimeout,
                        ..
                    }
                )
            })
    );
}

#[tokio::test]
async fn timeout_retry_and_cron_successors_time_out_without_reacquisition() {
    for cron in [false, true] {
        let repo = Arc::new(Repo {
            inner: InMemoryStore::with_shard_count(8),
            ..Default::default()
        });
        let (mut request, home) = timeout_start();
        if cron {
            request.cron_schedule = Some("* * * * *".into());
        } else {
            request.retry_policy = Some(RetryPolicy {
                initial_interval: Duration::seconds(1),
                backoff_coefficient: 2.0,
                maximum_interval: None,
                maximum_attempts: 2,
                non_retryable_error_types: Vec::new(),
            });
        }
        let execution = ExecutionRef {
            namespace_id: request.namespace_id,
            workflow_id: request.workflow_id.clone(),
            run_id: None,
        };
        let first = request.run_key;
        let runtime = timeout_runtime(repo.clone(), home);
        assert!(matches!(
            runtime.lanes[0]
                .submit(first, Command::Start(request))
                .await
                .unwrap(),
            CommitResult::Applied { .. }
        ));
        expire_tracked_run(&runtime, first, home).await;
        let successor = repo.resolve_execution(&execution).await.unwrap().unwrap();
        assert_ne!(successor, first);
        let LoadedRun::Existing(state) = repo.load_run(successor).await.unwrap() else {
            panic!("successor missing")
        };
        assert_eq!(state.status, ExecutionStatus::Running);
        assert_eq!(state.attempt, if cron { 1 } else { 2 });
        assert_eq!(runtime.active_shards(), vec![home]);
        expire_tracked_run(&runtime, successor, home).await;
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn start_deadlines_survive_lost_reply_and_unrelated_home_reacquisition() {
    for variant in 0..3 {
        let repo = Arc::new(Repo {
            inner: InMemoryStore::with_shard_count(8),
            ..Default::default()
        });
        let (request, home) = timeout_start();
        let key = request.run_key;
        let run_hash_home = crate::shard::shard_for(key, 8);
        assert_ne!(home, run_hash_home);
        let runtime = timeout_runtime(repo.clone(), home);
        let (committing, resume) = repo.pause(Point::Commit);
        let lane = runtime.lanes[0].clone();
        let command = match variant {
            0 => Command::Start(request.clone()),
            1 => Command::StartAndUpdate(StartAndUpdateRequest {
                start: request.clone(),
                update_id: "folded".into(),
            }),
            _ => Command::SignalWithStart(signal_start(request.clone())),
        };
        let caller = tokio::spawn(async move { lane.submit(key, command).await });
        committing.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        resume.send(()).unwrap();
        runtime.lanes[0]
            .submit(
                key,
                Command::Signal(SignalRequest {
                    signal_name: "after-start".into(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId("after-start".into()),
                        ..request.request.clone()
                    },
                    now: request.now,
                }),
            )
            .await
            .unwrap();
        let before = runtime.workflow_timeout_tracking.snapshot();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].run_key, key);
        assert_eq!(before[0].shard_id, home);
        assert_eq!(before[0].started_at, request.now);
        for _ in 0..2 {
            runtime
                .recover_self_assigned_shard(run_hash_home, ShardEpoch::ZERO)
                .await
                .unwrap();
            assert_eq!(runtime.workflow_timeout_tracking.snapshot(), before);
        }
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn dispatch_batch_uses_committed_home_without_loading_run() {
    let repo = Arc::new(Repo::default());
    *repo.fail.lock().unwrap() = Some(Point::Load);
    let (request, home) = timeout_start();
    let runtime = timeout_runtime(repo.clone(), home);
    let publisher = RuntimeDispatchPublisher::new(
        runtime.broker.clone(),
        runtime.activity_broker.clone(),
        repo.clone(),
        Arc::new(Mutex::new(runtime.lanes.clone())),
        runtime.lanes.len(),
        Arc::new(NoopNexusHttpClient),
        Arc::new(crate::nexus::NoopNexusCompletionClient),
        crate::nexus::NexusCompletionRuntimeConfig::default(),
        NexusEndpointRegistry::default(),
        crate::nexus::NexusTaskBroker::default(),
        runtime.nexus_timeout_tracking.clone(),
        runtime.completion_callback_tracking.clone(),
        runtime.activity_tracking.clone(),
        DeliveryMetrics::new(),
    );
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue,
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    };
    let ops: Vec<_> = (0..32)
        .map(|n| DispatchOp::EnqueueActivityTask {
            queue: queue.clone(),
            activity_id: format!("activity-{n}"),
            input: Payloads::default(),
            schedule_event_id: n + 1,
            attempt: 1,
            dispatch_revision: 0,
            stamp: 0,
            dispatch_at: OffsetDateTime::UNIX_EPOCH,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            priority: None,
        })
        .collect();
    crate::lane::DispatchPublisher::publish(&publisher, request.run_key, home, &ops)
        .await
        .unwrap();
    assert_eq!(repo.loads.load(Ordering::SeqCst), 0);
    let entries = runtime.activity_tracking.snapshot();
    assert_eq!(entries.len(), 32);
    assert!(entries.iter().all(|entry| entry.shard_id == home));
    for _ in 0..32 {
        let task = runtime
            .activity_broker
            .poll_activity_task(&queue, std::time::Duration::ZERO)
            .await
            .unwrap()
            .expect("one failed tracker read must not truncate the batch");
        assert_eq!(task.0.run_key, request.run_key);
    }
    stop(&runtime).await;
}

fn signal_start(start: StartRequest) -> SignalWithStartRequest {
    SignalWithStartRequest {
        run_key: start.run_key,
        advice_policy: start.advice_policy,
        namespace_id: start.namespace_id,
        workflow_id: start.workflow_id,
        run_id: start.run_id,
        workflow_type: start.workflow_type,
        task_queue: start.task_queue,
        input: start.input,
        memo: start.memo,
        search_attributes: start.search_attributes,
        workflow_execution_timeout: start.workflow_execution_timeout,
        workflow_run_timeout: start.workflow_run_timeout,
        workflow_task_timeout: start.workflow_task_timeout,
        retry_policy: start.retry_policy,
        conflict_policy: start.conflict_policy,
        reuse_policy: start.reuse_policy,
        header: start.header,
        deployment: start.deployment,
        build_id: start.build_id,
        versioning_override: start.versioning_override,
        workflow_start_delay: start.workflow_start_delay,
        user_metadata: start.user_metadata,
        links: start.links,
        priority: start.priority,
        initiator: start.initiator,
        cron_schedule: start.cron_schedule,
        attempt: start.attempt,
        continued_execution_run_id: start.continued_execution_run_id,
        first_execution_run_id: start.first_execution_run_id,
        parent_run_key: start.parent_run_key,
        parent_workflow_id: start.parent_workflow_id,
        parent_run_id: start.parent_run_id,
        parent_namespace_id: start.parent_namespace_id,
        parent_namespace_name: start.parent_namespace_name,
        parent_initiated_event_id: start.parent_initiated_event_id,
        root_workflow_id: start.root_workflow_id,
        root_run_id: start.root_run_id,
        original_execution_run_id: start.original_execution_run_id,
        continued_failure: start.continued_failure,
        last_completion_result: start.last_completion_result,
        first_run_started_at: start.first_run_started_at,
        request: start.request,
        now: start.now,
        client_cron_schedule: start.client_cron_schedule,
        signal_name: "signal-at-start".into(),
        signal_input: Payloads::default(),
    }
}

#[tokio::test]
async fn execution_home_admission_delivery_and_completion_use_the_same_epoch() {
    for repair in [false, true] {
        let repo = Arc::new(Repo {
            inner: InMemoryStore::with_shard_count(8),
            ..Default::default()
        });
        let (request, home) = timeout_start();
        let key = request.run_key;
        let other = crate::shard::shard_for(key, 8);
        let runtime = home_runtime(repo.clone(), home, repair);
        {
            let mut owner = runtime.shard_owner.write().unwrap();
            owner.record_acquired(home, ShardEpoch(17));
            owner.mark_active(home);
        }
        runtime.start_workflow(request.clone()).await.unwrap();
        assert_eq!(runtime.active_shards(), [home]);
        let task = runtime
            .poll_workflow_task(
                QueueKey {
                    namespace_id: request.namespace_id,
                    task_queue: request.task_queue.clone(),
                    task_kind: TaskKind::Workflow,
                    deployment: None,
                    build_id: None,
                },
                WorkerIdentity("home-worker".into()),
                std::time::Duration::ZERO,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.token.shard_epoch, ShardEpoch(17));
        {
            let mut owner = runtime.shard_owner.write().unwrap();
            owner.record_acquired(other, ShardEpoch(33));
            owner.mark_active(other);
        }
        let mut completion = WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("home-worker".into()),
            client_discards_speculative_with_events: false,
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![],
            command_sizes: vec![],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: vec![],
            request: RequestContext::unattributed(request.now),
            now: request.now,
        };
        completion.token.shard_epoch = ShardEpoch(33);
        assert!(
            runtime
                .complete_workflow_task(completion.clone())
                .await
                .unwrap_err()
                .is::<crate::errors::NotShardOwner>()
        );
        completion.token.shard_epoch = ShardEpoch(17);
        assert!(matches!(
            runtime.complete_workflow_task(completion).await.unwrap(),
            CommitResult::Applied { .. }
        ));
        stop(&runtime).await;

        let wrong = home_runtime(
            Arc::new(Repo {
                inner: InMemoryStore::with_shard_count(8),
                ..Default::default()
            }),
            other,
            repair,
        );
        let error = wrong.start_workflow(request).await.unwrap_err();
        assert!(error.is::<crate::errors::NotShardOwner>());
        stop(&wrong).await;
    }
}

#[tokio::test]
async fn execution_home_lookup_reuses_the_lane_load_for_cold_and_hot_commands() {
    let repo = Arc::new(Repo {
        inner: InMemoryStore::with_shard_count(8),
        ..Default::default()
    });
    let (request, home) = timeout_start();
    let key = request.run_key;
    let transition = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(request.clone()))
        .unwrap();
    repo.inner
        .commit_transition(key, transition, ShardEpoch::ZERO)
        .await
        .unwrap();
    let runtime = home_runtime(repo.clone(), home, false);
    for index in 0..2 {
        runtime
            .submit(
                key,
                Command::Signal(SignalRequest {
                    signal_name: "cache".into(),
                    input: Payloads::default(),
                    header: None,
                    links: vec![],
                    request: RequestContext {
                        request_id: RequestId(format!("cache-{index}")),
                        ..request.request.clone()
                    },
                    now: request.now,
                }),
            )
            .await
            .unwrap();
        assert_eq!(
            repo.loads.load(Ordering::SeqCst),
            1,
            "routing and execution share the normal cold load"
        );
    }
    stop(&runtime).await;
}

#[tokio::test]
async fn restored_store_requires_preparation_before_any_runtime_construction() {
    let store = InMemoryStore::default();
    let restored =
        Arc::new(InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap());
    assert!(!restored.placement_ready());
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        TokeiraRuntime::new(
            restored.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        )
    }));
    assert!(
        refused.is_err(),
        "the assertion is before spawning or seeding any runtime work"
    );
    prepare_execution_placement(restored.as_ref())
        .await
        .unwrap();
    let runtime = TokeiraRuntime::new(
        restored,
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    runtime.runtime_shutdown.begin_shutdown();
    runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test]
async fn placement_preparation_retries_without_restart_and_backoff_is_cancellable() {
    let repo = Repo::default();
    repo.preparation_retries.store(2, Ordering::SeqCst);
    assert!(prepare_execution_placement(&repo).await.unwrap().complete());
    assert_eq!(repo.preparation_attempts.load(Ordering::SeqCst), 3);

    repo.preparation_retries.store(100, Ordering::SeqCst);
    let mut waiting = Box::pin(prepare_execution_placement(&repo));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(
            std::future::Future::poll(waiting.as_mut(), cx).is_pending()
        ))
        .await
    );
    assert_eq!(repo.preparation_attempts.load(Ordering::SeqCst), 4);
    drop(waiting);
    assert_eq!(repo.preparation_retries.load(Ordering::SeqCst), 99);
}
