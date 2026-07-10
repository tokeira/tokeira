//! Durable backlog: the bridge between the in-memory brokers and storage.
//!
//! The brokers ([`InMemoryBroker`], [`InMemoryActivityBroker`]) are a derived
//! delivery optimization; the durable backlog in storage is authoritative. This
//! module owns the two background loops that keep those two views reconciled
//! without making an idle system expensive:
//!
//! - The **grace scanner** (`scan_grace_once`) demotes tasks that have sat
//!   unclaimed in a broker past their grace window into the durable backlog.
//!   This caps how long delivery state lives only in memory: if no poller takes
//!   a task promptly, it must not pin memory or be lost on process death, so it
//!   moves to storage where it can be re-delivered later.
//! - The **drain loop** (`drain_once`) re-hydrates the brokers from the
//!   durable backlog, but only for queues that currently have waiting pollers.
//!   Draining is demand-driven precisely so inactivity stays cheap — a backlog
//!   with no listeners is never read.
//!
//! Because the durable backlog is the source of truth, a failed persist during
//! grace scanning is recovered by re-publishing the tasks to the broker rather
//! than dropping them: until storage accepts them, the in-memory copy is the
//! only record that the work exists.

use std::sync::Arc;

use time::OffsetDateTime;
use tokeira_storage::{
    BacklogEntry, BacklogPayload, DispatchableActivityTask, DispatchableWorkflowTask, RunRepository,
};
use tokio_util::sync::CancellationToken;

use crate::{DeliveryMetrics, FairnessState, InMemoryActivityBroker, InMemoryBroker};

/// Timing and batching knobs for the backlog reconciliation loops.
///
/// The grace windows decide how long a task may live only in a broker before it
/// is demoted to durable storage; longer windows favor low-latency re-delivery
/// to a returning poller, shorter windows bound in-memory exposure to process
/// loss. `drain_batch_limit` caps how much is re-hydrated per queue per pass so
/// a deep backlog cannot starve other queues or flood a single drain tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BacklogConfig {
    pub workflow_grace_window: tokio::time::Duration,
    pub activity_grace_window: tokio::time::Duration,
    pub grace_scan_interval: tokio::time::Duration,
    pub drain_interval: tokio::time::Duration,
    pub drain_batch_limit: usize,
}

impl Default for BacklogConfig {
    fn default() -> Self {
        Self {
            workflow_grace_window: tokio::time::Duration::from_secs(5),
            activity_grace_window: tokio::time::Duration::from_secs(5),
            grace_scan_interval: tokio::time::Duration::from_secs(1),
            drain_interval: tokio::time::Duration::from_secs(2),
            drain_batch_limit: 100,
        }
    }
}

/// Demote broker tasks that have outlived their grace window into the durable
/// backlog, in one pass.
///
/// A task only reaches here if it sat in a broker, unclaimed, longer than its
/// grace window — the signal that no poller is currently interested. Moving it
/// to storage frees the in-memory copy and ensures the work survives process
/// loss. The drain loop will bring it back when a poller reappears.
pub(crate) async fn scan_grace_once<R>(
    broker: &InMemoryBroker,
    activity_broker: &InMemoryActivityBroker,
    repo: &R,
    config: &BacklogConfig,
) where
    R: RunRepository + ?Sized,
{
    let expired_workflow = broker.take_expired(config.workflow_grace_window).await;
    let expired_activity = activity_broker
        .take_expired(config.activity_grace_window)
        .await;

    if expired_workflow.is_empty() && expired_activity.is_empty() {
        return;
    }
    tracing::debug!(
        workflow_tasks = expired_workflow.len(),
        activity_tasks = expired_activity.len(),
        "demoting grace-expired live-ready tasks to durable backlog"
    );

    let mut entries = Vec::with_capacity(expired_workflow.len() + expired_activity.len());
    entries.extend(expired_workflow.iter().map(workflow_to_backlog_entry));
    entries.extend(expired_activity.iter().map(activity_to_backlog_entry));

    if let Err(error) = repo.persist_to_backlog(entries).await {
        // The tasks were already removed from the brokers above. Until storage
        // acknowledges them, the in-memory copy is the only record they exist,
        // so re-publish rather than drop — losing them here would silently lose
        // work the durable backlog never received.
        tracing::warn!(
            ?error,
            "failed to persist expired live-ready tasks to backlog"
        );
        for task in expired_workflow {
            broker.publish_workflow_task(task.task, None).await;
        }
        for task in expired_activity {
            if let Err(republish_error) =
                activity_broker.publish_activity_task(task.task, None).await
            {
                tracing::warn!(
                    ?republish_error,
                    "failed to re-publish expired activity task after backlog persist failure"
                );
            }
        }
    }
}

pub(crate) async fn run_grace_scanner<R>(
    broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
    repo: Arc<R>,
    config: BacklogConfig,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.grace_scan_interval) => {}
        }

        scan_grace_once(&broker, &activity_broker, &*repo, &config).await;
    }
}

/// Re-hydrate the brokers from the durable backlog, in one pass.
///
/// Draining is demand-driven: only queues with waiting pollers are read, so an
/// idle backlog with no listeners costs nothing to keep. Per queue, the amount
/// pulled is the lesser of the fairness budget and `drain_batch_limit` — the
/// budget is what prevents one hot queue from monopolising re-delivery and
/// starving others.
pub(crate) async fn drain_once<R>(
    broker: &InMemoryBroker,
    activity_broker: &InMemoryActivityBroker,
    repo: &R,
    config: &BacklogConfig,
    fairness: &FairnessState,
    metrics: &DeliveryMetrics,
) where
    R: RunRepository + ?Sized,
{
    let waiting_queues = broker.workflow_waiter_counts().await;
    if !waiting_queues.is_empty() {
        tracing::trace!(
            queue_count = waiting_queues.len(),
            "drain tick: queues with parked workflow pollers"
        );
    }
    for (queue, parked_pollers) in waiting_queues {
        // The fairness budget is seeded from COMPLETED polls (`share ×
        // recent_poll_count`), but a parked long-poll only counts once it
        // completes — so a queue whose first poller arrived after its tasks
        // aged into the backlog would re-seed to 0 forever: no drain → the
        // poll never completes → no polls in the window → budget 0. Each
        // PARKED poller is one unit of live, unmet demand, so it floors the
        // effective budget; a hot queue's throughput is still capped by its
        // fairness allotment.
        let budget = fairness.remaining_budget(&queue).max(parked_pollers);
        let limit = (budget as usize).min(config.drain_batch_limit);
        match repo.drain_backlog(&queue, limit).await {
            Ok(entries) => {
                let mut max_age = std::time::Duration::ZERO;
                let drained_count = entries.len();
                for entry in entries {
                    let age = (OffsetDateTime::now_utc() - entry.scheduled_at)
                        .try_into()
                        .unwrap_or(std::time::Duration::ZERO);
                    max_age = max_age.max(age);
                    match entry.payload {
                        BacklogPayload::Workflow { logical_seq } => {
                            // Re-delivered tasks drop their sticky hint: the
                            // original worker affinity is stale by the time work
                            // has aged into the backlog, so let any poller take
                            // it rather than wait again for a specific worker.
                            broker
                                .publish_workflow_task(
                                    DispatchableWorkflowTask {
                                        run_key: entry.run_key,
                                        queue: entry.queue,
                                        logical_seq,
                                        sticky_preferred: None,
                                        sticky_expires_at: None,
                                    },
                                    Some(metrics),
                                )
                                .await;
                        }
                        BacklogPayload::Activity { .. } => {
                            tracing::warn!(
                                ?queue,
                                run_key = ?entry.run_key,
                                "unexpected activity payload in workflow backlog drain"
                            );
                        }
                    }
                }
                if drained_count > 0 {
                    tracing::debug!(
                        ?queue,
                        drained_count,
                        "re-hydrated workflow tasks from durable backlog"
                    );
                    fairness.consume_budget(&queue, drained_count as u32);
                    metrics.set_backlog_age(&queue, max_age);
                }
                // A short read means the backlog is exhausted for this queue;
                // reset the reported age to zero so fairness stops treating it
                // as a pressured, aging backlog.
                if drained_count < limit {
                    metrics.set_backlog_age(&queue, std::time::Duration::ZERO);
                }
            }
            Err(error) => {
                tracing::warn!(?error, ?queue, "failed to drain workflow backlog");
            }
        }
    }

    for queue in activity_broker.queues_with_waiters().await {
        match repo.drain_backlog(&queue, config.drain_batch_limit).await {
            Ok(entries) => {
                for entry in entries {
                    match entry.payload {
                        BacklogPayload::Activity {
                            activity_id,
                            input,
                            schedule_event_id,
                            attempt,
                            dispatch_revision,
                        } => {
                            if let Err(error) = activity_broker
                                .publish_activity_task(
                                    DispatchableActivityTask {
                                        run_key: entry.run_key,
                                        queue: entry.queue,
                                        activity_id,
                                        input,
                                        schedule_event_id,
                                        attempt,
                                        dispatch_revision,
                                    },
                                    Some(metrics),
                                )
                                .await
                            {
                                tracing::warn!(
                                    ?error,
                                    "failed to re-publish drained activity backlog task"
                                );
                            }
                        }
                        BacklogPayload::Workflow { .. } => {
                            tracing::warn!(
                                ?queue,
                                run_key = ?entry.run_key,
                                "unexpected workflow payload in activity backlog drain"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                tracing::warn!(?error, ?queue, "failed to drain activity backlog");
            }
        }
    }
}

pub(crate) async fn run_drain_loop<R>(
    broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
    repo: Arc<R>,
    config: BacklogConfig,
    fairness: FairnessState,
    metrics: DeliveryMetrics,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.drain_interval) => {}
        }

        drain_once(
            &broker,
            &activity_broker,
            &*repo,
            &config,
            &fairness,
            &metrics,
        )
        .await;
    }
}

fn workflow_to_backlog_entry(task: &crate::broker::TimestampedWorkflowTask) -> BacklogEntry {
    BacklogEntry {
        run_key: task.task.run_key,
        queue: task.task.queue.clone(),
        payload: BacklogPayload::Workflow {
            logical_seq: task.task.logical_seq,
        },
        scheduled_at: task.scheduled_at,
        // Placeholder: storage assigns the authoritative monotonic insertion
        // sequence on persist; the value supplied here is ignored.
        insertion_seq: 0,
    }
}

fn activity_to_backlog_entry(task: &crate::broker::TimestampedActivityTask) -> BacklogEntry {
    BacklogEntry {
        run_key: task.task.run_key,
        queue: task.task.queue.clone(),
        payload: BacklogPayload::Activity {
            activity_id: task.task.activity_id.clone(),
            input: task.task.input.clone(),
            schedule_event_id: task.task.schedule_event_id,
            attempt: task.task.attempt,
            dispatch_revision: task.task.dispatch_revision,
        },
        scheduled_at: task.scheduled_at,
        insertion_seq: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::Mutex,
    };

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use proptest::prelude::*;
    use tokeira_kernel::{LoadedRun, Transition};
    use tokeira_storage::{
        ActivitySweepEntry, CommitResult, DispatchableWorkflowTask, DueTimer, NexusSweepEntry,
        RequestRecord, RunRepository, TransitionAuditRecord, WorkflowTimeoutSweepEntry,
    };
    use tokeira_types::{
        ExecutionRef, LogicalTaskSeq, NamespaceId, QueueKey, RequestId, RunKey, ShardEpoch,
        ShardId, TaskKind, TaskQueueName,
    };

    use super::*;

    #[derive(Default)]
    struct MockBacklogRepo {
        persisted: Mutex<Vec<BacklogEntry>>,
        drained: Mutex<HashMap<QueueKey, VecDeque<BacklogEntry>>>,
        drain_calls: Mutex<Vec<QueueKey>>,
        fail_persist: Mutex<bool>,
    }

    #[async_trait]
    impl RunRepository for MockBacklogRepo {
        async fn resolve_execution(&self, _execution: &ExecutionRef) -> Result<Option<RunKey>> {
            Ok(None)
        }
        async fn find_latest_run(
            &self,
            _namespace_id: NamespaceId,
            _workflow_id: &tokeira_types::WorkflowId,
        ) -> Result<Option<RunKey>> {
            Ok(None)
        }
        async fn load_run(&self, _run_key: RunKey) -> Result<LoadedRun> {
            Ok(LoadedRun::Absent)
        }
        async fn read_history(
            &self,
            _run_key: RunKey,
            _after_event_id: i64,
            _limit: usize,
        ) -> Result<Vec<tokeira_kernel::HistoryEvent>> {
            Ok(Vec::new())
        }
        async fn lookup_request_dedupe(
            &self,
            _execution: &ExecutionRef,
            _request_id: &RequestId,
        ) -> Result<Option<RequestRecord>> {
            Ok(None)
        }
        async fn read_transition_audit(
            &self,
            _run_key: RunKey,
        ) -> Result<Vec<TransitionAuditRecord>> {
            Ok(Vec::new())
        }
        // Test mock: backlog tests validate dispatch draining, not commit
        // fencing. These methods are unused stubs required by the trait.
        async fn commit_transition(
            &self,
            _run_key: RunKey,
            _transition: Transition,
            _epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            Err(anyhow!("unused"))
        }
        async fn commit_transition_for_bundle(
            &self,
            _run_key: RunKey,
            _execution_home_bundle: ShardId,
            _transition: Transition,
            _epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            Err(anyhow!("unused"))
        }
        async fn delete_run_for_bundle(
            &self,
            _run_key: RunKey,
            _execution_home_bundle: ShardId,
            _request: tokeira_storage::DeleteRunRequest,
            _epoch: ShardEpoch,
        ) -> Result<tokeira_storage::DeleteRunResult> {
            Err(anyhow!("unused"))
        }
        async fn materialize_reset_successor(
            &self,
            _base_run_key: RunKey,
            _fork_event_id: i64,
            _successor_run_id: tokeira_types::RunId,
        ) -> Result<()> {
            Err(anyhow!("unused"))
        }
        async fn list_dispatchable_workflow_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            Ok(Vec::new())
        }
        async fn list_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }
        async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
            if *self.fail_persist.lock().unwrap() {
                return Err(anyhow!("persist failed"));
            }
            self.persisted.lock().unwrap().extend(entries);
            Ok(())
        }
        async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
            self.drain_calls.lock().unwrap().push(queue.clone());
            let mut drained = Vec::new();
            let mut guard = self.drained.lock().unwrap();
            let backlog = guard.entry(queue.clone()).or_default();
            while drained.len() < limit {
                let Some(entry) = backlog.pop_front() else {
                    break;
                };
                drained.push(entry);
            }
            Ok(drained)
        }
        async fn list_due_timers(
            &self,
            _now: time::OffsetDateTime,
            _limit: usize,
        ) -> Result<Vec<DueTimer>> {
            Ok(Vec::new())
        }
        async fn list_dispatchable_workflow_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            Ok(Vec::new())
        }
        async fn list_dispatchable_activity_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }
        async fn list_due_timers_for_shard(
            &self,
            _shard_id: ShardId,
            _now: time::OffsetDateTime,
            _limit: usize,
        ) -> Result<Vec<DueTimer>> {
            Ok(Vec::new())
        }
        async fn list_runs_with_workflow_timeouts_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
            Ok(Vec::new())
        }
        async fn list_started_workflow_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::WftTimeoutSweepEntry>> {
            Ok(Vec::new())
        }
        async fn list_open_activities_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<ActivitySweepEntry>> {
            Ok(Vec::new())
        }
        async fn list_pending_nexus_operations_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<NexusSweepEntry>> {
            Ok(Vec::new())
        }

        async fn list_runs_with_pending_completion_callbacks_for_shard(
            &self,
            _shard_id: ShardId,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::CompletionCallbackSweepEntry>> {
            Ok(Vec::new())
        }
    }

    fn workflow_queue() -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("wq".into()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        }
    }

    fn activity_queue() -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("aq".into()),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        }
    }

    #[tokio::test]
    async fn grace_scan_persists_expired_tasks() {
        let repo = MockBacklogRepo::default();
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        broker
            .publish_workflow_task(
                DispatchableWorkflowTask {
                    run_key: RunKey::new(),
                    queue: workflow_queue(),
                    logical_seq: LogicalTaskSeq::ONE,
                    sticky_preferred: None,
                    sticky_expires_at: None,
                },
                None,
            )
            .await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let config = BacklogConfig {
            workflow_grace_window: std::time::Duration::from_millis(1),
            ..BacklogConfig::default()
        };
        scan_grace_once(&broker, &activity_broker, &repo, &config).await;

        let persisted = repo.persisted.lock().unwrap().clone();
        assert_eq!(persisted.len(), 1);
        assert!(matches!(
            persisted[0].payload,
            BacklogPayload::Workflow {
                logical_seq: LogicalTaskSeq::ONE
            }
        ));
    }

    #[tokio::test]
    async fn drain_targets_only_waiter_queues() {
        let repo = MockBacklogRepo::default();
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let fairness = FairnessState::new();
        let metrics = DeliveryMetrics::new();
        let queue = workflow_queue();
        repo.drained.lock().unwrap().insert(
            queue.clone(),
            VecDeque::from(vec![BacklogEntry {
                run_key: RunKey::new(),
                queue: queue.clone(),
                payload: BacklogPayload::Workflow {
                    logical_seq: LogicalTaskSeq::ONE,
                },
                scheduled_at: OffsetDateTime::now_utc(),
                insertion_seq: 0,
            }]),
        );

        let broker_clone = broker.clone();
        let queue_clone = queue.clone();
        let waiter = tokio::spawn(async move {
            broker_clone
                .poll_workflow_task(
                    &queue_clone,
                    &tokeira_types::WorkerIdentity("worker".into()),
                    std::time::Duration::from_millis(50),
                )
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;

        drain_once(
            &broker,
            &activity_broker,
            &repo,
            &BacklogConfig::default(),
            &fairness,
            &metrics,
        )
        .await;

        let calls = repo.drain_calls.lock().unwrap().clone();
        assert_eq!(calls, vec![queue.clone()]);
        let delivered = waiter.await.unwrap();
        assert!(delivered.is_some());
    }

    // The budget-livelock spine (TestGetWorkflowExecutionHistory_All): the
    // control loop re-seeds budget from COMPLETED polls, so a queue whose
    // first poller arrives after its task aged into the backlog sits at
    // budget 0 with the poll parked — and the parked poll can never complete
    // to earn budget. A parked poller must floor the effective drain budget.
    #[tokio::test]
    async fn drain_serves_parked_poller_despite_zero_fairness_budget() {
        let repo = MockBacklogRepo::default();
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let fairness = FairnessState::new();
        let metrics = DeliveryMetrics::new();
        let queue = workflow_queue();
        // Simulate the control tick that seeded this queue's budget from an
        // empty poll window (share × 0 completed polls = 0).
        fairness.apply_adjustment(
            std::collections::HashMap::from([(queue.clone(), 0.5)]),
            &std::collections::HashMap::new(),
            OffsetDateTime::now_utc(),
        );
        assert_eq!(fairness.remaining_budget(&queue), 0);
        repo.drained.lock().unwrap().insert(
            queue.clone(),
            VecDeque::from(vec![BacklogEntry {
                run_key: RunKey::new(),
                queue: queue.clone(),
                payload: BacklogPayload::Workflow {
                    logical_seq: LogicalTaskSeq::ONE,
                },
                scheduled_at: OffsetDateTime::now_utc(),
                insertion_seq: 0,
            }]),
        );

        let broker_clone = broker.clone();
        let queue_clone = queue.clone();
        let waiter = tokio::spawn(async move {
            broker_clone
                .poll_workflow_task(
                    &queue_clone,
                    &tokeira_types::WorkerIdentity("worker".into()),
                    std::time::Duration::from_millis(50),
                )
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;

        drain_once(
            &broker,
            &activity_broker,
            &repo,
            &BacklogConfig::default(),
            &fairness,
            &metrics,
        )
        .await;

        let delivered = waiter.await.unwrap();
        assert!(
            delivered.is_some(),
            "a parked poller is live demand and must not be starved by a zero budget"
        );
    }

    proptest! {
        #[test]
        fn property_grace_scanner_clears_dedup_keys(logical_seq in 1u64..8u64) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = MockBacklogRepo::default();
                let broker = InMemoryBroker::default();
                let activity_broker = InMemoryActivityBroker::default();
                let queue = workflow_queue();
                let task = DispatchableWorkflowTask {
                    run_key: RunKey::new(),
                    queue: queue.clone(),
                    logical_seq: LogicalTaskSeq(logical_seq),
                    sticky_preferred: None,
                    sticky_expires_at: None,
                };

                broker.publish_workflow_task(task.clone(), None).await;
                tokio::time::sleep(std::time::Duration::from_millis(3)).await;

                scan_grace_once(
                    &broker,
                    &activity_broker,
                    &repo,
                    &BacklogConfig {
                        workflow_grace_window: std::time::Duration::from_millis(1),
                        ..BacklogConfig::default()
                    },
                ).await;

                broker.publish_workflow_task(task.clone(), None).await;
                let delivered = broker
                    .poll_workflow_task(
                        &queue,
                        &tokeira_types::WorkerIdentity("worker".into()),
                        std::time::Duration::from_millis(5),
                    )
                    .await
                    .unwrap();

                prop_assert_eq!(delivered.and_then(|entry| entry.into_queued().map(|queued| queued.0)), Some(task));
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_persist_failure_retains_tasks_in_live_ready(logical_seq in 1u64..8u64) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = MockBacklogRepo::default();
                *repo.fail_persist.lock().unwrap() = true;
                let broker = InMemoryBroker::default();
                let activity_broker = InMemoryActivityBroker::default();
                let queue = workflow_queue();
                let task = DispatchableWorkflowTask {
                    run_key: RunKey::new(),
                    queue: queue.clone(),
                    logical_seq: LogicalTaskSeq(logical_seq),
                    sticky_preferred: None,
                    sticky_expires_at: None,
                };

                broker.publish_workflow_task(task.clone(), None).await;
                tokio::time::sleep(std::time::Duration::from_millis(3)).await;

                scan_grace_once(
                    &broker,
                    &activity_broker,
                    &repo,
                    &BacklogConfig {
                        workflow_grace_window: std::time::Duration::from_millis(1),
                        ..BacklogConfig::default()
                    },
                ).await;

                let delivered = broker
                    .poll_workflow_task(
                        &queue,
                        &tokeira_types::WorkerIdentity("worker".into()),
                        std::time::Duration::from_millis(5),
                    )
                    .await
                    .unwrap();

                prop_assert_eq!(delivered.and_then(|entry| entry.into_queued().map(|queued| queued.0)), Some(task));
                prop_assert!(repo.persisted.lock().unwrap().is_empty());
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_drain_routes_entries_to_the_correct_broker(logical_seq in 1u64..8u64, attempt in 1u32..4u32) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = MockBacklogRepo::default();
                let broker = InMemoryBroker::default();
                let activity_broker = InMemoryActivityBroker::default();
                let fairness = FairnessState::new();
                let metrics = DeliveryMetrics::new();
                let workflow_queue = workflow_queue();
                let activity_queue = activity_queue();

                repo.drained.lock().unwrap().insert(
                    workflow_queue.clone(),
                    VecDeque::from(vec![BacklogEntry {
                        run_key: RunKey::new(),
                        queue: workflow_queue.clone(),
                        payload: BacklogPayload::Workflow {
                            logical_seq: LogicalTaskSeq(logical_seq),
                        },
                        scheduled_at: OffsetDateTime::now_utc(),
                        insertion_seq: 0,
                    }]),
                );
                repo.drained.lock().unwrap().insert(
                    activity_queue.clone(),
                    VecDeque::from(vec![BacklogEntry {
                        run_key: RunKey::new(),
                        queue: activity_queue.clone(),
                        payload: BacklogPayload::Activity {
                            activity_id: "activity".into(),
                            input: tokeira_types::Payloads::default(),
                            schedule_event_id: 42,
                            attempt,
                            dispatch_revision: 0,
                        },
                        scheduled_at: OffsetDateTime::now_utc(),
                        insertion_seq: 1,
                    }]),
                );

                let workflow_waiter = {
                    let broker = broker.clone();
                    let queue = workflow_queue.clone();
                    tokio::spawn(async move {
                        broker
                            .poll_workflow_task(
                                &queue,
                                &tokeira_types::WorkerIdentity("worker".into()),
                                std::time::Duration::from_millis(50),
                            )
                            .await
                            .unwrap()
                    })
                };

                let activity_waiter = {
                    let activity_broker = activity_broker.clone();
                    let queue = activity_queue.clone();
                    tokio::spawn(async move {
                        activity_broker
                            .poll_activity_task(&queue, std::time::Duration::from_millis(50))
                            .await
                            .unwrap()
                    })
                };

                while !broker.queues_with_waiters().await.contains(&workflow_queue)
                    || !activity_broker
                        .queues_with_waiters()
                        .await
                        .contains(&activity_queue)
                {
                    tokio::task::yield_now().await;
                }

                drain_once(
                    &broker,
                    &activity_broker,
                    &repo,
                    &BacklogConfig::default(),
                    &fairness,
                    &metrics,
                ).await;

                let workflow = workflow_waiter.await.unwrap();
                let activity = activity_waiter.await.unwrap();
                let Some(workflow) = workflow else {
                    prop_assert!(false, "workflow backlog entry was not delivered");
                    return Ok::<(), proptest::test_runner::TestCaseError>(());
                };
                let Some(activity) = activity else {
                    prop_assert!(false, "activity backlog entry was not delivered");
                    return Ok::<(), proptest::test_runner::TestCaseError>(());
                };
                let Some((workflow, _)) = workflow.into_queued() else {
                    prop_assert!(false, "expected queued workflow task");
                    return Ok::<(), proptest::test_runner::TestCaseError>(());
                };
                prop_assert_eq!(workflow.logical_seq, LogicalTaskSeq(logical_seq));
                prop_assert_eq!(activity.0.activity_id, "activity");
                prop_assert_eq!(activity.0.attempt, attempt);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn property_fifo_order_preserved_through_drain(logical_seqs in proptest::collection::vec(1u64..32u64, 1..6)) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let repo = MockBacklogRepo::default();
                let broker = InMemoryBroker::default();
                let activity_broker = InMemoryActivityBroker::default();
                let fairness = FairnessState::new();
                let metrics = DeliveryMetrics::new();
                let queue = workflow_queue();

                let entries: Vec<_> = logical_seqs
                    .iter()
                    .enumerate()
                    .map(|(idx, seq)| BacklogEntry {
                        run_key: RunKey::new(),
                        queue: queue.clone(),
                        payload: BacklogPayload::Workflow {
                            logical_seq: LogicalTaskSeq(*seq),
                        },
                        scheduled_at: OffsetDateTime::now_utc(),
                        insertion_seq: idx as u64,
                    })
                    .collect();
                repo.drained
                    .lock()
                    .unwrap()
                    .insert(queue.clone(), VecDeque::from(entries));

                let broker_clone = broker.clone();
                let queue_clone = queue.clone();
                let first_waiter = tokio::spawn(async move {
                    broker_clone
                        .poll_workflow_task(
                            &queue_clone,
                            &tokeira_types::WorkerIdentity("worker".into()),
                            std::time::Duration::from_secs(5),
                        )
                        .await
                        .unwrap()
                });

                while !broker.queues_with_waiters().await.contains(&queue) {
                    tokio::task::yield_now().await;
                }
                drain_once(
                    &broker,
                    &activity_broker,
                    &repo,
                    &BacklogConfig::default(),
                    &fairness,
                    &metrics,
                ).await;

                let mut seen = Vec::new();
                let Some(task) = first_waiter.await.unwrap() else {
                    prop_assert!(false, "workflow backlog entry was not delivered");
                    return Ok::<(), proptest::test_runner::TestCaseError>(());
                };
                let Some((task, _)) = task.into_queued() else {
                    prop_assert!(false, "expected queued workflow task");
                    return Ok::<(), proptest::test_runner::TestCaseError>(());
                };
                seen.push(task.logical_seq.0);

                for _ in 1..logical_seqs.len() {
                    let Some(task) = broker
                        .poll_workflow_task(
                            &queue,
                            &tokeira_types::WorkerIdentity("worker".into()),
                            std::time::Duration::ZERO,
                        )
                        .await
                        .unwrap()
                    else {
                        prop_assert!(false, "workflow backlog entry was not delivered");
                        return Ok::<(), proptest::test_runner::TestCaseError>(());
                    };
                    let Some((task, _)) = task.into_queued() else {
                        prop_assert!(false, "expected queued workflow task");
                        return Ok::<(), proptest::test_runner::TestCaseError>(());
                    };
                    seen.push(task.logical_seq.0);
                }

                prop_assert_eq!(seen, logical_seqs);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 3: Grace scanner moves exactly expired ─
    // Feature: runtime-durable-backlog
    // Validates: Requirements 3.2, 3.3, 8.2
    #[tokio::test]
    async fn property_grace_scanner_moves_exactly_expired() {
        let repo = MockBacklogRepo::default();
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let queue = workflow_queue();

        // Publish 3 tasks. task_a is published first and
        // will be the oldest.
        let task_a = DispatchableWorkflowTask {
            run_key: RunKey::new(),
            queue: queue.clone(),
            logical_seq: LogicalTaskSeq(1),
            sticky_preferred: None,
            sticky_expires_at: None,
        };
        broker.publish_workflow_task(task_a.clone(), None).await;

        // Wait so task_a ages past the grace window.
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;

        // Publish task_b and task_c — they are fresh.
        let task_b = DispatchableWorkflowTask {
            run_key: RunKey::new(),
            queue: queue.clone(),
            logical_seq: LogicalTaskSeq(2),
            sticky_preferred: None,
            sticky_expires_at: None,
        };
        broker.publish_workflow_task(task_b.clone(), None).await;

        let task_c = DispatchableWorkflowTask {
            run_key: RunKey::new(),
            queue: queue.clone(),
            logical_seq: LogicalTaskSeq(3),
            sticky_preferred: None,
            sticky_expires_at: None,
        };
        broker.publish_workflow_task(task_c.clone(), None).await;

        // Grace window = 10ms. task_a (age ~15ms) exceeds
        // it. task_b and task_c (age ~0ms) do not.
        let config = BacklogConfig {
            workflow_grace_window: std::time::Duration::from_millis(10),
            ..BacklogConfig::default()
        };
        scan_grace_once(&broker, &activity_broker, &repo, &config).await;

        let persisted = repo.persisted.lock().unwrap().clone();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].run_key, task_a.run_key);

        // task_b and task_c should still be in live-ready.
        let delivered_b = broker
            .poll_workflow_task(
                &queue,
                &tokeira_types::WorkerIdentity("w".into()),
                std::time::Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert_eq!(
            delivered_b
                .as_ref()
                .and_then(|task| task.queued_task())
                .map(|task| task.run_key),
            Some(task_b.run_key)
        );

        let delivered_c = broker
            .poll_workflow_task(
                &queue,
                &tokeira_types::WorkerIdentity("w".into()),
                std::time::Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert_eq!(
            delivered_c
                .as_ref()
                .and_then(|task| task.queued_task())
                .map(|task| task.run_key),
            Some(task_c.run_key)
        );

        // No more tasks.
        let delivered_none = broker
            .poll_workflow_task(
                &queue,
                &tokeira_types::WorkerIdentity("w".into()),
                std::time::Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert_eq!(delivered_none, None);
    }
}
