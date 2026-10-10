//! Activity backlog retention and demand-driven delivery.
//!
//! Workflow delivery comes from durable dispatch discovery. These loops retain
//! only activity spill/drain behavior; neither broker is an authority for work.

use std::sync::Arc;

#[cfg(test)]
use time::OffsetDateTime;
use tokeira_kernel::RetiredField;
use tokeira_storage::{BacklogEntry, BacklogPayload, DispatchableActivityTask, RunRepository};
use tokio_util::sync::CancellationToken;

use crate::{DeliveryMetrics, FairnessState, InMemoryActivityBroker, InMemoryBroker};

/// Timing and batching knobs for the backlog reconciliation loops.
///
/// The activity grace window decides how long a ready copy remains in a broker before it
/// is demoted to durable storage; longer windows favor low-latency re-delivery
/// to a returning poller, shorter windows bound in-memory exposure to process
/// loss. `drain_batch_limit` caps how much is re-hydrated per queue per pass so
/// a deep backlog cannot starve other queues or flood a single drain tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BacklogConfig {
    pub activity_grace_window: tokio::time::Duration,
    pub grace_scan_interval: tokio::time::Duration,
    pub drain_interval: tokio::time::Duration,
    pub drain_batch_limit: usize,
}

impl Default for BacklogConfig {
    fn default() -> Self {
        Self {
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
    _broker: &InMemoryBroker,
    activity_broker: &InMemoryActivityBroker,
    repo: &R,
    config: &BacklogConfig,
) where
    R: RunRepository + ?Sized,
{
    let expired_activity = activity_broker
        .take_expired(config.activity_grace_window)
        .await;
    if expired_activity.is_empty() {
        return;
    }
    let entries = expired_activity
        .iter()
        .map(activity_to_backlog_entry)
        .collect();

    if let Err(error) = repo.persist_to_backlog(entries).await {
        // The tasks were already removed from the brokers above. Until storage
        // acknowledges them, retain the volatile copies to avoid delaying
        // delivery until the independent activity-dispatch reconciliation pass. The spill writes the entries
        // in order, in transactions of their own, so the first `persisted` are
        // stored and stay out of the brokers (`runtime-durable-backlog` criterion 3.7).
        tracing::warn!(
            error = ?error.source,
            persisted = error.persisted,
            "failed to persist expired live-ready tasks to backlog"
        );
        for task in expired_activity.into_iter().skip(error.persisted) {
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
    _broker: &InMemoryBroker,
    activity_broker: &InMemoryActivityBroker,
    repo: &R,
    config: &BacklogConfig,
    _fairness: &FairnessState,
    metrics: &DeliveryMetrics,
) where
    R: RunRepository + ?Sized,
{
    for queue in activity_broker.queues_with_waiters().await {
        match repo.drain_backlog(&queue, config.drain_batch_limit).await {
            Ok(entries) => {
                for entry in entries {
                    match entry.payload {
                        BacklogPayload::Activity {
                            activity_id,
                            retired_input: _,
                            schedule_event_id,
                            attempt,
                            dispatch_revision,
                            stamp,
                        } => {
                            if let Err(error) = activity_broker
                                .publish_activity_task(
                                    DispatchableActivityTask {
                                        run_key: entry.run_key,
                                        queue: entry.queue,
                                        activity_id,
                                        schedule_event_id,
                                        attempt,
                                        dispatch_revision,
                                        stamp,
                                        priority: entry.priority,
                                        order: Some(entry.order),
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

fn activity_to_backlog_entry(task: &crate::broker::TimestampedActivityTask) -> BacklogEntry {
    BacklogEntry {
        run_key: task.task.run_key,
        queue: task.task.queue.clone(),
        payload: BacklogPayload::Activity {
            activity_id: task.task.activity_id.clone(),
            retired_input: RetiredField::default(),
            schedule_event_id: task.task.schedule_event_id,
            attempt: task.task.attempt,
            dispatch_revision: task.task.dispatch_revision,
            stamp: task.task.stamp,
        },
        priority: task.task.priority.clone(),
        scheduled_at: task.scheduled_at,
        order: task
            .task
            .order
            .expect("broker publication always assigns activity delivery order"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::Mutex,
    };
    use tokeira_storage::WriteFence;

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use proptest::prelude::*;
    use tokeira_kernel::{LoadedRun, Transition};
    use tokeira_storage::{
        CommitResult, DeliveryOrder, DispatchableWorkflowTask, DueTimer, RequestRecord,
        RunRepository, TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, LogicalTaskSeq, NamespaceId, QueueKey, RequestId, RunKey, ShardId, TaskKind,
        TaskQueueName,
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
        async fn list_runs_for_namespace(&self, _namespace_id: NamespaceId) -> Result<Vec<RunKey>> {
            Ok(Vec::new())
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
            _fence: WriteFence,
        ) -> Result<CommitResult> {
            Err(anyhow!("unused"))
        }
        async fn commit_transition_for_bundle(
            &self,
            _run_key: RunKey,
            _execution_home_bundle: ShardId,
            _transition: Transition,
            _fence: WriteFence,
        ) -> Result<CommitResult> {
            Err(anyhow!("unused"))
        }
        async fn delete_run_for_bundle(
            &self,
            _run_key: RunKey,
            _execution_home_bundle: ShardId,
            _request: tokeira_storage::DeleteRunRequest,
            _fence: WriteFence,
        ) -> Result<tokeira_storage::DeleteRunResult> {
            Err(anyhow!("unused"))
        }
        async fn materialize_reset_successor(
            &self,
            _base_run_key: RunKey,
            _fork_event_id: i64,
            _successor_run_id: tokeira_types::RunId,
            _expected_current: Option<RunKey>,
            _fence: WriteFence,
        ) -> Result<()> {
            Err(anyhow!("unused"))
        }

        async fn abandon_materialization(
            &self,
            _run_key: RunKey,
            _fence: WriteFence,
        ) -> Result<()> {
            Ok(())
        }

        async fn purge_run(&self, _run_key: RunKey) -> Result<()> {
            Ok(())
        }

        async fn list_run_bulk_writes(
            &self,
            _shard_id: tokeira_types::ShardId,
            _after: Option<RunKey>,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::RunBulkWrite>> {
            Ok(Vec::new())
        }
        async fn list_workflow_dispatch_page(
            &self,
            _range: &tokeira_storage::WorkflowDiscoveryRange,
            _after: Option<tokeira_storage::WorkflowDispatchPosition>,
            _limit: std::num::NonZeroU32,
        ) -> Result<tokeira_storage::WorkflowDispatchPage> {
            anyhow::bail!("workflow discovery is outside this test repository")
        }

        async fn list_workflow_dispatch_for_home(
            &self,
            _home: tokeira_types::ShardId,
            _after: Option<RunKey>,
            _limit: std::num::NonZeroU32,
        ) -> Result<Vec<RunKey>> {
            anyhow::bail!("workflow acquisition is outside this test repository")
        }

        async fn list_dispatchable_workflow_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            Ok(Vec::new())
        }
        async fn list_due_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _now: time::OffsetDateTime,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }
        async fn list_all_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }
        async fn delete_activity_dispatch_if_matches(
            &self,
            _candidate: &tokeira_storage::ActivityDispatchIdentity,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn persist_to_backlog(
            &self,
            entries: Vec<BacklogEntry>,
        ) -> std::result::Result<(), tokeira_storage::BacklogPersistError> {
            if *self
                .fail_persist
                .lock()
                .expect("fail_persist lock poisoned")
            {
                return Err(tokeira_storage::BacklogPersistError {
                    persisted: 0,
                    source: anyhow!("persist failed"),
                });
            }
            self.persisted
                .lock()
                .expect("persisted lock poisoned")
                .extend(entries);
            Ok(())
        }
        async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
            self.drain_calls
                .lock()
                .expect("drain_calls lock poisoned")
                .push(queue.clone());
            let mut drained = Vec::new();
            let mut guard = self.drained.lock().expect("drained lock poisoned");
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
        async fn list_recovery_candidates_for_shard(
            &self,
            _shard_id: ShardId,
            _cursor: Option<&tokeira_storage::RecoveryCursor>,
            _limit: usize,
        ) -> Result<tokeira_storage::RecoveryPage> {
            Ok(tokeira_storage::RecoveryPage {
                states: Vec::new(),
                next: None,
            })
        }
        async fn list_due_dispatchable_activity_tasks_for_shard(
            &self,
            _shard_id: ShardId,
            _now: time::OffsetDateTime,
            _after: Option<&tokeira_storage::DueActivityDispatch>,
            _limit: usize,
        ) -> Result<Vec<tokeira_storage::DueActivityDispatch>> {
            Ok(Vec::new())
        }
        async fn list_due_timers_for_shard(
            &self,
            _shard_id: ShardId,
            _now: time::OffsetDateTime,
            _after: Option<&DueTimer>,
            _limit: usize,
        ) -> Result<Vec<DueTimer>> {
            Ok(Vec::new())
        }
        async fn delete_due_timer_if_matches(
            &self,
            _timer: &DueTimer,
            _reason: tokeira_storage::StaleTimer,
        ) -> Result<bool> {
            Ok(false)
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

    // A spill that fails at its second page keeps its first page stored and
    // re-publishes only the tasks after it, so no task is both stored and in a
    // broker (`bounded-bulk-writes` criterion 2.2).
    #[tokio::test]
    async fn a_failed_spill_re_publishes_only_the_tasks_it_did_not_store() {
        let repo = tokeira_storage::InMemoryStore::default();
        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let queue = activity_queue();
        for index in 0..1_500 {
            activity_broker
                .publish_activity_task(
                    tokeira_storage::DispatchableActivityTask {
                        run_key: RunKey::new(),
                        queue: queue.clone(),
                        activity_id: format!("activity-{index}"),
                        schedule_event_id: 5,
                        attempt: 1,
                        dispatch_revision: 0,
                        stamp: 0,
                        priority: None,
                        order: None,
                    },
                    None,
                )
                .await
                .unwrap();
        }
        // The first page, of 1,000 entries, commits, and the second fails.
        repo.fail_bulk_write_after(1).await;
        let config = BacklogConfig {
            activity_grace_window: std::time::Duration::ZERO,
            ..BacklogConfig::default()
        };
        scan_grace_once(&broker, &activity_broker, &repo, &config).await;

        let stored = repo
            .drain_backlog(&queue, 2_000)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| match entry.payload {
                BacklogPayload::Activity { activity_id, .. } => activity_id,
                BacklogPayload::Workflow { .. } => unreachable!("only activities were spilled"),
            })
            .collect::<std::collections::HashSet<_>>();
        let republished = activity_broker
            .take_expired(std::time::Duration::ZERO)
            .await
            .into_iter()
            .map(|expired| expired.task.activity_id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(stored.len(), 1_000);
        assert_eq!(republished.len(), 500);
        assert!(stored.is_disjoint(&republished));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        // Feature: workflow-dispatch, Property 10: Workflow-only backlog retirement
        // Neither workflow spill nor drain runs; activity payload and ordering survive both.
        #[test]
        fn workflow_backlog_is_untouched_by_activity_loops(seq in 1u64..100, attempt in 1u32..5, priority in 1i16..=5, fair_pass in 0i64..1000) {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let repo = MockBacklogRepo::default();
                let broker = InMemoryBroker::default();
                let activities = InMemoryActivityBroker::default();
                let workflow = workflow_queue();
                let activity = activity_queue();
                let run_key = RunKey::new();
                let wf = DispatchableWorkflowTask {
                    run_key, logical_seq: LogicalTaskSeq(seq), queue: workflow.clone(), sticky_preferred: None,
                    normal_queue: None, sticky_deadline: None, priority: None, order: None,
                };
                broker.publish_workflow_task(wf.clone(), None).await;
                let order = DeliveryOrder { priority_key: priority, fair_pass, insertion_tie: 17 };
                let task = DispatchableActivityTask {
                    run_key, queue: activity.clone(), activity_id: "activity".into(),
                    schedule_event_id: 42, attempt, dispatch_revision: 7, stamp: 9, priority: None, order: Some(order),
                };
                activities.publish_activity_task(task.clone(), None).await.unwrap();
                scan_grace_once(&broker, &activities, &repo, &BacklogConfig { activity_grace_window: std::time::Duration::ZERO, ..Default::default() }).await;
                let stored = repo.persisted.lock().unwrap().clone();
                assert_eq!(stored.len(), 1);
                assert_eq!(stored[0].queue, activity);
                assert_eq!(stored[0].order, order);
                assert!(broker.try_claim_workflow_task(&workflow, run_key).await.is_some());
                repo.drained.lock().unwrap().insert(activity.clone(), stored.into());
                repo.drained.lock().unwrap().insert(workflow.clone(), VecDeque::from([BacklogEntry {
                    run_key, queue: workflow.clone(), payload: BacklogPayload::Workflow { logical_seq: LogicalTaskSeq(seq) },
                    priority: None, scheduled_at: OffsetDateTime::now_utc(), order,
                }]));
                let waiter = {
                    let broker = broker.clone(); let queue = workflow.clone();
                    tokio::spawn(async move { broker.poll_workflow_task(&queue, &tokeira_types::WorkerIdentity("worker".into()), std::time::Duration::from_secs(300)).await })
                };
                let activity_waiter = {
                    let broker = activities.clone(); let queue = activity.clone();
                    tokio::spawn(async move { broker.poll_activity_task(&queue, std::time::Duration::from_secs(300)).await.unwrap().unwrap().0 })
                };
                while broker.queues_with_waiters().await.is_empty() || activities.queues_with_waiters().await.is_empty() { tokio::task::yield_now().await; }
                drain_once(&broker, &activities, &repo, &BacklogConfig::default(), &FairnessState::new(), &DeliveryMetrics::new()).await;
                let delivered = activity_waiter.await.unwrap();
                assert_eq!(delivered, task);
                assert_eq!(*repo.drain_calls.lock().unwrap(), vec![activity]);
                assert_eq!(repo.drained.lock().unwrap()[&workflow].len(), 1);
                assert!(!waiter.is_finished());
                waiter.abort();
                let _ = waiter.await;
            });
        }
    }
}
