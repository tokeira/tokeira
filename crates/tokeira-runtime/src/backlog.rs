use std::sync::Arc;

use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use tokeira_storage::{
    BacklogEntry, BacklogPayload, DispatchableActivityTask, DispatchableWorkflowTask,
    RunRepository,
};

use crate::{DeliveryMetrics, FairnessState, InMemoryActivityBroker, InMemoryBroker};

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

    let mut entries = Vec::with_capacity(expired_workflow.len() + expired_activity.len());
    entries.extend(expired_workflow.iter().map(workflow_to_backlog_entry));
    entries.extend(expired_activity.iter().map(activity_to_backlog_entry));

    if let Err(error) = repo.persist_to_backlog(entries).await {
        tracing::warn!(?error, "failed to persist expired live-ready tasks to backlog");
        for task in expired_workflow {
            broker.publish_workflow_task(task.task, None).await;
        }
        for task in expired_activity {
            if let Err(republish_error) =
                activity_broker.publish_activity_task(task.task, None).await
            {
                tracing::warn!(?republish_error, "failed to re-publish expired activity task after backlog persist failure");
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
    for queue in broker.queues_with_waiters().await {
        let budget = fairness.remaining_budget(&queue);
        if budget == 0 {
            continue;
        }
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
                            broker
                                .publish_workflow_task(DispatchableWorkflowTask {
                                    run_key: entry.run_key,
                                    queue: entry.queue,
                                    logical_seq,
                                    sticky_preferred: None,
                                    sticky_expires_at: None,
                                }, Some(metrics))
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
                    fairness.consume_budget(&queue, drained_count as u32);
                    metrics.set_backlog_age(&queue, max_age);
                }
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
                        } => {
                            if let Err(error) = activity_broker
                                .publish_activity_task(DispatchableActivityTask {
                                    run_key: entry.run_key,
                                    queue: entry.queue,
                                    activity_id,
                                    input,
                                    schedule_event_id,
                                    attempt,
                                }, Some(metrics))
                                .await
                            {
                                tracing::warn!(?error, "failed to re-publish drained activity backlog task");
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
        ActivitySweepEntry, CommitResult, DispatchableWorkflowTask, DueTimer,
        NexusSweepEntry, RequestRecord, RunRepository, TransitionAuditRecord,
        WorkflowTimeoutSweepEntry,
    };
    use tokeira_types::{
        ExecutionRef, LogicalTaskSeq, NamespaceId, QueueKey, RequestId, RunKey,
        ShardEpoch, ShardId, TaskKind, TaskQueueName,
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
        async fn commit_transition(
            &self,
            _run_key: RunKey,
            _transition: Transition,
            _epoch: ShardEpoch,
        ) -> Result<CommitResult> {
            Err(anyhow!("unused"))
        }
        async fn materialize_reset_successor(
            &self,
            _base_run_key: RunKey,
            _fork_event_id: i64,
            _successor_run_key: RunKey,
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
        async fn drain_backlog(
            &self,
            queue: &QueueKey,
            limit: usize,
        ) -> Result<Vec<BacklogEntry>> {
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
            .publish_workflow_task(DispatchableWorkflowTask {
                run_key: RunKey::new(),
                queue: workflow_queue(),
                logical_seq: LogicalTaskSeq::ONE,
                sticky_preferred: None,
                sticky_expires_at: None,
            }, None)
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

                prop_assert_eq!(delivered.map(|entry| entry.0), Some(task));
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

                prop_assert_eq!(delivered.map(|entry| entry.0), Some(task));
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
                            .poll_activity_task(
                                &queue,
                                std::time::Duration::from_millis(50),
                            )
                            .await
                            .unwrap()
                    })
                };

                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                drain_once(
                    &broker,
                    &activity_broker,
                    &repo,
                    &BacklogConfig::default(),
                    &fairness,
                    &metrics,
                ).await;

                let workflow = workflow_waiter.await.unwrap().unwrap();
                let activity = activity_waiter.await.unwrap().unwrap();
                prop_assert_eq!(workflow.0.logical_seq, LogicalTaskSeq(logical_seq));
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

                let expected = logical_seqs.clone();
                let broker_clone = broker.clone();
                let queue_clone = queue.clone();
                let waiter = tokio::spawn(async move {
                    let mut seen = Vec::new();
                    for _ in 0..expected.len() {
                        let task = broker_clone
                            .poll_workflow_task(
                                &queue_clone,
                                &tokeira_types::WorkerIdentity("worker".into()),
                                std::time::Duration::from_millis(50),
                            )
                            .await
                            .unwrap()
                            .unwrap();
                        seen.push(task.0.logical_seq.0);
                    }
                    seen
                });

                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                for _ in 0..logical_seqs.len() {
                    drain_once(
                        &broker,
                        &activity_broker,
                        &repo,
                        &BacklogConfig::default(),
                        &fairness,
                        &metrics,
                    ).await;
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }

                let seen = waiter.await.unwrap();
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
        let activity_broker =
            InMemoryActivityBroker::default();
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
        tokio::time::sleep(
            std::time::Duration::from_millis(15),
        )
        .await;

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
            workflow_grace_window:
                std::time::Duration::from_millis(10),
            ..BacklogConfig::default()
        };
        scan_grace_once(
            &broker,
            &activity_broker,
            &repo,
            &config,
        )
        .await;

        let persisted =
            repo.persisted.lock().unwrap().clone();
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
            delivered_b.as_ref().map(|t| t.0.run_key),
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
            delivered_c.as_ref().map(|t| t.0.run_key),
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
