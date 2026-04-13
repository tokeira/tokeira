use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::sync::{RwLock, watch};
use tokeira_kernel::{HistoryEvent, LoadedRun, Transition};
use tokeira_storage::{
    ActivitySweepEntry, BacklogEntry, CommitResult, DispatchableActivityTask,
    DispatchableWorkflowTask, DueTimer, LeaseOutcome, LeaseRepository,
    NexusSweepEntry, RequestRecord, RunRepository, TransitionAuditRecord,
    WorkflowTimeoutSweepEntry,
};
use tokeira_types::{
    ExecutionRef, QueueKey, RequestId, RunId, RunKey, ShardEpoch, ShardId,
};

#[derive(Clone, Default)]
pub struct HistoryWaitRegistry {
    inner: Arc<RwLock<HashMap<RunKey, watch::Sender<i64>>>>,
}

impl std::fmt::Debug for HistoryWaitRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryWaitRegistry")
            .finish_non_exhaustive()
    }
}

impl HistoryWaitRegistry {
    pub async fn receiver(
        &self,
        run_key: RunKey,
        current_last_event_id: i64,
    ) -> watch::Receiver<i64> {
        let mut waiters = self.inner.write().await;
        let sender = waiters.entry(run_key).or_insert_with(|| {
            let (sender, _receiver) = watch::channel(current_last_event_id);
            sender
        });
        if *sender.borrow() < current_last_event_id {
            let _ = sender.send(current_last_event_id);
        }
        sender.subscribe()
    }

    pub async fn notify(&self, run_key: RunKey, last_event_id: i64) {
        let waiters = self.inner.read().await;
        if let Some(sender) = waiters.get(&run_key) {
            let _ = sender.send(last_event_id);
        }
    }
}

#[derive(Clone, Debug)]
pub struct HistoryNotifyingRepository<R> {
    inner: Arc<R>,
    waits: HistoryWaitRegistry,
}

impl<R> HistoryNotifyingRepository<R> {
    pub fn new(inner: Arc<R>, waits: HistoryWaitRegistry) -> Self {
        Self { inner, waits }
    }
}

#[async_trait]
impl<R> RunRepository for HistoryNotifyingRepository<R>
where
    R: RunRepository + Send + Sync + 'static,
{
    async fn resolve_execution(
        &self,
        execution: &ExecutionRef,
    ) -> Result<Option<RunKey>> {
        self.inner.resolve_execution(execution).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        self.inner.load_run(run_key).await
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        self.inner.read_history(run_key, after_event_id, limit).await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        self.inner.lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(
        &self,
        run_key: RunKey,
    ) -> Result<Vec<TransitionAuditRecord>> {
        self.inner.read_transition_audit(run_key).await
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        let last_event_id = transition
            .history_events
            .last()
            .map(|event| event.event_id);
        let result = self
            .inner
            .commit_transition(run_key, transition, epoch)
            .await?;
        if matches!(result, CommitResult::Applied { .. })
            && let Some(last_event_id) = last_event_id
        {
            self.waits.notify(run_key, last_event_id).await;
        }
        Ok(result)
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_key: RunKey,
        successor_run_id: RunId,
    ) -> Result<()> {
        self.inner
            .materialize_reset_successor(
                base_run_key,
                fork_event_id,
                successor_run_key,
                successor_run_id,
            )
            .await?;
        self.waits.notify(successor_run_key, fork_event_id).await;
        Ok(())
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

    async fn list_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.inner
            .list_dispatchable_activity_tasks(queue, limit)
            .await
    }

    async fn persist_to_backlog(&self, entries: Vec<BacklogEntry>) -> Result<()> {
        self.inner.persist_to_backlog(entries).await
    }

    async fn drain_backlog(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<BacklogEntry>> {
        self.inner.drain_backlog(queue, limit).await
    }

    async fn list_due_timers(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        self.inner.list_due_timers(now, limit).await
    }

    async fn list_dispatchable_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        self.inner
            .list_dispatchable_workflow_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.inner
            .list_dispatchable_activity_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        self.inner
            .list_due_timers_for_shard(shard_id, now, limit)
            .await
    }

    async fn list_runs_with_workflow_timeouts_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WorkflowTimeoutSweepEntry>> {
        self.inner
            .list_runs_with_workflow_timeouts_for_shard(shard_id, limit)
            .await
    }

    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        self.inner.list_open_activities_for_shard(shard_id, limit).await
    }

    async fn list_pending_nexus_operations_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<NexusSweepEntry>> {
        self.inner
            .list_pending_nexus_operations_for_shard(shard_id, limit)
            .await
    }
}

#[async_trait]
impl<R> LeaseRepository for HistoryNotifyingRepository<R>
where
    R: LeaseRepository + Send + Sync + 'static,
{
    async fn try_acquire_bundle(
        &self,
        bundle: ShardId,
        owner: String,
    ) -> Result<LeaseOutcome> {
        self.inner.try_acquire_bundle(bundle, owner).await
    }

    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        self.inner.renew_bundle(bundle, owner, epoch).await
    }
}
