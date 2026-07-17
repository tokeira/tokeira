//! Edge-local history wait primitives used by history long-poll APIs.
//!
//! History long-poll is a transport concern: callers want to block until a run
//! advances beyond the last event they have observed. The runtime should not
//! know about individual gRPC waiters, so the edge keeps a lightweight
//! notification registry keyed by `RunKey` and updates it from committed
//! history-appending transitions.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{HistoryEvent, LoadedRun, Transition};
use tokeira_storage::{
    ActivitySweepEntry, AttributedHistoryEvent, BacklogEntry, BundleLease, CommitResult,
    DeleteRunRequest, DeleteRunResult, DispatchableActivityTask, DispatchableWorkflowTask,
    DueTimer, LeaseOutcome, LeaseRepository, NexusSweepEntry, RequestRecord, RunRepository,
    TransitionAuditRecord, WftTimeoutSweepEntry, WorkflowRuleCreateResult,
    WorkflowRuleDeleteResult, WorkflowTimeoutSweepEntry,
};
use tokeira_types::{
    ExecutionRef, NamespaceId, QueueKey, RequestId, RunId, RunKey, ShardEpoch, ShardId, WorkflowId,
    WorkflowRuleRecord,
};
use tokio::sync::{RwLock, watch};

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
        // This wrapper adds wakeups only. Falling back to RunRepository's
        // legacy default would silently erase the durable principal sidecar
        // before public history serialization.
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
        let last_event_id = transition.history_events.last().map(|event| event.event_id);
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

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        epoch: ShardEpoch,
    ) -> Result<CommitResult> {
        let last_event_id = transition.history_events.last().map(|event| event.event_id);
        let result = self
            .inner
            .commit_transition_for_bundle(run_key, execution_home_bundle, transition, epoch)
            .await?;
        if matches!(result, CommitResult::Applied { .. })
            && let Some(last_event_id) = last_event_id
        {
            self.waits.notify(run_key, last_event_id).await;
        }
        Ok(result)
    }

    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        epoch: ShardEpoch,
    ) -> Result<DeleteRunResult> {
        let result = self
            .inner
            .delete_run_for_bundle(run_key, execution_home_bundle, request, epoch)
            .await?;
        if matches!(result, DeleteRunResult::Deleted { .. }) {
            // Wake long-polling history readers after the purge. Their next loop
            // iteration reloads the run and returns NOT_FOUND instead of waiting
            // for the ordinary 20-second long-poll expiry.
            self.waits.notify(run_key, i64::MAX).await;
        }
        Ok(result)
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
    ) -> Result<()> {
        self.inner
            .materialize_reset_successor(base_run_key, fork_event_id, successor_run_id)
            .await?;
        let loaded = self.inner.load_run(base_run_key).await?;
        if let LoadedRun::Existing(base_state) = loaded {
            let successor_run_key = RunKey::derive(
                base_state.namespace_id,
                &base_state.workflow_id,
                successor_run_id,
            );
            self.waits.notify(successor_run_key, fork_event_id).await;
        }
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

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        self.inner.drain_backlog(queue, limit).await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
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

    async fn list_started_workflow_tasks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<WftTimeoutSweepEntry>> {
        self.inner
            .list_started_workflow_tasks_for_shard(shard_id, limit)
            .await
    }

    async fn list_open_activities_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<ActivitySweepEntry>> {
        self.inner
            .list_open_activities_for_shard(shard_id, limit)
            .await
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

    async fn list_runs_with_pending_completion_callbacks_for_shard(
        &self,
        shard_id: ShardId,
        limit: usize,
    ) -> Result<Vec<tokeira_storage::CompletionCallbackSweepEntry>> {
        self.inner
            .list_runs_with_pending_completion_callbacks_for_shard(shard_id, limit)
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
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        self.inner
            .try_acquire_bundle(bundle, owner, node_endpoint)
            .await
    }

    async fn renew_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
        node_endpoint: String,
    ) -> Result<LeaseOutcome> {
        self.inner
            .renew_bundle(bundle, owner, epoch, node_endpoint)
            .await
    }

    async fn list_bundle_leases(&self) -> Result<Vec<BundleLease>> {
        self.inner.list_bundle_leases().await
    }

    async fn relinquish_bundle(
        &self,
        bundle: ShardId,
        owner: String,
        epoch: ShardEpoch,
    ) -> Result<LeaseOutcome> {
        self.inner.relinquish_bundle(bundle, owner, epoch).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_storage::InMemoryStore;
    use tokeira_types::{WorkflowRuleAction, WorkflowRuleTrigger};

    #[tokio::test]
    async fn workflow_rule_storage_delegates_through_history_notifier() {
        let inner = Arc::new(InMemoryStore::default());
        let repo = HistoryNotifyingRepository::new(inner, HistoryWaitRegistry::default());
        let namespace_id = NamespaceId::new();
        let rule = WorkflowRuleRecord {
            id: "pause".to_string(),
            create_time: OffsetDateTime::UNIX_EPOCH,
            created_by_identity: "operator".to_string(),
            description: "policy".to_string(),
            trigger: WorkflowRuleTrigger::ActivityStart {
                predicate: "ActivityType = 'type'".to_string(),
            },
            visibility_query: String::new(),
            actions: vec![WorkflowRuleAction::ActivityPause],
            expiration_time: None,
        };

        assert_eq!(
            repo.create_workflow_rule(namespace_id, rule.clone(), 10)
                .await
                .expect("create through wrapper"),
            WorkflowRuleCreateResult::Created,
        );
        assert_eq!(
            repo.get_workflow_rule(namespace_id, &rule.id)
                .await
                .expect("get through wrapper"),
            Some(rule.clone()),
        );
        assert_eq!(
            repo.list_workflow_rules(namespace_id)
                .await
                .expect("list through wrapper"),
            vec![rule],
        );
        assert_eq!(
            repo.delete_workflow_rule(namespace_id, "pause")
                .await
                .expect("delete through wrapper"),
            WorkflowRuleDeleteResult::Deleted,
        );
    }
}
