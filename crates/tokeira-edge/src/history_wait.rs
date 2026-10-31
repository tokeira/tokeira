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
    AttributedHistoryEvent, BacklogEntry, BundleLease, CommitResult, DeleteRunRequest,
    DeleteRunResult, DispatchableActivityTask, DispatchableWorkflowTask, DueTimer, LeaseOutcome,
    LeaseRepository, PairCommitResult, RequestRecord, RunHistoryStats, RunRepository,
    TransitionAuditRecord, WorkerDeploymentVersionKey, WorkflowRuleCreateResult,
    WorkflowRuleDeleteResult, WriteFence,
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
    async fn read_published_preparations(
        &self,
        home: tokeira_types::ShardId,
        bound: tokeira_storage::ScanBound<uuid::Uuid>,
        after: Option<uuid::Uuid>,
        limit: usize,
    ) -> anyhow::Result<tokeira_storage::OutboundPage<uuid::Uuid, tokeira_storage::PreparationToken>>
    {
        self.inner
            .read_published_preparations(home, bound, after, limit)
            .await
    }

    async fn complete_run_preparation(
        &self,
        token: tokeira_storage::PreparationToken,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<bool> {
        self.inner.complete_run_preparation(token, fence).await
    }

    async fn commit_successor_fallback(
        &self,
        run: tokeira_types::RunKey,
        closing: tokeira_kernel::Transition,
        start: Box<tokeira_kernel::StartRequest>,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<tokeira_storage::CommitResult> {
        let result = self
            .inner
            .commit_successor_fallback(run, closing, start, fence)
            .await?;
        if let tokeira_storage::CommitResult::Applied { new_state } = &result {
            self.waits
                .notify(new_state.run_key, new_state.last_event_id)
                .await;
        }
        Ok(result)
    }

    async fn find_run_preparation(
        &self,
        request_key: &str,
    ) -> anyhow::Result<Option<tokeira_storage::PreparationToken>> {
        self.inner.find_run_preparation(request_key).await
    }

    async fn prepare_run_publication(
        &self,
        preparation: Box<tokeira_storage::RunPreparation>,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<tokeira_storage::PreparationToken> {
        self.inner.prepare_run_publication(preparation, fence).await
    }

    async fn publish_prepared_run(
        &self,
        token: tokeira_storage::PreparationToken,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<tokeira_storage::PublicationResult> {
        let result = self.inner.publish_prepared_run(token, fence).await?;
        if let tokeira_storage::PublicationResult::Applied {
            participants,
            new_run,
        } = &result
        {
            for state in participants.iter().chain(std::iter::once(new_run.as_ref())) {
                self.waits.notify(state.run_key, state.last_event_id).await;
            }
        }
        Ok(result)
    }

    async fn abandon_prepared_run(
        &self,
        token: tokeira_storage::PreparationToken,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<bool> {
        self.inner.abandon_prepared_run(token, fence).await
    }

    async fn read_operation_page(
        &self,
        home: tokeira_types::ShardId,
        kind: tokeira_kernel::OperationKind,
        bound: tokeira_storage::ScanBound<tokeira_kernel::OperationKey>,
        after: Option<tokeira_kernel::OperationKey>,
        limit: usize,
    ) -> anyhow::Result<
        tokeira_storage::OutboundPage<tokeira_kernel::OperationKey, tokeira_storage::OperationRow>,
    > {
        self.inner
            .read_operation_page(home, kind, bound, after, limit)
            .await
    }
    async fn read_intent_page(
        &self,
        home: tokeira_types::ShardId,
        bound: tokeira_storage::ScanBound<tokeira_storage::IntentKey>,
        after: Option<tokeira_storage::IntentKey>,
        limit: usize,
    ) -> anyhow::Result<
        tokeira_storage::OutboundPage<
            tokeira_storage::IntentKey,
            tokeira_storage::IntentDescriptor,
        >,
    > {
        self.inner.read_intent_page(home, bound, after, limit).await
    }
    async fn load_intent(
        &self,
        key: tokeira_storage::IntentKey,
    ) -> anyhow::Result<Option<tokeira_storage::IntentRow>> {
        self.inner.load_intent(key).await
    }
    async fn reconcile_outbound_run(
        &self,
        home: tokeira_types::ShardId,
        run: tokeira_types::RunKey,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<()> {
        self.inner.reconcile_outbound_run(home, run, fence).await
    }
    async fn retire_intent(
        &self,
        proof: tokeira_storage::IntentRetirement,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<bool> {
        self.inner.retire_intent(proof, fence).await
    }
    async fn load_outbound_source(
        &self,
        run: tokeira_types::RunKey,
    ) -> anyhow::Result<tokeira_kernel::LoadedRun> {
        self.inner.load_outbound_source(run).await
    }

    async fn commit_outbound_transition(
        &self,
        run: tokeira_types::RunKey,
        transition: tokeira_kernel::Transition,
        guard: tokeira_storage::DeliveryGuard,
        source_fence: tokeira_storage::WriteFence,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<tokeira_storage::CommitResult> {
        let last = transition.history_events.last().map(|event| event.event_id);
        let result = self
            .inner
            .commit_outbound_transition(run, transition, guard, source_fence, fence)
            .await?;
        if matches!(result, CommitResult::Applied { .. })
            && let Some(last) = last
        {
            self.waits.notify(run, last).await;
        }
        Ok(result)
    }

    fn recovery_is_empty(&self) -> bool {
        self.inner.recovery_is_empty()
    }

    fn outbound_ready(&self) -> bool {
        self.inner.outbound_ready()
    }
    async fn ensure_outbound_prepared(&self) -> anyhow::Result<()> {
        self.inner.ensure_outbound_prepared().await
    }
    async fn outbound_upgrade_status(
        &self,
    ) -> anyhow::Result<tokeira_storage::outbound_upgrade::OutboundUpgradeStatus> {
        self.inner.outbound_upgrade_status().await
    }
    async fn read_outbound_upgrade_run(
        &self,
        after: Option<tokeira_types::RunKey>,
    ) -> anyhow::Result<Option<tokeira_kernel::WorkflowState>> {
        self.inner.read_outbound_upgrade_run(after).await
    }
    async fn commit_outbound_upgrade_page(
        &self,
        expected: Option<tokeira_storage::outbound_upgrade::OutboundUpgradeProgress>,
        next: tokeira_storage::outbound_upgrade::OutboundUpgradeProgress,
        write: Option<tokeira_storage::outbound_upgrade::OutboundUpgradeWrite>,
    ) -> anyhow::Result<()> {
        self.inner
            .commit_outbound_upgrade_page(expected, next, write)
            .await
    }

    fn placement_ready(&self) -> bool {
        self.inner.placement_ready()
    }

    async fn prepare_placement_page(&self) -> Result<tokeira_storage::PlacementPage> {
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
        self.inner.load_run(run_key).await
    }

    async fn load_run_with_stats(&self, run_key: RunKey) -> Result<(LoadedRun, RunHistoryStats)> {
        // Forwarded explicitly: the trait default would drop the statistic.
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

    async fn read_history_event(
        &self,
        run_key: RunKey,
        event_id: i64,
    ) -> Result<Option<HistoryEvent>> {
        // Forwarded explicitly: the trait default would read through the general
        // history read instead of the store's one-batch lookup.
        self.inner.read_history_event(run_key, event_id).await
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

    async fn has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        // This adapter adds long-poll wakeups only. Delegating every authoritative read
        // is essential: falling back to RunRepository's compatibility default would
        // make Worker Deployment drainage report DRAINED while an open pinned run still
        // exists (`GetVersionDrainageStatus`, client.go @ v1.31.0).
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
        fence: WriteFence,
    ) -> Result<CommitResult> {
        let last_event_id = transition.history_events.last().map(|event| event.event_id);
        let result = self
            .inner
            .commit_transition(run_key, transition, fence)
            .await?;
        if matches!(result, CommitResult::Applied { .. })
            && let Some(last_event_id) = last_event_id
        {
            self.waits.notify(run_key, last_event_id).await;
        }
        Ok(result)
    }

    async fn commit_transition_pair(
        &self,
        run_key: RunKey,
        closing: Transition,
        successor_run_key: RunKey,
        successor: Transition,
        fence: WriteFence,
    ) -> Result<PairCommitResult> {
        let closing_last = closing.history_events.last().map(|event| event.event_id);
        let successor_last = successor.history_events.last().map(|event| event.event_id);
        let result = self
            .inner
            .commit_transition_pair(run_key, closing, successor_run_key, successor, fence)
            .await?;
        // Both histories are durable before either waiter wakes. Conflicts and
        // size refusals publish nothing and must not announce uncommitted events.
        if matches!(result, PairCommitResult::Applied { .. }) {
            if let Some(last_event_id) = closing_last {
                self.waits.notify(run_key, last_event_id).await;
            }
            if let Some(last_event_id) = successor_last {
                self.waits.notify(successor_run_key, last_event_id).await;
            }
        }
        Ok(result)
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        let last_event_id = transition.history_events.last().map(|event| event.event_id);
        let result = self
            .inner
            .commit_transition_for_bundle(run_key, execution_home_bundle, transition, fence)
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
        fence: WriteFence,
    ) -> Result<DeleteRunResult> {
        let result = self
            .inner
            .delete_run_for_bundle(run_key, execution_home_bundle, request, fence)
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
        expected_current: Option<RunKey>,
        fence: WriteFence,
    ) -> Result<()> {
        self.inner
            .materialize_reset_successor(
                base_run_key,
                fork_event_id,
                successor_run_id,
                expected_current,
                fence,
            )
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

    async fn reconcile_workflow_dispatch_run(
        &self,
        home: ShardId,
        run_key: RunKey,
        fence: WriteFence,
    ) -> Result<()> {
        // Repair changes derived intent only, so it must reach storage without
        // waking history readers or falling back to the trait's test-double stub.
        self.inner
            .reconcile_workflow_dispatch_run(home, run_key, fence)
            .await
    }

    async fn list_workflow_dispatch_page(
        &self,
        range: &tokeira_storage::WorkflowDiscoveryRange,
        after: Option<tokeira_storage::WorkflowDispatchPosition>,
        limit: std::num::NonZeroU32,
    ) -> Result<tokeira_storage::WorkflowDispatchPage> {
        self.inner
            .list_workflow_dispatch_page(range, after, limit)
            .await
    }

    async fn list_workflow_dispatch_for_home(
        &self,
        home: tokeira_types::ShardId,
        after: Option<RunKey>,
        limit: std::num::NonZeroU32,
    ) -> Result<Vec<RunKey>> {
        self.inner
            .list_workflow_dispatch_for_home(home, after, limit)
            .await
    }

    async fn list_sticky_workflow_dispatch(
        &self,
        home: ShardId,
        after: Option<RunKey>,
        limit: std::num::NonZeroU32,
    ) -> Result<tokeira_storage::StickyWorkflowDispatchPage> {
        self.inner
            .list_sticky_workflow_dispatch(home, after, limit)
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
        candidate: &tokeira_storage::ActivityDispatchIdentity,
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

    // Purges and abandoned materializations change no visible run: a recorded
    // run has no mutable state, so no waiter can be watching it.
    async fn abandon_materialization(&self, run_key: RunKey, fence: WriteFence) -> Result<()> {
        self.inner.abandon_materialization(run_key, fence).await
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

    async fn dispose_workflow_backlog_page(
        &self,
        after: Option<uuid::Uuid>,
    ) -> Result<tokeira_storage::WorkflowBacklogDisposal> {
        self.inner.dispose_workflow_backlog_page(after).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        self.inner.drain_backlog(queue, limit).await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        self.inner.list_due_timers(now, limit).await
    }

    async fn list_recovery_candidates_for_shard(
        &self,
        shard_id: ShardId,
        cursor: Option<&tokeira_storage::RecoveryCursor>,
        limit: usize,
    ) -> Result<tokeira_storage::RecoveryPage> {
        self.inner
            .list_recovery_candidates_for_shard(shard_id, cursor, limit)
            .await
    }

    async fn list_due_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        after: Option<&tokeira_storage::DueActivityDispatch>,
        limit: usize,
    ) -> Result<Vec<tokeira_storage::DueActivityDispatch>> {
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
    use time::Duration;
    use tokeira_kernel::{
        BasicKernel, Command, Kernel, RetryState, StartRequest, WorkflowExecutionTimedOutRequest,
        WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowTimeoutType,
        state::{VersioningOverride, WorkerDeploymentVersionRef},
    };
    use tokeira_storage::{BuildId, DeploymentName, InMemoryStore};
    use tokeira_types::{
        Memo, Payloads, RequestContext, RunId, SearchAttributes, TaskQueueName, WorkflowRuleAction,
        WorkflowRuleTrigger, WorkflowType,
    };

    fn pinned_start_transition(
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Transition {
        let workflow_id = WorkflowId("pinned-workflow".to_string());
        let run_id = RunId::new();
        let run_key = RunKey::derive(namespace_id, &workflow_id, run_id);
        BasicKernel
            .apply(
                LoadedRun::Absent,
                Command::Start(StartRequest {
                    advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
                    initiator: None,
                    run_key,
                    namespace_id,
                    workflow_id,
                    run_id,
                    workflow_type: WorkflowType("workflow-type".to_string()),
                    task_queue: TaskQueueName("queue".to_string()),
                    deployment: None,
                    build_id: None,
                    versioning_override: Some(VersioningOverride::Pinned {
                        version: WorkerDeploymentVersionRef {
                            deployment_name: version.deployment_name.0.clone(),
                            build_id: version.build_id.0.clone(),
                        },
                    }),
                    workflow_start_delay: None,
                    client_cron_schedule: None,
                    completion_callbacks: Vec::new(),
                    user_metadata: None,
                    links: Vec::new(),
                    on_conflict_options: None,
                    priority: None,
                    input: Payloads::default(),
                    header: None,
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: Duration::seconds(10),
                    retry_policy: None,
                    conflict_policy: WorkflowIdConflictPolicy::Fail,
                    reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                    attempt: 1,
                    continued_execution_run_id: None,
                    first_execution_run_id: Some(run_id),
                    parent_run_key: None,
                    parent_workflow_id: None,
                    parent_run_id: None,
                    parent_namespace_id: None,
                    parent_namespace_name: None,
                    parent_initiated_event_id: 0,
                    root_workflow_id: None,
                    root_run_id: None,
                    original_execution_run_id: Some(run_id),
                    continued_failure: None,
                    last_completion_result: None,
                    first_run_started_at: None,
                    request: RequestContext {
                        request_id: RequestId("pinned-start".to_string()),
                        caller_identity: None,
                        principal: None,
                        received_at: OffsetDateTime::UNIX_EPOCH,
                    },
                    now: OffsetDateTime::UNIX_EPOCH,
                    cron_schedule: None,
                    reserved_poller_identity: None,
                    eager_execution_accepted: false,
                    inherited_versioning_info: None,
                }),
            )
            .expect("pinned start transition")
    }

    async fn seed_open_pinned_run(
        repo: &HistoryNotifyingRepository<InMemoryStore>,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) {
        let transition = pinned_start_transition(namespace_id, version);
        let run_key = transition.next_state.run_key;
        assert!(matches!(
            repo.commit_transition(run_key, transition, WriteFence::Unfenced)
                .await
                .expect("commit pinned start"),
            CommitResult::Applied { .. }
        ));
    }

    #[tokio::test]
    async fn atomic_successor_commit_notifies_both_histories_only_after_application() {
        let inner = Arc::new(InMemoryStore::default());
        let waits = HistoryWaitRegistry::default();
        let repo = HistoryNotifyingRepository::new(inner.clone(), waits.clone());
        let namespace_id = NamespaceId::new();
        let version = WorkerDeploymentVersionKey {
            deployment_name: DeploymentName("deployment".into()),
            build_id: BuildId("build".into()),
        };
        let start = pinned_start_transition(namespace_id, &version);
        let state = start.next_state.clone();
        let run_key = state.run_key;
        inner
            .commit_transition(run_key, start, WriteFence::Unfenced)
            .await
            .unwrap();
        let successor = pinned_start_transition(namespace_id, &version);
        let successor_key = successor.next_state.run_key;
        let closing = BasicKernel
            .apply(
                LoadedRun::Existing(state.clone()),
                Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
                    timeout_type: WorkflowTimeoutType::RunTimeout,
                    retry_state: RetryState::InProgress,
                    new_execution_run_id: Some(successor.next_state.run_id),
                    now: state.started_at + Duration::seconds(10),
                }),
            )
            .unwrap();
        let closing_last = closing.history_events.last().unwrap().event_id;
        let successor_last = successor.history_events.last().unwrap().event_id;
        let mut closing_waiter = waits.receiver(run_key, state.last_event_id).await;
        let mut successor_waiter = waits.receiver(successor_key, 0).await;

        assert!(
            repo.commit_transition_pair(
                run_key,
                closing.clone(),
                run_key,
                successor.clone(),
                WriteFence::Unfenced,
            )
            .await
            .is_err()
        );
        assert!(!closing_waiter.has_changed().unwrap());
        assert!(!successor_waiter.has_changed().unwrap());

        assert!(matches!(
            repo.commit_transition_pair(
                run_key,
                closing.clone(),
                successor_key,
                successor.clone(),
                WriteFence::Unfenced,
            )
            .await
            .unwrap(),
            PairCommitResult::Applied { .. }
        ));
        assert!(closing_waiter.has_changed().unwrap());
        assert!(successor_waiter.has_changed().unwrap());
        assert_eq!(*closing_waiter.borrow_and_update(), closing_last);
        assert_eq!(*successor_waiter.borrow_and_update(), successor_last);
        assert_eq!(
            inner
                .find_latest_run(namespace_id, &state.workflow_id)
                .await
                .unwrap(),
            Some(successor_key)
        );
        assert_eq!(
            inner
                .read_history_event(run_key, closing_last)
                .await
                .unwrap()
                .unwrap()
                .event_id,
            closing_last
        );
        assert_eq!(
            inner
                .read_history_event(successor_key, successor_last)
                .await
                .unwrap()
                .unwrap()
                .event_id,
            successor_last
        );

        assert!(matches!(
            repo.commit_transition_pair(
                run_key,
                closing,
                successor_key,
                successor,
                WriteFence::Unfenced,
            )
            .await
            .unwrap(),
            PairCommitResult::Conflict { .. }
        ));
        assert!(!closing_waiter.has_changed().unwrap());
        assert!(!successor_waiter.has_changed().unwrap());
    }

    #[tokio::test]
    async fn open_pinned_workflow_read_delegates_through_history_notifier() {
        let inner = Arc::new(InMemoryStore::default());
        let repo = HistoryNotifyingRepository::new(inner, HistoryWaitRegistry::default());
        let namespace_id = NamespaceId::new();
        let version = WorkerDeploymentVersionKey {
            deployment_name: DeploymentName("deployment".to_string()),
            build_id: BuildId("build".to_string()),
        };
        seed_open_pinned_run(&repo, namespace_id, &version).await;

        assert!(
            repo.has_open_pinned_workflows(namespace_id, &version)
                .await
                .expect("read through wrapper")
        );
    }

    #[tokio::test]
    async fn restored_store_placement_preparation_delegates_through_history_notifier() {
        let original = Arc::new(InMemoryStore::default());
        let repo =
            HistoryNotifyingRepository::new(original.clone(), HistoryWaitRegistry::default());
        seed_open_pinned_run(
            &repo,
            NamespaceId::new(),
            &WorkerDeploymentVersionKey {
                deployment_name: DeploymentName("deployment".into()),
                build_id: BuildId("build".into()),
            },
        )
        .await;
        let restored =
            Arc::new(InMemoryStore::from_snapshot(&original.snapshot().await.unwrap()).unwrap());
        let repo =
            HistoryNotifyingRepository::new(restored.clone(), HistoryWaitRegistry::default());
        assert!(!restored.placement_ready());
        assert!(!repo.placement_ready());
        let progress = tokeira_storage::prepare_execution_placement(&repo)
            .await
            .unwrap();
        assert!(progress.complete());
        assert_eq!(progress.hot_examined, 1);
        assert!(restored.placement_ready());
        assert!(repo.placement_ready());
    }

    #[tokio::test]
    async fn sticky_workflow_dispatch_delegates_through_history_notifier() {
        let inner = Arc::new(InMemoryStore::with_shard_count(8));
        let repo = HistoryNotifyingRepository::new(inner.clone(), HistoryWaitRegistry::default());
        let version = WorkerDeploymentVersionKey {
            deployment_name: DeploymentName("deployment".into()),
            build_id: BuildId("build".into()),
        };
        let home = ShardId(5);
        let mut namespace = 1;
        let mut expected = Vec::new();
        for (selected_home, sticky) in [
            (home, true),
            (home, true),
            (ShardId(6), true),
            (home, false),
        ] {
            let mut transition = loop {
                let candidate = pinned_start_transition(
                    NamespaceId(uuid::Uuid::from_u128(namespace)),
                    &version,
                );
                namespace += 1;
                let state = &candidate.next_state;
                if tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    8,
                ) == selected_home
                {
                    break candidate;
                }
            };
            let state = &mut transition.next_state;
            let key = state.run_key;
            if sticky {
                state.sticky = Some(tokeira_types::StickyAffinity {
                    worker_identity: tokeira_types::WorkerIdentity("worker".into()),
                    sticky_queue: TaskQueueName("sticky".into()),
                    schedule_to_start_timeout: Duration::seconds(30),
                });
                state
                    .pending_workflow_task
                    .as_mut()
                    .unwrap()
                    .schedule_to_start_deadline =
                    Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(30));
                if selected_home == home {
                    expected.push(tokeira_storage::derive_workflow_dispatch(state, home).unwrap());
                }
            }
            assert!(matches!(
                repo.commit_transition(key, transition, WriteFence::Unfenced)
                    .await
                    .unwrap(),
                CommitResult::Applied { .. }
            ));
        }
        expected.sort_by_key(|row| row.incarnation.run_key);
        let limit = std::num::NonZeroU32::new(1).unwrap();
        let mut after = None;
        for row in expected {
            let page = repo
                .list_sticky_workflow_dispatch(home, after, limit)
                .await
                .unwrap();
            assert_eq!(
                page,
                inner
                    .list_sticky_workflow_dispatch(home, after, limit)
                    .await
                    .unwrap()
            );
            assert_eq!(page.candidates, vec![row.clone()]);
            assert_eq!(page.last_examined, Some(row.incarnation.run_key));
            assert!(!page.exhausted);
            after = page.last_examined;
        }
        let end = repo
            .list_sticky_workflow_dispatch(home, after, limit)
            .await
            .unwrap();
        assert!(end.candidates.is_empty());
        assert!(end.last_examined.is_none());
        assert!(end.exhausted);
    }

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
