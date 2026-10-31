//! Fault seams for crash exploration; every ordinary operation reaches the memory store.

use anyhow::{Result, bail};
use async_trait::async_trait;
use std::{
    ops::Deref,
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
};
use time::OffsetDateTime;
use tokeira_kernel::{HistoryEvent, LoadedRun, Transition};
use tokeira_storage::{
    ActivityDispatchIdentity, BacklogEntry, BacklogPersistError, CommitResult, DeleteRunRequest,
    DeleteRunResult, DispatchableActivityTask, DispatchableWorkflowTask, DueActivityDispatch,
    DueTimer, InMemoryStore, PairCommitResult, PlacementPage, RecoveryCursor, RecoveryPage,
    RequestRecord, RunBulkWrite, RunHistoryStats, RunRepository, StaleTimer, TransitionAuditRecord,
    WorkflowDiscoveryRange, WorkflowDispatchPage, WorkflowDispatchPosition, WriteFence,
};
use tokeira_types::{
    ExecutionRef, NamespaceId, QueueKey, RequestId, RunId, RunKey, ShardId, WorkflowId,
};

#[derive(Debug, Default)]
pub(super) struct Repository {
    pub(super) inner: InMemoryStore,
    pub(super) refuse_pair: AtomicBool,
    pub(super) fail_reset: AtomicBool,
    pub(super) lose_publication_reply: AtomicBool,
    pub(super) lose_outbound_reply: AtomicU8,
    pub(super) fail_outbound_before: AtomicU8,
    pub(super) lose_child_seed_reply: AtomicBool,
    pub(super) lose_retirement: AtomicU8,
}

impl Deref for Repository {
    type Target = InMemoryStore;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl RunRepository for Repository {
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
        self.inner
            .commit_successor_fallback(run, closing, start, fence)
            .await
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
        if self.fail_reset.load(Ordering::SeqCst) && preparation.successor_intent.is_none() {
            bail!("injected reset preparation failure");
        }
        self.inner.prepare_run_publication(preparation, fence).await
    }

    async fn publish_prepared_run(
        &self,
        token: tokeira_storage::PreparationToken,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<tokeira_storage::PublicationResult> {
        let result = self.inner.publish_prepared_run(token, fence).await?;
        if self.lose_publication_reply.swap(false, Ordering::SeqCst) {
            bail!("injected lost publication reply after commit");
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
        if self.lose_retirement.fetch_and(!1, Ordering::SeqCst) & 1 != 0 {
            bail!("injected interruption before intent retirement");
        }
        let result = self.inner.retire_intent(proof, fence).await?;
        if self.lose_retirement.fetch_and(!2, Ordering::SeqCst) & 2 != 0 {
            bail!("injected lost intent retirement reply");
        }
        Ok(result)
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
        let bit = match guard.operation() {
            Some(incarnation) => match (incarnation.key.kind, run == incarnation.key.source_run) {
                (tokeira_kernel::OperationKind::ChildStart, false) => 1,
                (tokeira_kernel::OperationKind::ChildStart, true) => 2,
                (tokeira_kernel::OperationKind::ChildRelease, false) => 4,
                (
                    tokeira_kernel::OperationKind::ExternalSignal
                    | tokeira_kernel::OperationKind::ExternalCancel,
                    false,
                ) => 8,
                (
                    tokeira_kernel::OperationKind::ExternalSignal
                    | tokeira_kernel::OperationKind::ExternalCancel,
                    true,
                ) => 32,
                (tokeira_kernel::OperationKind::NexusStart, true) => 64,
                (tokeira_kernel::OperationKind::NexusCancel, true) => 128,
                _ => 0,
            },
            None => 16,
        };
        if bit != 0 && self.fail_outbound_before.fetch_and(!bit, Ordering::SeqCst) & bit != 0 {
            bail!("injected interruption before outbound commit");
        }
        let result = self
            .inner
            .commit_outbound_transition(run, transition, guard, source_fence, fence)
            .await?;
        if bit != 0 && self.lose_outbound_reply.fetch_and(!bit, Ordering::SeqCst) & bit != 0 {
            bail!("injected lost outbound commit reply");
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
        if self.refuse_pair.load(Ordering::SeqCst) {
            return Ok(PairCommitResult::SizeRefused);
        }
        self.inner
            .commit_transition_pair(run_key, closing, successor_run_key, successor, fence)
            .await
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        home: ShardId,
        transition: Transition,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        self.inner
            .commit_transition_for_bundle(run_key, home, transition, fence)
            .await
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

    fn recovery_is_empty(&self) -> bool {
        self.inner.recovery_is_empty()
    }

    async fn prepare_placement_page(&self) -> Result<PlacementPage> {
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
        self.inner.read_history_event(run_key, event_id).await
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

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        let seed_write = transition.history_events.is_empty()
            && transition
                .next_state
                .outbound
                .as_ref()
                .is_some_and(|outbound| !outbound.child_seeds.is_empty());
        let result = self
            .inner
            .commit_transition(run_key, transition, fence)
            .await?;
        if seed_write && self.lose_child_seed_reply.swap(false, Ordering::SeqCst) {
            bail!("injected lost child seed commit reply");
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
        self.inner
            .delete_run_for_bundle(run_key, execution_home_bundle, request, fence)
            .await
    }

    async fn materialize_reset_successor(
        &self,
        base_run_key: RunKey,
        fork_event_id: i64,
        successor_run_id: RunId,
        expected_current: Option<RunKey>,
        fence: WriteFence,
    ) -> Result<()> {
        if self.fail_reset.load(Ordering::SeqCst) {
            bail!("injected reset preparation failure");
        }
        self.inner
            .materialize_reset_successor(
                base_run_key,
                fork_event_id,
                successor_run_id,
                expected_current,
                fence,
            )
            .await
    }

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
    ) -> Result<Vec<RunBulkWrite>> {
        self.inner
            .list_run_bulk_writes(shard_id, after, limit)
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
        self.inner
            .list_workflow_dispatch_for_home(home, after, limit)
            .await
    }

    async fn reconcile_workflow_dispatch_run(
        &self,
        home: ShardId,
        run_key: RunKey,
        fence: WriteFence,
    ) -> Result<()> {
        self.inner
            .reconcile_workflow_dispatch_run(home, run_key, fence)
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

    async fn persist_to_backlog(
        &self,
        entries: Vec<BacklogEntry>,
    ) -> std::result::Result<(), BacklogPersistError> {
        self.inner.persist_to_backlog(entries).await
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
        cursor: Option<&RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage> {
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

    async fn delete_activity_dispatch_if_matches(
        &self,
        candidate: &ActivityDispatchIdentity,
    ) -> Result<bool> {
        self.inner
            .delete_activity_dispatch_if_matches(candidate)
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
        reason: StaleTimer,
    ) -> Result<bool> {
        self.inner.delete_due_timer_if_matches(timer, reason).await
    }
}
