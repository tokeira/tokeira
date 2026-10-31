//! DSQL-backed implementation of the semantic `RunRepository` contract.
//!
//! The physical schema is spread-key-first: hot write tables use UUID keys
//! derived from logical identifiers, while secondary indexes serve targeted
//! read paths. The repository is the only module that should know how those
//! tables combine into workflow semantics; callers continue to use the storage
//! trait in terms of runs, history, leases, dispatch, and sweep entries.

use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Connection;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityOp, BasicKernel, DispatchOp, HistoryEvent, LoadedRun, ReplayContext, TimerOp,
    Transition, WorkflowState, merge_priority,
};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionRef, ExecutionStatus, NamespaceId, QueueKey, RequestId, RunId,
    RunKey, ShardEpoch, ShardId, TaskKind, TaskQueueName, TransitionSeq, WorkflowId,
    WorkflowRuleRecord, dsql_spread_uuid,
};
use tracing::{Instrument, instrument};
use uuid::Uuid;

use crate::{
    ActivityDispatchIdentity, AttributedHistoryEvent, BacklogEntry, BacklogPayload, CommitResult,
    CurrentExecutionConflictPolicy, DbClass, DeleteRunRequest, DeleteRunResult, DeliveryOrder,
    DispatchableActivityTask, DispatchableWorkflowTask, DueActivityDispatch, DueTimer,
    LeaseFenceLost, ProjectionRecord, RequestRecord, RunHistoryStats, RunRepository,
    TransitionAuditRecord, WorkerDeploymentVersionKey, WorkflowRuleCreateResult,
    WorkflowRuleDeleteResult, WriteFence, api::AppliedProjectionSeed,
    deleted_workflow_projection_context, dispatchable_workflow_task, metrics,
    prepare_workflow_projection, seed_workflow_projection_accumulator_from_newer,
    workflow_is_open_and_pinned_to_version,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, codec, convert};
use crate::recovery_index::{
    RecoveryCursor, RecoveryPage, RecoveryPhase, read_candidate_page, recovery_needed,
};

/// Current projection fanout for records written by this repository.
///
/// The schema supports partitioned projection scans; the MVP uses one logical
/// fanout value while still assigning deterministic partitions.
const PROJECTION_FANOUT: i16 = 1;

fn dsql_retry_operation_label(
    operation: &str,
) -> Option<tokeira_observability::StorageOperationLabel> {
    match operation {
        "commit_transition" => Some(tokeira_observability::StorageOperationLabel::CommitTransition),
        "commit_transition_for_bundle" => {
            Some(tokeira_observability::StorageOperationLabel::CommitTransitionForBundle)
        }
        _ => None,
    }
}

macro_rules! record_dsql_operation {
    ($repo:expr, $operation:expr, $shard_id:expr, $body:block) => {{
        let started = std::time::Instant::now();
        let result = (async $body).await;
        // Instrument at the repository boundary so helper functions can stay
        // focused on persistence semantics and every public operation gets the
        // same outcome/error classification.
        $repo.record_operation_result($operation, $shard_id, started.elapsed(), &result);
        result
    }};
}

macro_rules! record_dsql_commit_operation {
    ($repo:expr, $operation:expr, $shard_id:expr, $body:block) => {{
        let started = std::time::Instant::now();
        let result = (async $body).await;
        $repo.record_commit_operation_result($operation, $shard_id, started.elapsed(), &result);
        result
    }};
}

mod activity;
#[cfg(all(test, feature = "dsql-integration"))]
mod atomic_successors_live_tests;
mod bulk_write;
#[cfg(all(test, feature = "dsql-integration"))]
mod bulk_write_tests;
mod commit;
mod delete;
mod dispatch;
#[cfg(all(test, feature = "dsql-integration"))]
mod history_event_tests;
#[cfg(all(test, feature = "dsql-integration"))]
mod large_value_tests;
#[cfg(all(test, feature = "dsql-integration"))]
mod lease_fence_tests;
mod leases;
mod load;
mod outbound;
#[cfg(all(test, feature = "dsql-integration"))]
mod outbound_live_tests;
mod outbound_upgrade;
mod outbound_upgrade_parts;
mod pair;
mod placement;
#[cfg(all(test, feature = "dsql-integration"))]
mod placement_tests;
mod preparation;
mod run_value_parts;
mod successor;
mod timers;
#[cfg(all(test, feature = "dsql-integration"))]
mod update_refusals_tests;
mod visibility;
mod workflow_dispatch;
#[cfg(all(test, feature = "dsql-integration"))]
mod workflow_dispatch_tests;
mod workflow_rules;

#[cfg(test)]
#[cfg(feature = "dsql-integration")]
mod projection_accumulator_tests;

/// Test-only pause points (`bundle-lease-fence`): a write held after its fence
/// read and before its commit, or an acquisition held after its statements and
/// before its commit, so a live test can let another transaction commit in
/// between. They exist only in this crate's live tests.
#[cfg(all(test, feature = "dsql-integration"))]
pub(crate) mod pause_point {
    use std::sync::Mutex;

    use tokeira_types::{RunKey, ShardId};
    use tokio::sync::oneshot;
    use uuid::Uuid;

    /// What a pause point holds.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum PauseKey {
        /// A fenced write of this run, after its fence read.
        Write(RunKey),
        /// An acquisition of this home, before its commit.
        Acquisition(ShardId),
        /// A release of this home, before its commit.
        Release(ShardId),
        /// A copy page of this materialized run, before its transaction.
        Page(RunKey),
        /// Legacy workflow backlog deletes before the page commits.
        Disposal,
        /// A load of this run's split state, after its first read and before it
        /// reads the state again with its parts (`large-value-parts`).
        StateParts(RunKey),
        /// A history read of this run that met a split batch, after its first read
        /// of the page and before it reads the page again with its parts.
        HistoryParts(RunKey),
        /// A purge of this run, after each of its transactions.
        Purge(RunKey),
        /// A load of the CHASM execution with this run id that met a node stored
        /// in parts, after its first read and before it reads the nodes again
        /// with their parts.
        NodeParts(Uuid),
    }

    struct Armed {
        key: PauseKey,
        skip: usize,
        reached: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    }

    static ARMED: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

    /// Hold the next transaction of `key` that reaches its pause point. It
    /// signals the returned receiver when it gets there, and waits until the
    /// returned sender sends or drops. Other transactions pass.
    pub(crate) fn arm(key: PauseKey) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        arm_after(key, 0)
    }

    /// As [`arm`], after `skip` transactions of `key` have passed.
    pub(crate) fn arm_after(
        key: PauseKey,
        skip: usize,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let mut armed = ARMED.lock().expect("pause point lock poisoned");
        armed.retain(|armed| armed.key != key);
        armed.push(Armed {
            key,
            skip,
            reached: reached_tx,
            resume: resume_rx,
        });
        (reached_rx, resume_tx)
    }

    /// A transaction of `key` at its pause point.
    pub(crate) async fn reached(key: PauseKey) {
        let held = {
            let mut armed = ARMED.lock().expect("pause point lock poisoned");
            match armed.iter().position(|armed| armed.key == key) {
                Some(index) if armed[index].skip == 0 => Some(armed.remove(index)),
                Some(index) => {
                    armed[index].skip -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some(held) = held {
            let _ = held.reached.send(());
            let _ = held.resume.await;
        }
    }
}

#[cfg(test)]
use activity::{ActivityDispatchRow, activity_dispatch_from_row};
#[cfg(test)]
use dispatch::collect_dispatchable_workflow_tasks;
#[cfg(test)]
use leases::{RenewDecision, decide_renew, interpret_acquire};

/// Production `RunRepository` backed by Aurora DSQL.
#[derive(Debug)]
pub struct DsqlRunRepository {
    /// Abstracted acquisition boundary.
    ///
    /// Production uses `DsqlConnectionDirector`; tests use a fake acquirer to
    /// prove routing and zero-limit behavior without opening SQL connections.
    director: Arc<dyn DsqlConnectionAcquirer>,
    placement_ready: std::sync::atomic::AtomicBool,
    outbound_ready: std::sync::atomic::AtomicBool,
    /// Non-zero runtime shard count used to map run keys to shard ownership.
    shard_count: u32,
    /// Non-zero projection partition count used when writing projection logs.
    projection_partition_count: u32,
    /// Workflow-id conflict policy applied during start commits.
    conflict_policy: CurrentExecutionConflictPolicy,
    /// Duration added to one captured application timestamp for lease expiry.
    lease_duration: Duration,
}

impl DsqlRunRepository {
    /// Build a repository using the production DSQL connection director.
    pub fn new(
        director: Arc<DsqlConnectionDirector>,
        shard_count: u32,
        projection_partition_count: u32,
        conflict_policy: CurrentExecutionConflictPolicy,
        lease_duration: Duration,
    ) -> Result<Self> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        if projection_partition_count == 0 {
            bail!("projection_partition_count must be greater than zero");
        }
        if lease_duration <= Duration::ZERO {
            bail!("lease_duration must be positive");
        }
        Ok(Self {
            placement_ready: std::sync::atomic::AtomicBool::new(false),
            outbound_ready: std::sync::atomic::AtomicBool::new(false),
            director: director as Arc<dyn DsqlConnectionAcquirer>,
            shard_count,
            projection_partition_count,
            conflict_policy,
            lease_duration,
        })
    }

    #[cfg(test)]
    fn new_with_acquirer(
        director: Arc<dyn DsqlConnectionAcquirer>,
        shard_count: u32,
        projection_partition_count: u32,
        conflict_policy: CurrentExecutionConflictPolicy,
        lease_duration: Duration,
    ) -> Result<Self> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        if projection_partition_count == 0 {
            bail!("projection_partition_count must be greater than zero");
        }
        if lease_duration <= Duration::ZERO {
            bail!("lease_duration must be positive");
        }
        Ok(Self {
            placement_ready: std::sync::atomic::AtomicBool::new(false),
            outbound_ready: std::sync::atomic::AtomicBool::new(false),
            director,
            shard_count,
            projection_partition_count,
            conflict_policy,
            lease_duration,
        })
    }

    #[cfg(test)]
    pub(crate) fn shard_for_run_key_with_count(
        run_key: RunKey,
        shard_count: u32,
    ) -> Result<ShardId> {
        if shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        Ok(ShardId((run_key.0.as_u128() as u32) % shard_count))
    }

    pub(crate) fn shard_for_run_key(&self, run_key: RunKey) -> ShardId {
        debug_assert!(self.shard_count > 0);
        // The run key is already a spread UUID. Taking its low bits is stable
        // and cheap, and keeps routing aligned with rows written by this
        // repository.
        ShardId((run_key.0.as_u128() as u32) % self.shard_count)
    }

    /// Stable encoding of `ShardId(u32)` to UUID for SQL binding.
    ///
    /// Feature specs originally modeled shard IDs as UUID columns. This helper
    /// preserves that schema without changing the public `ShardId(pub u32)`
    /// type used throughout runtime ownership code.
    pub(crate) fn shard_id_to_uuid(shard_id: ShardId) -> Uuid {
        let mut bytes = *b"tokeira-shard-id";
        bytes[12..16].copy_from_slice(&shard_id.0.to_be_bytes());
        Uuid::from_bytes(bytes)
    }

    pub(crate) fn shard_id_from_uuid(value: Uuid) -> Result<ShardId> {
        let bytes = value.into_bytes();
        if &bytes[0..12] != b"tokeira-shar" {
            bail!("shard UUID does not use reversible shard-id encoding");
        }
        Ok(ShardId(u32::from_be_bytes(
            bytes[12..16]
                .try_into()
                .context("invalid shard UUID length")?,
        )))
    }

    pub(crate) fn current_execution_key(
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Uuid {
        // Workflow-id uniqueness is a workflow-level invariant, so this key
        // deliberately excludes run_id.
        dsql_spread_uuid(&[
            b"current-execution",
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
        ])
    }

    pub(crate) fn request_dedupe_key(
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
        request_id: &RequestId,
    ) -> Uuid {
        // Request dedupe is workflow-scoped for start/signal/update style
        // operations. Explicit run filtering is applied when the row is read.
        dsql_spread_uuid(&[
            b"request-dedupe",
            namespace_id.0.as_bytes(),
            workflow_id.0.as_bytes(),
            request_id.0.as_bytes(),
        ])
    }

    pub(crate) fn dispatch_backlog_key(
        partition_id: u32,
        queue_namespace: NamespaceId,
        queue_name: &str,
        task_kind: TaskKind,
        deployment: Option<&str>,
        build_id: Option<&str>,
        run_key: RunKey,
        payload: &BacklogPayload,
    ) -> Uuid {
        let deployment = option_key_part(deployment);
        let build_id = option_key_part(build_id);
        let task_kind = (task_kind.to_db_smallint() as u16).to_le_bytes();
        // Delivery order resets with a queue-home broker and therefore cannot
        // identify durable work. Logical task identity makes the same publish
        // idempotent without colliding distinct work after a restart.
        match payload {
            BacklogPayload::Workflow { logical_seq } => dsql_spread_uuid(&[
                b"dispatch-backlog",
                &partition_id.to_le_bytes(),
                queue_namespace.0.as_bytes(),
                queue_name.as_bytes(),
                &task_kind,
                &deployment,
                &build_id,
                run_key.0.as_bytes(),
                b"workflow",
                &logical_seq.0.to_be_bytes(),
            ]),
            BacklogPayload::Activity {
                activity_id,
                attempt,
                stamp,
                ..
            } => dsql_spread_uuid(&[
                b"dispatch-backlog",
                &partition_id.to_le_bytes(),
                queue_namespace.0.as_bytes(),
                queue_name.as_bytes(),
                &task_kind,
                &deployment,
                &build_id,
                run_key.0.as_bytes(),
                b"activity",
                activity_id.as_bytes(),
                &attempt.to_be_bytes(),
                &stamp.to_be_bytes(),
            ]),
        }
    }

    pub(crate) fn activity_dispatch_key(run_key: RunKey, activity_id: &str) -> Uuid {
        dsql_spread_uuid(&[
            b"activity-dispatch",
            run_key.0.as_bytes(),
            activity_id.as_bytes(),
        ])
    }

    pub(crate) fn is_serialization_failure(err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("40001"))
    }

    fn record_operation_result<T>(
        &self,
        operation: &'static str,
        shard_id: Option<ShardId>,
        duration: std::time::Duration,
        result: &Result<T>,
    ) {
        let outcome = classify_outcome(result);
        metrics::record_dsql_operation_duration(operation, outcome, duration);
        metrics::record_dsql_query_duration(operation, outcome, duration);
        metrics::record_dsql_operation_total(operation, outcome);
        metrics::record_storage_operation(operation, outcome);
        match operation {
            "load_run" => metrics::record_load_run_duration(duration),
            "read_history" => metrics::record_read_history_duration(duration),
            _ => {}
        }
        if let Some(shard_id) = shard_id {
            metrics::record_dsql_shard_operation(shard_id.0, operation);
            metrics::record_dsql_shard_duration(shard_id.0, duration);
            if outcome == "conflict" {
                metrics::record_dsql_shard_conflict(shard_id.0);
            }
        }
        if let Err(error) = result {
            if outcome == "conflict"
                && let Some(operation) = dsql_retry_operation_label(operation)
            {
                metrics::record_dsql_occ_conflict(operation);
            }
            if let Some(sqlstate) = extract_sqlstate(error) {
                metrics::record_dsql_error_code(&sqlstate);
            }
            if let Some(kind) = classify_connection_error(error) {
                metrics::record_dsql_connection_error(kind);
            }
        }
    }

    fn record_commit_operation_result(
        &self,
        operation: &'static str,
        shard_id: Option<ShardId>,
        duration: std::time::Duration,
        result: &Result<CommitResult>,
    ) {
        let outcome = match result {
            Ok(CommitResult::Conflict { .. }) => "conflict",
            Ok(_) => "success",
            Err(error) if is_serialization_failure_error(error) => "conflict",
            Err(error) if is_lease_fence_lost(error) => "lease_lost",
            Err(_) => "error",
        };
        metrics::record_dsql_operation_duration(operation, outcome, duration);
        metrics::record_dsql_query_duration(operation, outcome, duration);
        metrics::record_dsql_operation_total(operation, outcome);
        metrics::record_storage_operation(operation, outcome);
        if matches!(
            operation,
            "commit_transition" | "commit_transition_for_bundle"
        ) {
            metrics::record_commit_transition_duration(None, outcome, duration);
        }
        if let Some(shard_id) = shard_id {
            metrics::record_dsql_shard_operation(shard_id.0, operation);
            metrics::record_dsql_shard_duration(shard_id.0, duration);
            if outcome == "conflict" {
                metrics::record_dsql_shard_conflict(shard_id.0);
            }
        }
        if outcome == "conflict"
            && let Some(operation) = dsql_retry_operation_label(operation)
        {
            metrics::record_dsql_occ_conflict(operation);
        }
        if let Err(error) = result {
            if let Some(sqlstate) = extract_sqlstate(error) {
                metrics::record_dsql_error_code(&sqlstate);
            }
            if let Some(kind) = classify_connection_error(error) {
                metrics::record_dsql_connection_error(kind);
            }
        }
    }
}

fn classify_outcome<T>(result: &Result<T>) -> &'static str {
    match result {
        Ok(_) => "success",
        Err(error) if is_serialization_failure_error(error) => "conflict",
        Err(error) if is_lease_fence_lost(error) => "lease_lost",
        Err(_) => "error",
    }
}

fn is_serialization_failure_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .is_some_and(DsqlRunRepository::is_serialization_failure)
}

fn extract_sqlstate(error: &anyhow::Error) -> Option<String> {
    // SQLSTATE values are the most stable way to alert on DSQL/PostgreSQL
    // classes across connector versions.
    error
        .chain()
        .find_map(|cause| match cause.downcast_ref::<sqlx::Error>() {
            Some(sqlx::Error::Database(database_error)) => {
                database_error.code().map(|code| code.into_owned())
            }
            _ => None,
        })
}

fn classify_connection_error(error: &anyhow::Error) -> Option<&'static str> {
    // Keep this intentionally coarse. Operators need to distinguish transport,
    // timeout, refusal, and TLS classes; SQL semantics are reported separately
    // through SQLSTATE.
    error
        .chain()
        .find_map(|cause| match cause.downcast_ref::<sqlx::Error>() {
            Some(sqlx::Error::Io(io_error)) => Some(match io_error.kind() {
                std::io::ErrorKind::ConnectionReset => "reset",
                std::io::ErrorKind::TimedOut => "timeout",
                std::io::ErrorKind::ConnectionRefused => "refused",
                _ => "reset",
            }),
            Some(sqlx::Error::Tls(_)) => Some("tls"),
            _ => None,
        })
}

#[async_trait]
impl RunRepository for DsqlRunRepository {
    async fn read_published_preparations(
        &self,
        home: tokeira_types::ShardId,
        bound: crate::ScanBound<uuid::Uuid>,
        after: Option<uuid::Uuid>,
        limit: usize,
    ) -> anyhow::Result<crate::OutboundPage<uuid::Uuid, crate::PreparationToken>> {
        self.do_published_preparations(home, bound, after, limit)
            .await
    }

    async fn complete_run_preparation(
        &self,
        token: crate::PreparationToken,
        fence: crate::WriteFence,
    ) -> anyhow::Result<bool> {
        self.do_complete_preparation(token, fence).await
    }

    async fn commit_successor_fallback(
        &self,
        run: tokeira_types::RunKey,
        closing: tokeira_kernel::Transition,
        start: Box<tokeira_kernel::StartRequest>,
        fence: crate::WriteFence,
    ) -> anyhow::Result<crate::CommitResult> {
        Box::pin(self.do_commit_successor_fallback(run, closing, start, fence)).await
    }

    async fn find_run_preparation(
        &self,
        request_key: &str,
    ) -> Result<Option<crate::PreparationToken>> {
        self.do_find_run_preparation(request_key).await
    }
    async fn prepare_run_publication(
        &self,
        preparation: Box<crate::RunPreparation>,
        fence: WriteFence,
    ) -> Result<crate::PreparationToken> {
        Box::pin(self.do_prepare_publication(preparation, fence)).await
    }
    async fn publish_prepared_run(
        &self,
        token: crate::PreparationToken,
        fence: WriteFence,
    ) -> Result<crate::PublicationResult> {
        Box::pin(self.do_publish_preparation(token, fence)).await
    }
    async fn abandon_prepared_run(
        &self,
        token: crate::PreparationToken,
        fence: WriteFence,
    ) -> Result<bool> {
        self.do_abandon_preparation(token, fence).await
    }

    async fn read_operation_page(
        &self,
        home: ShardId,
        kind: tokeira_kernel::OperationKind,
        bound: crate::ScanBound<tokeira_kernel::OperationKey>,
        after: Option<tokeira_kernel::OperationKey>,
        limit: usize,
    ) -> Result<crate::OutboundPage<tokeira_kernel::OperationKey, crate::OperationRow>> {
        self.do_read_operation_page(home, kind, bound, after, limit)
            .await
    }
    async fn read_intent_page(
        &self,
        home: ShardId,
        bound: crate::ScanBound<crate::IntentKey>,
        after: Option<crate::IntentKey>,
        limit: usize,
    ) -> Result<crate::OutboundPage<crate::IntentKey, crate::IntentDescriptor>> {
        self.do_read_intent_page(home, bound, after, limit).await
    }
    async fn load_intent(&self, key: crate::IntentKey) -> Result<Option<crate::IntentRow>> {
        self.do_load_intent(key).await
    }
    async fn reconcile_outbound_run(
        &self,
        home: ShardId,
        run: RunKey,
        fence: WriteFence,
    ) -> Result<()> {
        self.do_reconcile_outbound_run(home, run, fence).await
    }
    async fn retire_intent(
        &self,
        proof: crate::IntentRetirement,
        fence: WriteFence,
    ) -> Result<bool> {
        self.do_retire_intent(proof, fence).await
    }
    async fn commit_outbound_transition(
        &self,
        run: RunKey,
        transition: Transition,
        guard: crate::DeliveryGuard,
        source_fence: WriteFence,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        Box::pin(self.do_commit_guarded(run, transition, Some((guard, source_fence)), fence)).await
    }

    // Allocate before constructing the erased future. An async-trait body would
    // first capture two large inline transitions in its own outer allocation.
    fn commit_transition_pair<'life0, 'async_trait>(
        &'life0 self,
        run_key: RunKey,
        closing: Transition,
        successor_run_key: RunKey,
        successor: Transition,
        fence: WriteFence,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<crate::PairCommitResult>> + Send + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(self.do_commit_transition_pair(
            run_key,
            closing,
            successor_run_key,
            successor,
            fence,
        ))
    }
    fn placement_ready(&self) -> bool {
        self.placement_ready
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn outbound_ready(&self) -> bool {
        self.outbound_ready
            .load(std::sync::atomic::Ordering::Acquire)
    }
    async fn ensure_outbound_prepared(&self) -> Result<()> {
        self.do_ensure_outbound_prepared().await
    }
    async fn outbound_upgrade_status(
        &self,
    ) -> Result<crate::outbound_upgrade::OutboundUpgradeStatus> {
        self.do_outbound_upgrade_status().await
    }
    async fn read_outbound_upgrade_run(
        &self,
        after: Option<RunKey>,
    ) -> Result<Option<WorkflowState>> {
        self.do_read_outbound_upgrade_run(after).await
    }
    async fn commit_outbound_upgrade_page(
        &self,
        expected: Option<crate::outbound_upgrade::OutboundUpgradeProgress>,
        next: crate::outbound_upgrade::OutboundUpgradeProgress,
        write: Option<crate::outbound_upgrade::OutboundUpgradeWrite>,
    ) -> Result<()> {
        self.do_commit_outbound_upgrade_page(expected, next, write)
            .await
    }

    async fn prepare_placement_page(&self) -> Result<crate::PlacementPage> {
        self.do_prepare_placement_page().await
    }

    async fn resolve_execution(&self, execution: &ExecutionRef) -> Result<Option<RunKey>> {
        self.do_resolve_execution(execution).await
    }

    async fn find_latest_run(
        &self,
        namespace_id: NamespaceId,
        workflow_id: &WorkflowId,
    ) -> Result<Option<RunKey>> {
        self.do_find_latest_run(namespace_id, workflow_id).await
    }

    async fn list_runs_for_namespace(&self, namespace_id: NamespaceId) -> Result<Vec<RunKey>> {
        self.do_list_runs_for_namespace(namespace_id).await
    }

    async fn load_run(&self, run_key: RunKey) -> Result<LoadedRun> {
        self.do_load_run(run_key).await
    }

    async fn load_outbound_source(&self, run: RunKey) -> Result<LoadedRun> {
        match self.do_load_run(run).await? {
            LoadedRun::Existing(state) => Ok(LoadedRun::Existing(state)),
            LoadedRun::Absent => {
                let mut permit = self.director.acquire(DbClass::Read).await?;
                let mut tx = permit
                    .connection()?
                    .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .await?;
                let row = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>(
                    "SELECT state_data,state_parts FROM outbound_retained_run WHERE run_key=$1",
                )
                .bind(run.0)
                .fetch_optional(&mut *tx)
                .await?;
                let result = match row {
                    Some((first, parts)) => LoadedRun::Existing(codec::decode_workflow_state(
                        run,
                        &run_value_parts::join_state(&mut tx, run, first, parts).await?,
                    )?),
                    None => LoadedRun::Absent,
                };
                tx.commit().await?;
                Ok(result)
            }
        }
    }

    async fn load_run_with_stats(&self, run_key: RunKey) -> Result<(LoadedRun, RunHistoryStats)> {
        self.do_load_run_with_stats(run_key).await
    }

    async fn read_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<HistoryEvent>> {
        self.do_read_history(run_key, after_event_id, limit).await
    }

    async fn read_history_event(
        &self,
        run_key: RunKey,
        event_id: i64,
    ) -> Result<Option<HistoryEvent>> {
        self.do_read_history_event(run_key, event_id).await
    }

    async fn read_attributed_history(
        &self,
        run_key: RunKey,
        after_event_id: i64,
        limit: usize,
    ) -> Result<Vec<AttributedHistoryEvent>> {
        self.do_read_attributed_history(run_key, after_event_id, limit)
            .await
    }

    async fn lookup_request_dedupe(
        &self,
        execution: &ExecutionRef,
        request_id: &RequestId,
    ) -> Result<Option<RequestRecord>> {
        self.do_lookup_request_dedupe(execution, request_id).await
    }

    async fn read_transition_audit(&self, run_key: RunKey) -> Result<Vec<TransitionAuditRecord>> {
        self.do_read_transition_audit(run_key).await
    }

    async fn has_open_pinned_workflows(
        &self,
        namespace_id: NamespaceId,
        version: &WorkerDeploymentVersionKey,
    ) -> Result<bool> {
        self.do_has_open_pinned_workflows(namespace_id, version)
            .await
    }

    async fn create_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule: WorkflowRuleRecord,
        max_rules: usize,
    ) -> Result<WorkflowRuleCreateResult> {
        self.do_create_workflow_rule(namespace_id, rule, max_rules)
            .await
    }

    async fn get_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<Option<WorkflowRuleRecord>> {
        self.do_get_workflow_rule(namespace_id, rule_id).await
    }

    async fn delete_workflow_rule(
        &self,
        namespace_id: NamespaceId,
        rule_id: &str,
    ) -> Result<WorkflowRuleDeleteResult> {
        self.do_delete_workflow_rule(namespace_id, rule_id).await
    }

    async fn list_workflow_rules(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkflowRuleRecord>> {
        self.do_list_workflow_rules(namespace_id).await
    }

    async fn commit_transition(
        &self,
        run_key: RunKey,
        transition: Transition,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        if crate::read_probe::reject_commit(run_key) {
            return Ok(CommitResult::Conflict {
                reason: "read-baseline injected OCC conflict".into(),
            });
        }
        self.do_commit_transition(run_key, transition, fence).await
    }

    async fn commit_transition_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        transition: Transition,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        if crate::read_probe::reject_commit(run_key) {
            return Ok(CommitResult::Conflict {
                reason: "read-baseline injected OCC conflict".into(),
            });
        }
        self.do_commit_transition_for_bundle(run_key, execution_home_bundle, transition, fence)
            .await
    }

    async fn delete_run_for_bundle(
        &self,
        run_key: RunKey,
        execution_home_bundle: ShardId,
        request: DeleteRunRequest,
        fence: WriteFence,
    ) -> Result<DeleteRunResult> {
        self.do_delete_run_for_bundle(run_key, execution_home_bundle, request, fence)
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
        self.do_materialize_reset_successor(
            base_run_key,
            fork_event_id,
            successor_run_id,
            expected_current,
            fence,
        )
        .await
    }

    async fn abandon_materialization(&self, run_key: RunKey, fence: WriteFence) -> Result<()> {
        self.do_abandon_materialization(run_key, &fence).await
    }

    async fn purge_run(&self, run_key: RunKey) -> Result<()> {
        self.do_purge_run(run_key).await
    }

    async fn list_run_bulk_writes(
        &self,
        shard_id: ShardId,
        after: Option<RunKey>,
        limit: usize,
    ) -> Result<Vec<crate::RunBulkWrite>> {
        self.do_list_run_bulk_writes(shard_id, after, limit).await
    }

    async fn reconcile_workflow_dispatch_run(
        &self,
        home: ShardId,
        run_key: RunKey,
        fence: WriteFence,
    ) -> Result<()> {
        self.do_reconcile_workflow_dispatch_run(home, run_key, &fence)
            .await
            .map_err(|error| {
                if error
                    .downcast_ref::<sqlx::Error>()
                    .is_some_and(Self::is_serialization_failure)
                {
                    crate::WorkflowDispatchRepairConflict.into()
                } else {
                    error
                }
            })
    }

    async fn list_workflow_dispatch_page(
        &self,
        range: &crate::WorkflowDiscoveryRange,
        after: Option<crate::WorkflowDispatchPosition>,
        limit: std::num::NonZeroU32,
    ) -> Result<crate::WorkflowDispatchPage> {
        self.do_list_workflow_dispatch_page(range, after, limit)
            .await
    }

    async fn list_workflow_dispatch_for_home(
        &self,
        home: ShardId,
        after: Option<RunKey>,
        limit: std::num::NonZeroU32,
    ) -> Result<Vec<RunKey>> {
        self.do_list_workflow_dispatch_for_home(home, after, limit)
            .await
    }

    async fn list_sticky_workflow_dispatch(
        &self,
        home: ShardId,
        after: Option<RunKey>,
        limit: std::num::NonZeroU32,
    ) -> Result<crate::StickyWorkflowDispatchPage> {
        self.do_list_sticky_workflow_dispatch(home, after, limit)
            .await
    }

    async fn list_dispatchable_workflow_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableWorkflowTask>> {
        self.do_list_dispatchable_workflow_tasks(queue, limit).await
    }

    async fn list_due_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.do_list_due_dispatchable_activity_tasks(queue, now, limit)
            .await
    }

    async fn list_all_dispatchable_activity_tasks(
        &self,
        queue: &QueueKey,
        limit: usize,
    ) -> Result<Vec<DispatchableActivityTask>> {
        self.do_list_all_dispatchable_activity_tasks(queue, limit)
            .await
    }

    async fn delete_activity_dispatch_if_matches(
        &self,
        candidate: &ActivityDispatchIdentity,
    ) -> Result<bool> {
        self.do_delete_activity_dispatch_if_matches(candidate).await
    }

    async fn persist_to_backlog(
        &self,
        entries: Vec<BacklogEntry>,
    ) -> std::result::Result<(), crate::BacklogPersistError> {
        self.do_persist_to_backlog(entries).await
    }

    async fn drain_backlog(&self, queue: &QueueKey, limit: usize) -> Result<Vec<BacklogEntry>> {
        self.do_drain_backlog(queue, limit).await
    }

    async fn dispose_workflow_backlog_page(
        &self,
        after: Option<Uuid>,
    ) -> Result<crate::WorkflowBacklogDisposal> {
        self.do_dispose_workflow_backlog_page(after).await
    }

    async fn backlog_stats_by_priority(
        &self,
        queue: &QueueKey,
    ) -> Result<std::collections::BTreeMap<i16, crate::BacklogBandStats>> {
        self.do_backlog_stats_by_priority(queue).await
    }

    async fn list_versioned_backlog_queue_keys(&self) -> Result<Vec<QueueKey>> {
        self.do_list_versioned_backlog_queue_keys().await
    }

    async fn list_due_timers(&self, now: OffsetDateTime, limit: usize) -> Result<Vec<DueTimer>> {
        self.do_list_due_timers(now, limit).await
    }

    async fn list_recovery_candidates_for_shard(
        &self,
        shard_id: ShardId,
        cursor: Option<&RecoveryCursor>,
        limit: usize,
    ) -> Result<RecoveryPage> {
        self.do_list_recovery_candidates_for_shard(shard_id, cursor, limit)
            .await
    }

    async fn list_due_dispatchable_activity_tasks_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        after: Option<&DueActivityDispatch>,
        limit: usize,
    ) -> Result<Vec<DueActivityDispatch>> {
        self.do_list_due_dispatchable_activity_tasks_for_shard(shard_id, now, after, limit)
            .await
    }

    async fn list_due_timers_for_shard(
        &self,
        shard_id: ShardId,
        now: OffsetDateTime,
        after: Option<&DueTimer>,
        limit: usize,
    ) -> Result<Vec<DueTimer>> {
        self.do_list_due_timers_for_shard(shard_id, now, after, limit)
            .await
    }

    async fn delete_due_timer_if_matches(
        &self,
        timer: &DueTimer,
        reason: crate::StaleTimer,
    ) -> Result<bool> {
        self.do_delete_due_timer_if_matches(timer, reason).await
    }
}

fn partition_for(run_key: RunKey, partition_count: u32) -> u32 {
    debug_assert!(partition_count > 0);
    // The projection partition is stable because it is derived from the spread
    // run UUID, not from insertion order.
    (run_key.0.as_u128() as u32) % partition_count
}

/// Encode an optional string for use in spread-key hash input.
///
/// Uses an explicit tag byte (0x00 for None, 0x01 for Some) so that
/// `None` and `Some("")` produce different hash inputs. This is
/// important because the type system does not prevent empty strings
/// in `DeploymentId` or `BuildId`.
fn option_key_part(value: Option<&str>) -> Vec<u8> {
    match value {
        Some(value) => {
            let mut bytes = Vec::with_capacity(value.len() + 1);
            bytes.push(1);
            bytes.extend_from_slice(value.as_bytes());
            bytes
        }
        None => vec![0],
    }
}

pub(crate) fn epoch_to_sql(epoch: ShardEpoch) -> Result<i64> {
    // DSQL stores epochs in BIGINT. Rejecting overflow here is preferable to a
    // connector/database error after a transaction has started.
    convert::i64_from_u64(epoch.0, "shard_lease.epoch")
}

pub(crate) fn epoch_from_sql(value: i64) -> Result<ShardEpoch> {
    // Negative epochs indicate corrupt storage or an incompatible manual edit.
    Ok(ShardEpoch(convert::u64_from_i64(
        value,
        "shard_lease.epoch",
    )?))
}

/// The fence read (`bundle-lease-fence`): the execution home's lease, read
/// `FOR KEY SHARE` as the first statement of a write's transaction. A takeover
/// or a release writes the lease's epoch, a key column through
/// `idx_shard_lease_epoch`, so DSQL commits at most one of the two
/// transactions; a renewal or a refresh writes no key column and conflicts with
/// neither.
pub(super) const FENCE_READ: &str =
    "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1 FOR KEY SHARE";

/// Whether the error is a lost fence, which no caller retries.
fn is_lease_fence_lost(error: &anyhow::Error) -> bool {
    error.downcast_ref::<LeaseFenceLost>().is_some()
}

impl DsqlRunRepository {
    /// Read the lease of a fenced write in `tx`, and return its loss when the
    /// lease no longer names the fence's owner at its epoch; the caller then
    /// rolls `tx` back and fails with it. An unfenced write reads nothing.
    pub(super) async fn check_fence(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        fence: &WriteFence,
        run_key: RunKey,
    ) -> Result<Option<LeaseFenceLost>> {
        let WriteFence::Lease(fence) = fence else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, (Option<String>, i64)>(FENCE_READ)
            .bind(Self::shard_id_to_uuid(fence.home))
            .fetch_optional(&mut **tx)
            .await?;
        let found = row
            .map(|(owner, epoch)| epoch_from_sql(epoch).map(|epoch| (owner, epoch)))
            .transpose()?;
        if let Some((Some(owner), epoch)) = &found
            && *owner == fence.owner
            && *epoch == fence.epoch
        {
            #[cfg(all(test, feature = "dsql-integration"))]
            pause_point::reached(pause_point::PauseKey::Write(run_key)).await;
            #[cfg(not(all(test, feature = "dsql-integration")))]
            let _ = run_key;
            return Ok(None);
        }
        Ok(Some(LeaseFenceLost {
            home: fence.home,
            epoch: fence.epoch,
            found: found.map(|(owner, epoch)| (owner.unwrap_or_default(), epoch)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::WriteFence;
    use std::{
        borrow::Cow,
        error::Error,
        fmt,
        sync::{Arc, Mutex},
    };

    use anyhow::{Result, anyhow, bail};
    use async_trait::async_trait;
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        HistoryEvent, HistoryEventKind, PendingWorkflowTask, TimerState, Transition, WorkflowState,
    };
    use tokeira_types::{
        ArchetypeId, BuildId, DeploymentId, ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo,
        NamespaceId, Payloads, QueueKey, RequestId, RunId, RunKey, SearchAttributes, ShardEpoch,
        StickyAffinity, TaskKind, TaskQueueName, TransitionSeq, VisibilityLifecycleState,
        WorkerIdentity, WorkflowId, WorkflowType,
    };

    use super::{
        ActivityDispatchRow, DsqlConnectionAcquirer, DsqlRunRepository, RenewDecision,
        activity_dispatch_from_row, classify_connection_error, classify_outcome,
        collect_dispatchable_workflow_tasks, decide_renew, dispatchable_workflow_task,
        epoch_from_sql, epoch_to_sql, extract_sqlstate, interpret_acquire, partition_for,
    };
    use crate::{
        BacklogPayload, CurrentExecutionConflictPolicy, DbClass, LeaseOutcome, LeaseRepository,
        ProjectionContext, RunRepository,
        dsql::{DsqlPermit, codec},
    };

    proptest! {
        #[test]
        fn shard_assignment_is_deterministic(run_key_bytes in any::<[u8; 16]>(), shard_count in 1u32..1024) {
            let run_key = RunKey(uuid::Uuid::from_bytes(run_key_bytes));
            let first = DsqlRunRepository::shard_for_run_key_with_count(run_key, shard_count).unwrap();
            let second = DsqlRunRepository::shard_for_run_key_with_count(run_key, shard_count).unwrap();
            prop_assert_eq!(first, second);
            prop_assert_eq!(first.0, (run_key.0.as_u128() as u32) % shard_count);
        }

        #[test]
        fn configured_projection_partition_count_bounds_written_partitions(
            run_key_bytes in any::<[u8; 16]>(),
            partition_count in 1u32..64,
        ) {
            let run_key = RunKey(uuid::Uuid::from_bytes(run_key_bytes));
            let partition_id = partition_for(run_key, partition_count);

            prop_assert!(partition_id < partition_count);
        }

        #[test]
        fn shard_id_uuid_encoding_is_deterministic(shard_id in any::<u32>()) {
            let first = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(shard_id));
            let second = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(shard_id));
            prop_assert_eq!(first, second);
        }

        #[test]
        fn shard_id_uuid_encoding_spreads_across_keyspace(a in 0u32..1024, b in 0u32..1024) {
            prop_assume!(a != b);
            let uuid_a = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(a));
            let uuid_b = DsqlRunRepository::shard_id_to_uuid(tokeira_types::ShardId(b));
            // Different shard IDs must produce different UUIDs.
            prop_assert_ne!(uuid_a, uuid_b);
        }

        #[test]
        fn table_key_helpers_are_deterministic(seed in any::<u128>(), seq in any::<u64>()) {
            let namespace_id = NamespaceId(uuid::Uuid::from_u128(seed));
            let workflow_id = WorkflowId(format!("workflow-{seed}"));
            let request_id = RequestId(format!("request-{seed}"));
            let run_key = RunKey(uuid::Uuid::from_u128(seed));
            let payload = BacklogPayload::Workflow {
                logical_seq: tokeira_types::LogicalTaskSeq(seq),
            };

            prop_assert_eq!(
                DsqlRunRepository::current_execution_key(namespace_id, &workflow_id),
                DsqlRunRepository::current_execution_key(namespace_id, &workflow_id)
            );
            prop_assert_eq!(
                DsqlRunRepository::request_dedupe_key(namespace_id, &workflow_id, &request_id),
                DsqlRunRepository::request_dedupe_key(namespace_id, &workflow_id, &request_id)
            );
            prop_assert_eq!(
                DsqlRunRepository::dispatch_backlog_key(
                    7,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    Some("deployment"),
                    Some("build"),
                    run_key,
                    &payload,
                ),
                DsqlRunRepository::dispatch_backlog_key(
                    7,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    Some("deployment"),
                    Some("build"),
                    run_key,
                    &payload,
                )
            );
            prop_assert_eq!(
                DsqlRunRepository::activity_dispatch_key(RunKey(uuid::Uuid::from_u128(seed)), "activity"),
                DsqlRunRepository::activity_dispatch_key(RunKey(uuid::Uuid::from_u128(seed)), "activity")
            );
        }

        #[test]
        fn backlog_key_includes_full_queue_identity(
            namespace in any::<u128>(),
            seq in any::<u64>(),
        ) {
            let namespace_id = NamespaceId(uuid::Uuid::from_u128(namespace));
            let run_key = RunKey(uuid::Uuid::from_u128(namespace));
            let payload = BacklogPayload::Workflow {
                logical_seq: tokeira_types::LogicalTaskSeq(seq),
            };
            let base = DsqlRunRepository::dispatch_backlog_key(
                1,
                namespace_id,
                "queue",
                TaskKind::Workflow,
                None,
                None,
                run_key,
                &payload,
            );

            prop_assert_ne!(
                base,
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Activity,
                    None,
                    None,
                    run_key,
                    &payload,
                )
            );
            prop_assert_ne!(
                base,
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    Some("deployment"),
                    None,
                    run_key,
                    &payload,
                )
            );
            prop_assert_ne!(
                base,
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    None,
                    Some("build"),
                    run_key,
                    &payload,
                )
            );
        }

        // Feature: task-queue-priority-fairness, Property 9
        #[test]
        fn backlog_key_uses_logical_task_identity(
            seed in any::<u128>(),
            first_seq in any::<u64>(),
            second_seq in any::<u64>(),
        ) {
            prop_assume!(first_seq != second_seq);
            let namespace_id = NamespaceId(uuid::Uuid::from_u128(seed));
            let run_key = RunKey(uuid::Uuid::from_u128(seed));
            let first = BacklogPayload::Workflow {
                logical_seq: tokeira_types::LogicalTaskSeq(first_seq),
            };
            let second = BacklogPayload::Workflow {
                logical_seq: tokeira_types::LogicalTaskSeq(second_seq),
            };

            prop_assert_ne!(
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    None,
                    None,
                    run_key,
                    &first,
                ),
                DsqlRunRepository::dispatch_backlog_key(
                    1,
                    namespace_id,
                    "queue",
                    TaskKind::Workflow,
                    None,
                    None,
                    run_key,
                    &second,
                )
            );
        }

        #[test]
        fn activity_dispatch_key_includes_activity_identity(seed in any::<u128>()) {
            let run_key = RunKey(uuid::Uuid::from_u128(seed));
            prop_assert_ne!(
                DsqlRunRepository::activity_dispatch_key(run_key, "activity-a"),
                DsqlRunRepository::activity_dispatch_key(run_key, "activity-b")
            );
        }

        #[test]
        fn codec_round_trips_core_persistence_types(seed in 1u64..1_000_000) {
            let run_key = RunKey(uuid::Uuid::from_u128(seed as u128));
            let workflow_state = sample_state(run_key);
            let history_events = vec![sample_history_event(seed as i64)];
            let timer_state = sample_timer_state(seed);
            let projection_context = sample_projection_context(&workflow_state);

            let run_key = workflow_state.run_key;
            prop_assert_eq!(
                codec::decode_workflow_state(
                    run_key,
                    &codec::encode_workflow_state(&workflow_state).unwrap(),
                )
                .unwrap(),
                workflow_state
            );
            prop_assert_eq!(
                codec::decode_history_events(
                    run_key,
                    &codec::encode_history_events(&history_events).unwrap(),
                )
                .unwrap(),
                history_events
            );
            prop_assert_eq!(
                codec::decode_timer_state(&codec::encode_timer_state(&timer_state).unwrap()).unwrap(),
                timer_state
            );
            prop_assert_eq!(
                codec::decode_projection_context(&codec::encode_projection_context(&projection_context).unwrap()).unwrap(),
                projection_context
            );
        }

        #[test]
        fn sticky_dispatch_uses_pending_deadline_without_expiring_affinity(
            delta_seconds in -100i64..100i64
        ) {
            // Feature: api-conformance-client-misc, Property 6: derived dispatch and query deadlines
            let now = fixed_now();
            let mut state = sample_state(RunKey::new());
            state.sticky = Some(StickyAffinity {
                sticky_queue: TaskQueueName("sticky".into()),
                schedule_to_start_timeout: Duration::seconds(5),
                worker_identity: WorkerIdentity("sticky-worker".to_owned()),
            });
            let deadline = now + Duration::seconds(delta_seconds);
            state
                .pending_workflow_task
                .as_mut()
                .expect("sample has a pending workflow task")
                .schedule_to_start_deadline = Some(deadline);

            let task = dispatchable_workflow_task(&state).expect("task is dispatchable");
            prop_assert_eq!(
                task.sticky_preferred,
                Some(WorkerIdentity("sticky-worker".to_owned()))
            );
            prop_assert_eq!(task.sticky_deadline, Some(deadline));
            prop_assert_eq!(task.queue.task_queue, TaskQueueName("sticky".into()));
            prop_assert!(task.normal_queue.is_some());
        }

        #[test]
        fn result_limit_invariant_for_collect_helpers(limit in 0usize..16usize) {
            let rows = vec![
                encoded_workflow_row(sample_state(RunKey::new())),
                encoded_workflow_row(sample_state(RunKey::new())),
                encoded_workflow_row(sample_state(RunKey::new())),
            ];
            prop_assert!(collect_dispatchable_workflow_tasks(rows, None, limit).unwrap().len() <= limit);
        }

        #[test]
        fn shard_epoch_round_trip(value in 1u64..=i64::MAX as u64) {
            let epoch = ShardEpoch(value);

            prop_assert_eq!(epoch_from_sql(epoch_to_sql(epoch).unwrap()).unwrap(), epoch);
        }

        #[test]
        fn acquire_interpretation_accepts_valid_outcomes(epoch in 1i64..i64::MAX) {
            prop_assert_eq!(
                interpret_acquire(1, 0, Some(1), None).unwrap(),
                LeaseOutcome::Acquired { epoch: ShardEpoch(1) }
            );
            prop_assert_eq!(
                interpret_acquire(0, 1, Some(epoch), None).unwrap(),
                LeaseOutcome::Acquired { epoch: ShardEpoch(epoch as u64) }
            );
            prop_assert_eq!(
                interpret_acquire(0, 0, None, Some(("owner".to_owned(), epoch))).unwrap(),
                LeaseOutcome::Rejected {
                    current_owner: "owner".to_owned(),
                    current_epoch: ShardEpoch(epoch as u64),
                }
            );
        }

        #[test]
        fn active_lease_rejection_preserves_owner_and_epoch(owner in "\\PC{1,64}", epoch in 1i64..i64::MAX) {
            let outcome = interpret_acquire(0, 0, None, Some((owner.clone(), epoch))).unwrap();

            prop_assert_eq!(
                outcome,
                LeaseOutcome::Rejected {
                    current_owner: owner,
                    current_epoch: ShardEpoch(epoch as u64),
                }
            );
        }

        #[test]
        fn renew_decision_requires_owner_and_epoch_match(
            current_owner in "\\PC{1,64}",
            caller_owner in "\\PC{1,64}",
            current_epoch in 1i64..i64::MAX,
            caller_epoch in 1i64..i64::MAX,
        ) {
            let decision = decide_renew(
                Some((current_owner.as_str(), current_epoch)),
                caller_owner.as_str(),
                caller_epoch,
            )
            .unwrap();

            if current_owner == caller_owner && current_epoch == caller_epoch {
                prop_assert_eq!(decision, RenewDecision::Renew);
            } else {
                prop_assert_eq!(
                    decision,
                    RenewDecision::Reject {
                        current_owner,
                        current_epoch: ShardEpoch(current_epoch as u64),
                    }
                );
            }
        }
    }

    #[test]
    fn shard_id_uuid_encoding_changed_from_old_sha256_scheme() {
        // This test verifies the BLAKE3-based shard UUID differs from the old
        // SHA-256 scheme. It imports sha2 directly — if sha2 is ever removed
        // from tokeira-storage's dependencies, replace this with a hardcoded
        // expected UUID value.
        use sha2::{Digest, Sha256};

        let shard_id = tokeira_types::ShardId(42);
        let mut hasher = Sha256::new();
        hasher.update(b"tokeira-shard-id:");
        hasher.update(shard_id.0.to_le_bytes());
        let hash = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash[..16]);
        let old_uuid = uuid::Uuid::from_bytes(bytes);

        assert_ne!(DsqlRunRepository::shard_id_to_uuid(shard_id), old_uuid);
    }

    #[test]
    fn shard_assignment_rejects_zero_count() {
        assert!(DsqlRunRepository::shard_for_run_key_with_count(RunKey::new(), 0).is_err());
    }

    #[test]
    fn constructor_rejects_zero_shard_count() {
        let recorder = Arc::new(RecordingAcquirer::default());
        assert!(
            DsqlRunRepository::new_with_acquirer(
                recorder,
                0,
                16,
                CurrentExecutionConflictPolicy::Reject,
                Duration::seconds(30),
            )
            .is_err()
        );
    }

    #[test]
    fn constructor_rejects_non_positive_lease_duration() {
        let recorder = Arc::new(RecordingAcquirer::default());
        assert!(
            DsqlRunRepository::new_with_acquirer(
                recorder,
                4,
                16,
                CurrentExecutionConflictPolicy::Reject,
                Duration::ZERO,
            )
            .is_err()
        );
    }

    #[test]
    fn epoch_conversion_rejects_out_of_range_values() {
        assert!(epoch_to_sql(ShardEpoch(i64::MAX as u64 + 1)).is_err());
        assert!(epoch_from_sql(-1).is_err());
    }

    #[test]
    fn acquire_interpretation_rejects_invalid_sql_outcomes() {
        assert!(interpret_acquire(1, 1, Some(1), None).is_err());
        assert!(interpret_acquire(0, 0, None, None).is_err());
        assert!(interpret_acquire(1, 0, None, None).is_err());
        assert!(interpret_acquire(0, 1, None, None).is_err());
        assert!(interpret_acquire(1, 0, Some(2), None).is_err());
        assert!(interpret_acquire(0, 1, Some(-1), None).is_err());
        assert!(interpret_acquire(0, 0, None, Some(("owner".to_owned(), -1))).is_err());
    }

    #[test]
    fn absent_lease_renewal_is_rejected() {
        assert_eq!(
            decide_renew(None, "owner", 1).unwrap(),
            RenewDecision::Reject {
                current_owner: String::new(),
                current_epoch: ShardEpoch::ZERO,
            }
        );
    }

    #[test]
    fn serialization_failure_detection_is_false_for_non_database_errors() {
        assert!(!DsqlRunRepository::is_serialization_failure(
            &sqlx::Error::RowNotFound
        ));
    }

    #[test]
    fn serialization_failure_detection_matches_sqlstate_40001() {
        let retryable = sqlx::Error::Database(Box::new(TestDatabaseError {
            message: "test database error",
            code: Some("40001"),
        }));
        let other = sqlx::Error::Database(Box::new(TestDatabaseError {
            message: "test database error",
            code: Some("23505"),
        }));

        assert!(DsqlRunRepository::is_serialization_failure(&retryable));
        assert!(!DsqlRunRepository::is_serialization_failure(&other));
    }

    #[test]
    fn outcome_and_error_classification_helpers_match_contract() {
        let ok: Result<()> = Ok(());
        let generic_error: Result<()> = Err(anyhow!("boom"));
        let serialization_error: Result<()> =
            Err(sqlx::Error::Database(Box::new(TestDatabaseError {
                message: "test database error",
                code: Some("40001"),
            }))
            .into());

        assert_eq!(classify_outcome(&ok), "success");
        assert_eq!(classify_outcome(&generic_error), "error");
        assert_eq!(classify_outcome(&serialization_error), "conflict");
    }

    #[test]
    fn sqlstate_extraction_returns_database_error_code() {
        let error = anyhow!(sqlx::Error::Database(Box::new(TestDatabaseError {
            message: "test database error",
            code: Some("23505"),
        })));

        assert_eq!(extract_sqlstate(&error), Some("23505".to_owned()));
    }

    #[test]
    fn connection_error_classification_maps_io_kinds() {
        let timeout = anyhow!(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timeout",
        )));
        let refused = anyhow!(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        )));

        assert_eq!(classify_connection_error(&timeout), Some("timeout"));
        assert_eq!(classify_connection_error(&refused), Some("refused"));
    }

    // Feature: bundle-lease-fence, Property 1: No stale owner commits
    #[test]
    fn the_fence_read_is_a_key_share_read_of_the_lease() {
        assert_eq!(
            super::FENCE_READ,
            "SELECT owner, epoch FROM shard_lease WHERE shard_id = $1 FOR KEY SHARE"
        );
    }

    #[test]
    fn dispatch_envelope_preserves_sticky_affinity_and_pending_deadline() {
        let now = fixed_now();
        let mut state = sample_state(RunKey::new());
        let deadline = now - Duration::seconds(30);
        state.sticky = Some(StickyAffinity {
            sticky_queue: TaskQueueName("sticky".into()),
            schedule_to_start_timeout: Duration::seconds(5),
            worker_identity: WorkerIdentity("worker".to_owned()),
        });
        state
            .pending_workflow_task
            .as_mut()
            .expect("sample has a pending workflow task")
            .schedule_to_start_deadline = Some(deadline);

        let task = dispatchable_workflow_task(&state).expect("task is dispatchable");
        assert_eq!(
            task.sticky_preferred,
            Some(WorkerIdentity("worker".to_owned()))
        );
        assert_eq!(task.sticky_deadline, Some(deadline));
        assert_eq!(task.queue.task_queue, TaskQueueName("sticky".into()));
        assert_eq!(
            task.normal_queue
                .as_ref()
                .expect("sticky delivery has fallback")
                .task_queue,
            state.task_queue
        );
    }

    /// The hot-state upsert stores the recovery flag on insert and on update, in
    /// the one statement that writes the row (recovery-index Requirement 1).
    #[test]
    fn workflow_hot_upsert_writes_the_recovery_flag() {
        let sql = super::commit::WORKFLOW_HOT_UPSERT_SQL;
        let (insert, update) = sql.split_once("ON CONFLICT").expect("upsert");
        assert!(insert.contains("recovery_needed"));
        assert!(insert.contains("$8"));
        assert!(update.contains("recovery_needed = EXCLUDED.recovery_needed"));
        assert_eq!(sql.matches(';').count(), 0);
    }

    /// The hot-state upsert writes the state's count on insert and on update, so a
    /// state that shrinks to the part size clears it (`large-value-parts`).
    #[test]
    fn workflow_hot_upsert_writes_the_state_count() {
        let sql = super::commit::WORKFLOW_HOT_UPSERT_SQL;
        let (insert, update) = sql.split_once("ON CONFLICT").expect("upsert");
        assert!(insert.contains("state_parts"));
        assert!(insert.contains("$9"));
        assert!(update.contains("state_parts = EXCLUDED.state_parts"));
    }

    #[test]
    fn workflow_dispatch_collects_only_scheduled_unstarted_tasks() {
        let run_key = RunKey::new();
        let eligible = sample_state(run_key);
        let mut no_task = sample_state(RunKey::new());
        no_task.pending_workflow_task = None;
        let mut started = sample_state(RunKey::new());
        if let Some(task) = started.pending_workflow_task.as_mut() {
            task.started_event_id = Some(10);
        }

        let tasks = collect_dispatchable_workflow_tasks(
            vec![
                encoded_workflow_row(no_task),
                encoded_workflow_row(started),
                encoded_workflow_row(eligible),
            ],
            None,
            10,
        )
        .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].run_key, run_key);
        assert_eq!(tasks[0].queue.task_kind, TaskKind::Workflow);
    }

    #[test]
    fn activity_dispatch_row_preserves_field_fidelity() {
        let run_key = RunKey::new();
        let namespace_id = NamespaceId::new();
        let row: ActivityDispatchRow = (
            run_key.0,
            "activity".to_owned(),
            namespace_id.0,
            "queue".to_owned(),
            TaskKind::Activity.to_db_smallint(),
            Some("deployment".to_owned()),
            Some("build".to_owned()),
            42,
            3,
            17,
            5,
            None,
            OffsetDateTime::from_unix_timestamp(1_700_000_777).unwrap(),
        );

        let due = activity_dispatch_from_row(row).unwrap();
        assert_eq!(
            due.dispatch_at,
            OffsetDateTime::from_unix_timestamp(1_700_000_777).unwrap()
        );
        let task = due.task;

        assert_eq!(task.run_key, run_key);
        assert_eq!(task.activity_id, "activity");
        assert_eq!(task.schedule_event_id, 42);
        assert_eq!(task.attempt, 3);
        assert_eq!(task.dispatch_revision, 17);
        assert_eq!(task.stamp, 5);
        assert_eq!(task.queue.namespace_id, namespace_id);
        assert_eq!(task.queue.task_kind, TaskKind::Activity);
        assert_eq!(
            task.queue.deployment,
            Some(DeploymentId("deployment".to_owned()))
        );
        assert_eq!(task.queue.build_id, Some(BuildId("build".to_owned())));
    }

    #[tokio::test]
    async fn read_operations_request_read_class() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));
        let run_key = RunKey::new();
        let execution = ExecutionRef {
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".to_owned()),
            run_id: None,
        };

        let _ = repo.load_run(run_key).await;
        let _ = repo.resolve_execution(&execution).await;
        let _ = repo
            .find_latest_run(execution.namespace_id, &execution.workflow_id)
            .await;
        let _ = repo.read_history(run_key, 0, 1).await;
        let _ = repo
            .lookup_request_dedupe(&execution, &tokeira_types::RequestId("request".to_owned()))
            .await;
        let _ = repo.read_transition_audit(run_key).await;

        assert_eq!(
            recorder.classes(),
            vec![
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
                DbClass::Read,
            ]
        );
    }

    #[tokio::test]
    async fn write_operations_request_commit_class() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));
        let run_key = RunKey::new();

        let _ = repo
            .commit_transition(run_key, sample_transition(run_key), WriteFence::Unfenced)
            .await;
        let _ = repo
            .materialize_reset_successor(run_key, 1, RunId::new(), None, WriteFence::Unfenced)
            .await;

        assert_eq!(recorder.classes(), vec![DbClass::Commit, DbClass::Commit]);
    }

    #[tokio::test]
    async fn lease_operations_request_control_class() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));

        let _ = repo
            .try_acquire_bundle(
                tokeira_types::ShardId(1),
                "owner".to_owned(),
                "127.0.0.1:7233".to_owned(),
            )
            .await;
        let _ = repo
            .renew_bundle(
                tokeira_types::ShardId(1),
                "owner".to_owned(),
                ShardEpoch(1),
                "127.0.0.1:7233".to_owned(),
            )
            .await;

        assert_eq!(recorder.classes(), vec![DbClass::Control, DbClass::Control]);
    }

    #[test]
    fn read_history_fetches_batches_a_page_at_a_time() {
        let sql = super::load::READ_HISTORY_BATCHES_SQL;
        assert!(sql.contains("ORDER BY first_event_id ASC"));
        assert!(sql.trim_end().ends_with("LIMIT $3"));
    }

    #[tokio::test]
    async fn read_history_zero_limit_does_not_acquire_connection() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));

        let result = repo.read_history(RunKey::new(), 0, 0).await.unwrap();

        assert!(result.is_empty());
        assert!(recorder.classes().is_empty());
    }

    #[tokio::test]
    async fn side_table_queries_zero_limit_do_not_acquire_connection() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(Arc::clone(&recorder));
        let queue = QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".to_owned()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        let shard_id = tokeira_types::ShardId(0);

        assert!(
            repo.list_dispatchable_workflow_tasks(&queue, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_all_dispatchable_activity_tasks(&queue, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_due_timers(fixed_now(), 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_due_dispatchable_activity_tasks_for_shard(shard_id, fixed_now(), None, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.list_due_timers_for_shard(shard_id, fixed_now(), None, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(recorder.classes().is_empty());
    }

    #[derive(Debug, Default)]
    struct RecordingAcquirer {
        classes: Mutex<Vec<DbClass>>,
    }

    impl RecordingAcquirer {
        fn classes(&self) -> Vec<DbClass> {
            self.classes.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DsqlConnectionAcquirer for RecordingAcquirer {
        async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
            self.classes.lock().unwrap().push(class);
            bail!("test acquirer has no database connection")
        }
    }

    fn test_repo(recorder: Arc<RecordingAcquirer>) -> DsqlRunRepository {
        DsqlRunRepository::new_with_acquirer(
            recorder,
            4,
            16,
            CurrentExecutionConflictPolicy::Reject,
            Duration::seconds(30),
        )
        .unwrap()
    }

    #[test]
    fn pair_commit_future_is_no_larger_than_single_commit() {
        let recorder = Arc::new(RecordingAcquirer::default());
        let repo = test_repo(recorder.clone());
        let closing = sample_transition(RunKey::new());
        let successor = sample_transition(RunKey::new());
        let single = repo.do_commit_transition(
            closing.next_state.run_key,
            closing.clone(),
            WriteFence::Unfenced,
        );
        let single_bytes = std::mem::size_of_val(&single);
        drop(single);
        let pair = repo.do_commit_transition_pair(
            closing.next_state.run_key,
            closing.clone(),
            successor.next_state.run_key,
            successor.clone(),
            WriteFence::Unfenced,
        );
        let pair_bytes = std::mem::size_of_val(&pair);
        drop(pair);
        assert!(
            pair_bytes <= single_bytes,
            "pair implementation future ({pair_bytes} bytes) exceeds single commit ({single_bytes} bytes)"
        );

        // async_trait already returns a box: measure its allocation, not the
        // pointer. Constructing these futures must not poll or acquire a connection.
        let single = repo.commit_transition(
            closing.next_state.run_key,
            closing.clone(),
            WriteFence::Unfenced,
        );
        let single_bytes = std::mem::size_of_val(single.as_ref().get_ref());
        drop(single);
        let pair = repo.commit_transition_pair(
            closing.next_state.run_key,
            closing,
            successor.next_state.run_key,
            successor,
            WriteFence::Unfenced,
        );
        let pair_bytes = std::mem::size_of_val(pair.as_ref().get_ref());
        drop(pair);
        assert!(
            pair_bytes <= single_bytes,
            "pair trait future ({pair_bytes} bytes) exceeds single commit ({single_bytes} bytes)"
        );
        assert!(recorder.classes().is_empty());
    }

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn encoded_workflow_row(state: WorkflowState) -> (uuid::Uuid, Vec<u8>) {
        (
            state.run_key.0,
            codec::encode_workflow_state(&state).unwrap(),
        )
    }

    fn sample_transition(run_key: RunKey) -> Transition {
        Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: sample_state(run_key),
            history_events: Default::default(),
            event_principals: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            events_numbered_at_close: 0,
            growth_limits: None,
        }
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            used_worker_deployment_versions: Some(Vec::new()),
            completed_update_count: 0,
            signal_count: 0,
            outbound: Some(Default::default()),
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".to_owned()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("workflow-type".to_owned()),
            task_queue: TaskQueueName("queue".to_owned()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                advice: Default::default(),
                task_type: tokeira_kernel::WorkflowTaskType::Normal,
                schedule_to_start_deadline: None,
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
                target_worker_deployment_version_changed: false,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: None,
            original_execution_run_id: None,
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            history_admitted_updates: Default::default(),
            completed_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_info: None,
            worker_deployment_name: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
            auto_reset_points: Vec::new(),
        }
    }

    fn sample_history_event(event_id: i64) -> HistoryEvent {
        HistoryEvent {
            event_id,
            happened_at: fixed_now(),
            kind: HistoryEventKind::WorkflowExecutionSignaled {
                signal_name: format!("signal-{event_id}"),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request_id: format!("request-{event_id}"),
                identity: Some("tester".to_owned()),
            },
        }
    }

    fn sample_timer_state(seed: u64) -> TimerState {
        TimerState {
            timer_id: format!("timer-{seed}"),
            started_event_id: i64::try_from(seed).unwrap(),
            fire_at: fixed_now() + Duration::seconds(i64::try_from(seed % 60).unwrap()),
        }
    }

    fn sample_projection_context(state: &WorkflowState) -> ProjectionContext {
        ProjectionContext {
            archetype_id: ArchetypeId::WORKFLOW,
            namespace_id: state.namespace_id,
            business_id: state.workflow_id.0.clone(),
            authority_epoch: 0,
            status_keyword: format!("{:?}", state.status),
            lifecycle_state: if state.status.is_open() {
                VisibilityLifecycleState::Open
            } else {
                VisibilityLifecycleState::Closed
            },
            workflow_id: state.workflow_id.clone(),
            run_id: state.run_id,
            workflow_type: state.workflow_type.clone(),
            task_queue: state.task_queue.clone(),
            execution_status: state.status,
            start_time: state.started_at,
            update_time: state.closed_at.unwrap_or(state.started_at),
            // v1.31.0 ExecutionTime = StartTime + FirstWorkflowTaskBackoff
            // (mutable_state_impl.go:2859); tokeira carries that backoff (client
            // start delay / workflow-retry backoff) as `workflow_start_delay`.
            execution_time: Some(state.started_at + state.workflow_start_delay.unwrap_or_default()),
            close_time: state.closed_at,
            history_length: state.last_event_id,
            execution_duration: None,
            state_transition_count: state.transition_seq.0 as i64,
            transition_count: state.transition_seq.0 as i64,
            history_size_bytes: 0,
            parent_workflow_id: state.parent_workflow_id.clone(),
            parent_run_id: state.parent_run_id,
            root_workflow_id: state
                .root_workflow_id
                .clone()
                .or_else(|| Some(state.workflow_id.clone())),
            root_run_id: state.root_run_id.or(Some(state.run_id)),
            search_attr_generation: state.transition_seq.0,
            memo: state.memo.clone(),
            search_attributes: state.search_attributes.clone(),
        }
    }

    #[derive(Debug)]
    pub(super) struct TestDatabaseError {
        pub(super) message: &'static str,
        pub(super) code: Option<&'static str>,
    }

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.message)
        }
    }

    impl Error for TestDatabaseError {}

    impl sqlx::error::DatabaseError for TestDatabaseError {
        fn message(&self) -> &str {
            self.message
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }
}
