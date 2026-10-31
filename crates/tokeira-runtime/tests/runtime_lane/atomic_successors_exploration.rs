//! Exploration of committed closes whose successors can be lost.
//!
//! These assert the fixed contract on the real kernel and in-memory store:
//! a committed close already has its successor. Temporal v1.31.0 passes both
//! runs to persistence (service/history/api/respondworkflowtaskcompleted/api.go:608-626)
//! and writes them in one transaction (common/persistence/sql/execution.go:401-439).

use std::sync::{Arc, Mutex};

use anyhow::{Result, bail};
use async_trait::async_trait;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, LoadedRun, RetryState, StartRequest, Transition,
    WorkflowCommand, WorkflowIdConflictPolicy, WorkflowState, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, StartWorkflowResult, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{
    ActivityDispatchIdentity, BacklogEntry, BacklogPersistError, CommitResult, DeleteRunRequest,
    DeleteRunResult, DispatchableActivityTask, DispatchableWorkflowTask, DueActivityDispatch,
    DueTimer, InMemoryStore, PairCommitResult, PlacementPage, RecoveryCursor, RecoveryPage,
    RequestRecord, RunBulkWrite, RunHistoryStats, RunRepository, StaleTimer, TransitionAuditRecord,
    WorkflowDiscoveryRange, WorkflowDispatchPage, WorkflowDispatchPosition, WriteFence,
};
use tokeira_types::{
    ExecutionRef, ExecutionStatus, Memo, NamespaceId, Payloads, QueueKey, RequestContext,
    RequestId, RetryPolicy, RunId, RunKey, SearchAttributes, ShardId, WorkerIdentity, WorkflowId,
};
use tokio::sync::oneshot;
use tracing::instrument::WithSubscriber;

use super::{applied_state, queue, retryable_app_failure, start_request};

// Feature: atomic-successors, exploration 1: Continue-as-new survives a separate start failure.
#[tokio::test(start_paused = true)]
async fn continue_as_new_has_successor_when_separate_start_fails() -> Result<()> {
    let repo = Arc::new(ExplorationRepo::default());
    let runtime = runtime(repo.clone());
    let request = request("continue-as-new");
    let (predecessor, mut completion) = start_and_poll(&runtime, &request).await?;
    let successor_id = RunId::new();
    let successor_key = RunKey::derive(request.namespace_id, &request.workflow_id, successor_id);
    let failed_start = repo.fail_start(successor_key);
    completion.commands = vec![WorkflowCommand::ContinueAsNew {
        new_run_id: successor_id,
        workflow_type: request.workflow_type.clone(),
        task_queue: request.task_queue.clone(),
        input: Payloads::default(),
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: request.workflow_task_timeout,
        retry_policy: None,
        header: None,
        initial_versioning_behavior: Default::default(),
        successor_versioning_info: None,
    }];

    let result = runtime.complete_workflow_task(completion).await?;
    assert_eq!(
        applied_state(&result).status,
        ExecutionStatus::ContinuedAsNew
    );
    let closed = existing(&repo.inner, predecessor.run_key).await?;
    assert_eq!(closed.status, ExecutionStatus::ContinuedAsNew);
    assert!(closed.closed_at.is_some());
    assert!(repo.inner.read_history(predecessor.run_key, 0, 16).await?.iter().any(|event| {
        matches!(event.kind, HistoryEventKind::WorkflowExecutionContinuedAsNew { new_run_id, .. }
            if new_run_id == successor_id)
    }));

    // Wait for the injected refusal, not merely for the close's reply: today's
    // continue-as-new start is spawned. An atomic commit needs no separate start.
    if matches!(repo.inner.load_run(successor_key).await?, LoadedRun::Absent) {
        failed_start.await?;
    }
    assert!(
        matches!(
            repo.inner.load_run(successor_key).await?,
            LoadedRun::Existing(_)
        ),
        "criterion 1.1: continue-as-new close committed, but successor is absent after its separate start failed"
    );
    assert_successor(&repo.inner, &predecessor, successor_id).await?;
    Ok(())
}

// Feature: atomic-successors, exploration 2: Retry successor survives node drain.
#[tokio::test(start_paused = true)]
async fn retry_has_successor_when_node_is_draining() -> Result<()> {
    let repo = Arc::new(ExplorationRepo::default());
    let runtime = runtime(repo.clone());
    let mut request = request("retry-drain");
    request.retry_policy = Some(RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 1.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 3,
        non_retryable_error_types: Vec::new(),
    });
    let (predecessor, mut completion) = start_and_poll(&runtime, &request).await?;
    completion.commands = vec![WorkflowCommand::FailWorkflow {
        failure: retryable_app_failure("RetryableError"),
    }];
    runtime.runtime_drain().begin();
    assert!(runtime.runtime_drain().is_draining());

    let result = runtime
        .complete_workflow_task(completion)
        .with_subscriber(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::ERROR)
                .with_ansi(false)
                .without_time()
                .with_test_writer()
                .finish(),
        )
        .await?;
    assert_eq!(applied_state(&result).status, ExecutionStatus::Failed);
    let closed = existing(&repo.inner, predecessor.run_key).await?;
    assert_eq!(closed.status, ExecutionStatus::Failed);
    assert!(closed.closed_at.is_some());
    let history = repo.inner.read_history(predecessor.run_key, 0, 16).await?;
    let successor_id = history
        .iter()
        .find_map(|event| match event.kind {
            HistoryEventKind::WorkflowExecutionFailed {
                retry_state: RetryState::InProgress,
                new_execution_run_id,
                ..
            } => new_execution_run_id,
            _ => None,
        })
        .expect("retry-eligible close must name a successor with retry state InProgress");
    let successor_key = RunKey::derive(request.namespace_id, &request.workflow_id, successor_id);
    assert!(
        matches!(
            repo.inner.load_run(successor_key).await?,
            LoadedRun::Existing(_)
        ),
        "criterion 1.2: retry-eligible close committed while draining, but retry successor is absent"
    );
    assert_successor(&repo.inner, &predecessor, successor_id).await?;
    assert_eq!(existing(&repo.inner, successor_key).await?.attempt, 2);
    Ok(())
}

// Feature: atomic-successors, exploration 3: Cron successor is current before a racing client start.
#[tokio::test(start_paused = true)]
async fn cron_successor_is_current_before_racing_client_start() -> Result<()> {
    let repo = Arc::new(ExplorationRepo::default());
    let completing_runtime = runtime(repo.clone());
    // Independent command processors let the client run while the closing
    // processor is held after persistence, even if both run keys hash alike.
    let client_runtime = runtime(repo.clone());
    let mut request = request("cron-client-race");
    request.cron_schedule = Some("* * * * *".into());
    let (predecessor, mut completion) = start_and_poll(&completing_runtime, &request).await?;
    completion.commands = vec![WorkflowCommand::CompleteWorkflow {
        result: Payloads::default(),
    }];
    let (committed, resume) = repo.pause_after_close(predecessor.run_key);
    let completion_task =
        tokio::spawn(async move { completing_runtime.complete_workflow_task(completion).await });
    committed.await?;
    let closed = existing(&repo.inner, predecessor.run_key).await?;
    assert_eq!(closed.status, ExecutionStatus::Completed);
    assert!(closed.closed_at.is_some());
    let history = repo.inner.read_history(predecessor.run_key, 0, 16).await?;
    let successor_id = history
        .iter()
        .find_map(|event| match event.kind {
            HistoryEventKind::WorkflowExecutionCompleted {
                new_execution_run_id,
                ..
            } => new_execution_run_id,
            _ => None,
        })
        .expect("cron completion must name its successor");
    let successor_key = RunKey::derive(request.namespace_id, &request.workflow_id, successor_id);

    let mut client_request = start_request(
        request.namespace_id,
        request.workflow_id.clone(),
        "racing-client",
    );
    client_request.now = request.now;
    client_request.request.received_at = request.now;
    // UseExisting returns the running successor instead of creating a client
    // run (service/history/api/workflow_id_dedup.go:98-99 @ v1.31.0).
    client_request.conflict_policy = WorkflowIdConflictPolicy::UseExisting;
    let client_key = client_request.run_key;
    let client_result = client_runtime
        .start_workflow_with_policy(client_request)
        .await?;
    resume
        .send(())
        .expect("completion must still be held after its commit");
    let completion_result = completion_task.await??;
    assert_eq!(
        applied_state(&completion_result).status,
        ExecutionStatus::Completed
    );

    let successor_exists = matches!(
        repo.inner.load_run(successor_key).await?,
        LoadedRun::Existing(_)
    );
    let client_used_successor = matches!(client_result, StartWorkflowResult::UsedExisting { run_key, run_id }
        if run_key == successor_key && run_id == successor_id);
    let client_started = matches!(client_result, StartWorkflowResult::Started { run_key, .. } if run_key == client_key);
    let current = repo
        .inner
        .resolve_execution(&ExecutionRef {
            namespace_id: request.namespace_id,
            workflow_id: request.workflow_id.clone(),
            run_id: None,
        })
        .await?;
    let client_is_current = current == Some(client_key);
    let successor_conflicted_with_client = repo
        .start_conflicts
        .lock()
        .expect("start-conflict observations lock poisoned")
        .contains(&(successor_key, client_key));
    assert_eq!(
        (
            successor_exists,
            client_used_successor,
            current == Some(successor_key)
        ),
        (true, true, true),
        "criterion 1.3: successor_exists={successor_exists}, client_used_successor={client_used_successor}, client_started={client_started}, client_is_current={client_is_current}, successor_conflicted_with_client={successor_conflicted_with_client}"
    );
    assert_successor(&repo.inner, &predecessor, successor_id).await?;
    Ok(())
}

fn runtime(repo: Arc<ExplorationRepo>) -> TokeiraRuntime<ExplorationRepo> {
    TokeiraRuntime::new(
        repo,
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

fn request(workflow_id: &str) -> StartRequest {
    let mut request = start_request(NamespaceId::new(), WorkflowId(workflow_id.into()), "start");
    request.now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(20);
    request.request.received_at = request.now;
    request
}

async fn start_and_poll(
    runtime: &TokeiraRuntime<ExplorationRepo>,
    request: &StartRequest,
) -> Result<(WorkflowState, WorkflowTaskCompletedRequest)> {
    let start = runtime.start_workflow(request.clone()).await?;
    let state = applied_state(&start);
    let task = runtime
        .poll_workflow_task(
            queue(request.namespace_id, &request.task_queue.0),
            WorkerIdentity("worker".into()),
            std::time::Duration::ZERO,
        )
        .await?
        .expect("initial start must publish its first workflow task");
    assert_eq!(task.run_key, state.run_key);
    Ok((
        state,
        WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: Default::default(),
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: Vec::new(),
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(request.now),
            now: request.now,
            command_sizes: Vec::new(),
        },
    ))
}

async fn existing(repo: &InMemoryStore, key: RunKey) -> Result<WorkflowState> {
    match repo.load_run(key).await? {
        LoadedRun::Existing(state) => Ok(state),
        other => panic!("expected existing run, got {other:?}"),
    }
}

async fn assert_successor(
    repo: &InMemoryStore,
    predecessor: &WorkflowState,
    successor_id: RunId,
) -> Result<()> {
    let key = RunKey::derive(
        predecessor.namespace_id,
        &predecessor.workflow_id,
        successor_id,
    );
    let successor = existing(repo, key).await?;
    assert_eq!(successor.status, ExecutionStatus::Running);
    assert!(successor.closed_at.is_none());
    assert!(matches!(
        repo.read_history(key, 0, 1).await?.first().map(|event| &event.kind),
        Some(HistoryEventKind::WorkflowExecutionStartedV2 { continued_execution_run_id, .. })
            if *continued_execution_run_id == Some(predecessor.run_id)
    ));
    assert_eq!(
        repo.resolve_execution(&ExecutionRef {
            namespace_id: predecessor.namespace_id,
            workflow_id: predecessor.workflow_id.clone(),
            run_id: None,
        })
        .await?,
        Some(key)
    );
    Ok(())
}

#[derive(Debug)]
struct ClosePause {
    key: RunKey,
    committed: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[derive(Debug, Default)]
struct ExplorationRepo {
    inner: InMemoryStore,
    failed_start: Mutex<Option<(RunKey, oneshot::Sender<()>)>>,
    close_pause: Mutex<Option<ClosePause>>,
    pair_mode: Mutex<u8>,
    fail_any_start: Mutex<bool>,
    pair_keys: Mutex<Vec<RunKey>>,
    started: Mutex<Option<oneshot::Sender<()>>>,
    start_conflicts: Mutex<Vec<(RunKey, RunKey)>>,
}

impl ExplorationRepo {
    fn fail_start(&self, key: RunKey) -> oneshot::Receiver<()> {
        let (failed, observed) = oneshot::channel();
        *self
            .failed_start
            .lock()
            .expect("start-failure hook lock poisoned") = Some((key, failed));
        observed
    }

    fn pause_after_close(&self, key: RunKey) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (committed, observed) = oneshot::channel();
        let (resume, waiting) = oneshot::channel();
        *self
            .close_pause
            .lock()
            .expect("close-pause hook lock poisoned") = Some(ClosePause {
            key,
            committed,
            resume: waiting,
        });
        (observed, resume)
    }
}

#[async_trait]
impl RunRepository for ExplorationRepo {
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
        self.inner.prepare_run_publication(preparation, fence).await
    }

    async fn publish_prepared_run(
        &self,
        token: tokeira_storage::PreparationToken,
        fence: tokeira_storage::WriteFence,
    ) -> anyhow::Result<tokeira_storage::PublicationResult> {
        let failed = *self.fail_any_start.lock().expect("test hook lock poisoned");
        let result = if failed {
            Err(anyhow::anyhow!("injected successor publication failure"))
        } else {
            self.inner.publish_prepared_run(token, fence).await
        };
        if let Some(started) = self.started.lock().expect("test hook lock poisoned").take() {
            let _ = started.send(());
        }
        result
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
        self.inner
            .commit_outbound_transition(run, transition, guard, source_fence, fence)
            .await
    }

    async fn commit_transition_pair(
        &self,
        run_key: RunKey,
        closing: Transition,
        successor_run_key: RunKey,
        successor: Transition,
        fence: WriteFence,
    ) -> Result<PairCommitResult> {
        self.pair_keys
            .lock()
            .expect("test hook lock poisoned")
            .push(successor_run_key);
        match *self.pair_mode.lock().expect("test hook lock poisoned") {
            1 => return Ok(PairCommitResult::SizeRefused),
            2 => bail!("injected pair connection error"),
            _ => {}
        }
        let result = self
            .inner
            .commit_transition_pair(run_key, closing, successor_run_key, successor, fence)
            .await?;
        let pause = {
            let mut pause = self
                .close_pause
                .lock()
                .expect("close-pause hook lock poisoned");
            if pause.as_ref().is_some_and(|pause| pause.key == run_key)
                && matches!(result, PairCommitResult::Applied { .. })
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause
                .committed
                .send(())
                .expect("close observer must be alive");
            pause.resume.await?;
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
        let is_start = transition.history_events.iter().any(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowExecutionStarted { .. }
                    | HistoryEventKind::WorkflowExecutionStartedV2 { .. }
            )
        });
        if is_start && *self.fail_any_start.lock().expect("test hook lock poisoned") {
            if let Some(started) = self.started.lock().expect("test hook lock poisoned").take() {
                let _ = started.send(());
            }
            bail!("injected fallback start failure");
        }
        let failure = {
            let mut failure = self
                .failed_start
                .lock()
                .expect("start-failure hook lock poisoned");
            if is_start && failure.as_ref().is_some_and(|(key, _)| *key == run_key) {
                failure.take()
            } else {
                None
            }
        };
        if let Some((_, failed)) = failure {
            failed.send(()).expect("failure observer must be alive");
            bail!("injected separate successor start failure");
        }
        let result = self
            .inner
            .commit_transition_for_bundle(run_key, execution_home_bundle, transition, fence)
            .await?;
        if is_start
            && let CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } = &result
        {
            self.start_conflicts
                .lock()
                .expect("start-conflict observations lock poisoned")
                .push((run_key, *existing_run_key));
        }
        if is_start
            && matches!(result, CommitResult::Applied { .. })
            && let Some(started) = self.started.lock().expect("test hook lock poisoned").take()
        {
            started.send(()).expect("start observer must be alive");
        }
        let pause = {
            let mut pause = self
                .close_pause
                .lock()
                .expect("close-pause hook lock poisoned");
            if pause.as_ref().is_some_and(|pause| pause.key == run_key)
                && matches!(&result, CommitResult::Applied { new_state } if new_state.closed_at.is_some())
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause
                .committed
                .send(())
                .expect("close observer must be alive");
            pause.resume.await?;
        }
        Ok(result)
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
        self.inner
            .commit_transition(run_key, transition, fence)
            .await
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: atomic-successors, Property 4: The fallback keeps the close.
    #[test]
    fn size_fallback_starts_on_draining_node(attempts in 2u32..8, fail_start in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let repo = Arc::new(ExplorationRepo::default());
            let runtime = runtime(repo.clone());
            let mut request = request("size-fallback");
            request.retry_policy = Some(RetryPolicy { initial_interval: Duration::seconds(1), backoff_coefficient: 1.0, maximum_interval: None, maximum_attempts: attempts, non_retryable_error_types: Vec::new() });
            let (_, mut completion) = start_and_poll(&runtime, &request).await.unwrap();
            completion.commands = vec![WorkflowCommand::FailWorkflow { failure: retryable_app_failure("retry") }];
            *repo.pair_mode.lock().expect("test hook lock poisoned") = 1;
            let (started, observed) = oneshot::channel();
            *repo.started.lock().expect("test hook lock poisoned") = Some(started);
            runtime.runtime_drain().begin();
            // The fault cuts final publication after durable close/staging.
            // External drain must not suppress the owned recovery worker.
            if fail_start { *repo.fail_any_start.lock().expect("test hook lock poisoned") = true; }
            let result = runtime.complete_workflow_task(completion).await.unwrap();
            assert_eq!(applied_state(&result).status, ExecutionStatus::Failed);
            tokio::time::timeout(std::time::Duration::from_secs(5), observed).await.expect("size fallback must reach storage while draining").expect("fallback observer must receive start attempt");
            let successor = *repo.pair_keys.lock().expect("test hook lock poisoned").last().unwrap();
            assert_eq!(matches!(repo.inner.load_run(successor).await.unwrap(), LoadedRun::Existing(_)), !fail_start,
                "size fallback recovery must proceed past external drain admission");
            assert!(!existing(&repo.inner, request.run_key).await.unwrap().status.is_open(), "fallback failure must keep the close");
            if fail_start {
                let pending = repo.read_intent_page(ShardId(0), tokeira_storage::ScanBound::Capture, None, 100).await.unwrap();
                assert!(pending.rows.iter().any(|row| row.key.kind == tokeira_storage::IntentKind::Successor), "failed publication retains the durable successor recipe");
            }
        });
    }
}

#[tokio::test(start_paused = true)]
async fn pair_error_never_falls_back() -> Result<()> {
    let repo = Arc::new(ExplorationRepo::default());
    let runtime = runtime(repo.clone());
    let mut request = request("pair-error");
    request.cron_schedule = Some("* * * * *".into());
    let (_, mut completion) = start_and_poll(&runtime, &request).await?;
    completion.commands = vec![WorkflowCommand::CompleteWorkflow {
        result: Payloads::default(),
    }];
    *repo.pair_mode.lock().expect("test hook lock poisoned") = 2;
    let before = repo.inner.snapshot().await?;
    let result = runtime.complete_workflow_task(completion).await;
    assert!(
        result.is_err(),
        "ordinary pair errors must not commit the close via fallback"
    );
    assert_eq!(repo.inner.snapshot().await?, before);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn pair_conflict_retries_same_successor() -> Result<()> {
    let repo = Arc::new(ExplorationRepo::default());
    let runtime = runtime(repo.clone());
    let mut request = request("pair-conflict");
    request.cron_schedule = Some("* * * * *".into());
    let (_, mut completion) = start_and_poll(&runtime, &request).await?;
    completion.commands = vec![WorkflowCommand::CompleteWorkflow {
        result: Payloads::default(),
    }];
    repo.inner.inject_conflict(request.run_key, 1).await;
    let result = runtime.complete_workflow_task(completion).await?;
    assert_eq!(applied_state(&result).status, ExecutionStatus::Completed);
    let keys = repo.pair_keys.lock().expect("test hook lock poisoned");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1], "conflict must retain successor identity");
    Ok(())
}
