//! Recover the frozen successor selected by a committed size-fallback close.
//! Staging uses the original acquisition fence; only final publication takes the
//! local exclusive writer barrier. It never awaits another lane under that barrier.

use std::sync::{Arc, RwLock};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use tokeira_kernel::{BasicKernel, Command, Kernel, LoadedRun, OperationKind};
use tokeira_storage::{
    IntentKind, IntentPayload, PublicationResult, RunPreparation, RunRepository,
};
use tokio_util::sync::CancellationToken;

use super::driver::{self, Attempt, Executor, Work};
use crate::{
    lane::DispatchPublisher, publisher::RuntimeDispatchPublisher, shard::ShardOwner,
    timeout::WorkflowTimeoutTrackingState, wft_timeout::WftTimeoutTrackingState,
};

struct Recovery<R> {
    repo: Arc<R>,
    publisher: RuntimeDispatchPublisher<R>,
    owner: Arc<RwLock<ShardOwner>>,
    workflow_timeouts: WorkflowTimeoutTrackingState,
    wft_timeouts: WftTimeoutTrackingState,
    shutdown: crate::shutdown::RuntimeShutdownHandle,
}

#[async_trait]
impl<R: RunRepository + 'static> Executor for Recovery<R> {
    fn wakeup(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(self.publisher.outbound_wake.clone())
    }
    fn operation_kinds(&self) -> &'static [OperationKind] {
        &[
            OperationKind::ChildStart,
            OperationKind::ChildRelease,
            OperationKind::ExternalSignal,
            OperationKind::ExternalCancel,
            OperationKind::NexusStart,
            OperationKind::NexusCancel,
        ]
    }
    fn accepts_intent(&self, _: IntentKind) -> bool {
        true
    }

    async fn deliver(&self, work: Work, attempt: Attempt) -> Result<()> {
        let descriptor = match work {
            Work::Intent(descriptor) if descriptor.key.kind != IntentKind::Successor => {
                return self
                    .publisher
                    .drive_outbound_intent(descriptor, attempt, &self.owner)
                    .await;
            }
            Work::Intent(descriptor) => descriptor,
            Work::Publication(token) => return self.reconcile(token, attempt).await,
            Work::Operation(row) => {
                return self
                    .publisher
                    .drive_outbound_operation(row, attempt, &self.owner)
                    .await;
            }
        };
        attempt.validate(&self.owner)?;
        let Some(intent) = self.repo.load_intent(descriptor.key).await? else {
            return Ok(());
        };
        let IntentPayload::Successor { start } = intent.payload else {
            anyhow::bail!("successor descriptor does not name a frozen start");
        };
        ensure!(
            intent.source_home == attempt.acquisition.shard_id,
            "successor source home changed"
        );
        // Rebuilding is deterministic: neither a new clock nor a new request/run
        // identity is drawn after close. A manifest upload interrupted at any page
        // can therefore resume with the same digest under a replacement owner.
        let mut transition = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(*start))
            .map_err(crate::KernelRejected)?;
        transition.growth_limits = Some(crate::run_growth::run_growth_limits());
        let dispatch = transition.dispatch_ops.clone();
        let history = transition.history_events.clone();
        let preparation = RunPreparation {
            request_key: format!(
                "successor:{}:{}",
                descriptor.key.source_run.0,
                descriptor.key.storage_id()
            ),
            expected_current: Some(descriptor.key.source_run),
            participants: Vec::new(),
            new_run: Box::new(transition),
            successor_intent: Some(descriptor.key),
        };
        let token = self
            .repo
            .prepare_run_publication(Box::new(preparation), attempt.fence.clone())
            .await?;
        attempt.validate(&self.owner)?;
        let exclusive = crate::serving_gate::quiesce(&self.owner, &attempt.acquisition).await?;
        attempt.validate(&self.owner)?;
        let repo = self.repo.clone();
        let owner = self.owner.clone();
        let publisher = self.publisher.clone();
        let workflow_timeouts = self
            .workflow_timeouts
            .for_acquisition(attempt.acquisition.clone());
        let wft_timeouts = self
            .wft_timeouts
            .for_acquisition(attempt.acquisition.clone());
        // Cancellation of the transport attempt must not drop the local barrier
        // while COMMIT is in flight. Shutdown owns this short publication task;
        // it finishes cache invalidation even if the attempt stops awaiting it.
        self.shutdown
            .spawn(async move {
                let _exclusive = exclusive;
                attempt.validate(&owner)?;
                owner
                    .write()
                    .expect("shard owner lock poisoned")
                    .invalidate_published_states();
                let result = repo
                    .publish_prepared_run(token.clone(), attempt.fence.clone())
                    .await;
                owner
                    .write()
                    .expect("shard owner lock poisoned")
                    .invalidate_published_states();
                let state = match result? {
                    PublicationResult::Applied { new_run, .. }
                    | PublicationResult::AlreadyPublished { new_run } => new_run,
                    PublicationResult::Conflict { reason } => {
                        anyhow::bail!("reserved successor publication conflicted: {reason}")
                    }
                };
                attempt.validate(&owner)?;
                if state.is_open() {
                    crate::lane::track_committed_run(
                        &state,
                        &history,
                        intent.source_home,
                        &workflow_timeouts,
                        &wft_timeouts,
                    );
                }
                publisher.track_published_outbound(&state, &attempt);
                drop(_exclusive);
                // A durable publication descriptor remains if the reply is lost or
                // this bookkeeping fails; retirement of the intent cannot lose it.
                repo.complete_run_preparation(token, attempt.fence).await?;
                publisher
                    .publish(state.run_key, intent.source_home, &dispatch)
                    .await
            })
            .await?
    }
}

impl<R: RunRepository + 'static> Recovery<R> {
    async fn reconcile(
        &self,
        token: tokeira_storage::PreparationToken,
        attempt: Attempt,
    ) -> Result<()> {
        attempt.validate(&self.owner)?;
        let exclusive = crate::serving_gate::quiesce(&self.owner, &attempt.acquisition).await?;
        attempt.validate(&self.owner)?;
        let loaded = self.repo.load_run(token.target_run).await?;
        self.owner
            .write()
            .expect("shard owner lock poisoned")
            .invalidate_published_states();
        if let LoadedRun::Existing(state) = loaded {
            let workflow = self
                .workflow_timeouts
                .for_acquisition(attempt.acquisition.clone());
            let wft = self
                .wft_timeouts
                .for_acquisition(attempt.acquisition.clone());
            if let Some(entry) = tokeira_storage::recovery_entries(&state).workflow_timeout {
                workflow.insert(crate::timeout::WorkflowTimeoutEntry {
                    run_key: entry.run_key,
                    shard_id: token.home,
                    workflow_execution_timeout: entry.workflow_execution_timeout,
                    workflow_run_timeout: entry.workflow_run_timeout,
                    started_at: entry.started_at,
                    workflow_start_delay: entry.workflow_start_delay,
                    first_run_started_at: entry.first_run_started_at,
                    has_retry_policy: entry.has_retry_policy,
                });
            }
            crate::lane::track_committed_run(&state, &[], token.home, &workflow, &wft);
            self.publisher.track_published_outbound(&state, &attempt);
        }
        drop(exclusive);
        attempt.validate(&self.owner)?;
        self.repo
            .complete_run_preparation(token, attempt.fence)
            .await?;
        Ok(())
    }
}

pub(crate) async fn run<R: RunRepository + 'static>(
    repo: Arc<R>,
    publisher: RuntimeDispatchPublisher<R>,
    owner: Arc<RwLock<ShardOwner>>,
    workflow_timeouts: WorkflowTimeoutTrackingState,
    wft_timeouts: WftTimeoutTrackingState,
    cancel: CancellationToken,
    shutdown: crate::shutdown::RuntimeShutdownHandle,
) {
    let executor = Arc::new(Recovery {
        repo: repo.clone(),
        publisher,
        owner: owner.clone(),
        workflow_timeouts,
        wft_timeouts,
        shutdown,
    });
    driver::run(repo, executor, owner, cancel).await;
}
