//! Owned delivery of committed references. Hydration grants no authority: every
//! destination commit rechecks the source and every source outcome uses the
//! acquisition captured before transport. The publisher remains a delivery aid.

use std::sync::{Arc, RwLock};

use anyhow::{Result, ensure};
use tokeira_kernel::{Command, LoadedRun, OperationKind, Transition, WorkflowState};
use tokeira_storage::{CommitResult, DeliveryGuard, OperationRow, RecoveryError, RunRepository};
use tokeira_types::{NamespaceId, RunKey, WorkflowId, execution_home_bundle};

use super::RuntimeDispatchPublisher;
use crate::{lane::OutboundSubmission, outbound::driver::Attempt, shard::ShardOwner};

mod child;
mod close;
mod delivery;
mod external;
mod nexus;

impl<R: RunRepository + 'static> RuntimeDispatchPublisher<R> {
    /// Rebuild disposable tracking from a fully published snapshot under the
    /// original acquisition. Used before releasing the publication barrier and
    /// again after a lost publication reply; no new deadlines are calculated.
    pub(crate) fn track_published_outbound(&self, state: &WorkflowState, attempt: &Attempt) {
        let entries = tokeira_storage::recovery_entries(state);
        let home = attempt.acquisition.shard_id;
        let activity = self
            .activity_tracking
            .for_acquisition(attempt.acquisition.clone());
        activity.remove_all_for_run(state.run_key);
        for entry in entries.activities {
            let cancel_requested = state
                .activities
                .get(&entry.activity_id)
                .is_some_and(|activity| activity.cancel_requested);
            activity.insert(crate::ActivityTrackingEntry {
                run_key: state.run_key,
                shard_id: home,
                activity_id: entry.activity_id,
                original_scheduled_at: entry.original_scheduled_at,
                last_dispatched_at: entry
                    .current_attempt_scheduled_at
                    .unwrap_or(entry.original_scheduled_at),
                started_at: entry.started_at,
                last_heartbeat_at: entry.last_heartbeat_at,
                cancel_requested,
            });
        }
        let nexus = self
            .nexus_timeout_tracking
            .for_acquisition(attempt.acquisition.clone());
        nexus.remove_all_for_run(state.run_key);
        for entry in entries.nexus_timeouts {
            nexus.insert(crate::NexusTimeoutEntry {
                run_key: state.run_key,
                shard_id: home,
                operation_id: entry.operation_id,
                scheduled_event_id: entry.scheduled_event_id,
                scheduled_at: entry.scheduled_at,
            });
        }
        let callbacks = self
            .completion_callback_tracking
            .for_acquisition(attempt.acquisition.clone());
        callbacks.remove_all_for_run(state.run_key);
        for entry in entries.completion_callbacks {
            callbacks.insert(crate::CompletionCallbackTrackingEntry {
                run_key: state.run_key,
                shard_id: home,
                callback_index: entry.callback_index,
            });
        }
    }

    /// Execute only authoritative work discovered by the bounded owned driver.
    pub(crate) async fn drive_outbound_operation(
        &self,
        row: OperationRow,
        attempt: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        attempt.validate(owner)?;
        match row.incarnation.key.kind {
            OperationKind::ChildStart => self.drive_child_start(row, attempt, owner).await,
            OperationKind::ChildRelease => self.drive_child_release(row, attempt, owner).await,
            OperationKind::ExternalSignal | OperationKind::ExternalCancel => {
                self.drive_external(row, attempt, owner).await
            }
            OperationKind::NexusStart | OperationKind::NexusCancel => {
                self.drive_nexus(row, attempt, owner).await
            }
        }
    }

    async fn operation_source(&self, row: &OperationRow) -> Result<WorkflowState> {
        let LoadedRun::Existing(state) = self.repo.load_run(row.incarnation.key.source_run).await?
        else {
            return Err(RecoveryError::Obsolete.into());
        };
        ensure!(
            tokeira_storage::derive_operations(&state, row.source_home)?
                .iter()
                .any(|pending| pending.incarnation == row.incarnation),
            RecoveryError::Obsolete
        );
        Ok(state)
    }

    async fn source_outcome(
        &self,
        row: &OperationRow,
        command: Command,
        attempt: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<CommitResult> {
        attempt.validate(owner)?;
        let state = self.operation_source(row).await?;
        self.pick_lane(state.run_key)
            .submit_outbound(
                state.run_key,
                command,
                OutboundSubmission {
                    attempt: attempt.clone(),
                    source_fence: attempt.fence.clone(),
                    authority: DeliveryGuard::Operation {
                        incarnation: row.incarnation,
                        source_seq: state.transition_seq,
                    },
                },
            )
            .await
    }

    async fn destination(
        &self,
        run: RunKey,
        identity: (NamespaceId, &WorkflowId),
        phase: crate::DeliveryPhase,
        authority: DeliveryGuard,
        source_attempt: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<CommitResult> {
        source_attempt.validate(owner)?;
        let home = execution_home_bundle(
            identity.0.0.as_bytes(),
            identity.1.0.as_bytes(),
            owner
                .read()
                .expect("shard owner lock poisoned")
                .shard_count(),
        );
        let reference = crate::OutboundDeliveryReference {
            authority,
            source_fence: source_attempt.fence.clone(),
            phase,
            destination: run,
            destination_home: home,
        };
        // Same-home delivery must retain the original local acquisition token,
        // including its generation when the durable lease epoch was reused.
        let attempt = if home == source_attempt.acquisition.shard_id {
            source_attempt.clone()
        } else {
            match Attempt::capture(owner, home) {
                Some(attempt) => attempt,
                None => {
                    let transport = self
                        .outbound_transport
                        .read()
                        .expect("outbound transport lock poisoned")
                        .clone()
                        .ok_or_else(|| {
                            crate::NotShardOwner::local(home, tokeira_types::ShardEpoch::ZERO)
                        })?;
                    let outcome = transport.deliver(reference).await?;
                    source_attempt.validate(owner)?;
                    return self.observe_remote_outcome(run, home, outcome).await;
                }
            }
        };
        self.receive_outbound(&reference, attempt, owner).await
    }

    async fn observe_remote_outcome(
        &self,
        run: RunKey,
        home: tokeira_types::ShardId,
        outcome: crate::OutboundDeliveryOutcome,
    ) -> Result<CommitResult> {
        match outcome {
            crate::OutboundDeliveryOutcome::Applied { run_id, seq } => {
                let LoadedRun::Existing(state) = self.repo.load_run(run).await? else {
                    anyhow::bail!("outbound target observation unavailable");
                };
                ensure!(
                    state.run_id == run_id && state.transition_seq >= seq,
                    RecoveryError::Obsolete
                );
                Ok(CommitResult::Applied { new_state: state })
            }
            crate::OutboundDeliveryOutcome::Terminal(cause) => {
                Err(crate::KernelRejected(match cause {
                    crate::OutboundTerminalCause::MissingExecution => {
                        tokeira_kernel::Reject::MissingRun
                    }
                    crate::OutboundTerminalCause::ClosedExecution => {
                        tokeira_kernel::Reject::RunClosed(tokeira_types::ExecutionStatus::Completed)
                    }
                    crate::OutboundTerminalCause::SignalLimit => {
                        tokeira_kernel::Reject::SignalLimitExceeded
                    }
                })
                .into())
            }
            crate::OutboundDeliveryOutcome::Obsolete => Err(RecoveryError::Obsolete.into()),
            crate::OutboundDeliveryOutcome::NotOwner => {
                Err(crate::NotShardOwner::local(home, tokeira_types::ShardEpoch::ZERO).into())
            }
            crate::OutboundDeliveryOutcome::Retryable => {
                anyhow::bail!("outbound destination requires rediscovery")
            }
        }
    }
}

fn authority(row: &OperationRow, source: &WorkflowState) -> DeliveryGuard {
    DeliveryGuard::Operation {
        incarnation: row.incarnation,
        source_seq: source.transition_seq,
    }
}

/// Metadata owns a sequence but no public event. The lane/store still apply the
/// normal fencing, derivation and projection rules to this complete state.
fn metadata_transition(mut state: WorkflowState) -> Transition {
    let expected_seq = state.transition_seq;
    state.transition_seq = tokeira_types::TransitionSeq(expected_seq.0 + 1);
    Transition {
        expected_seq,
        next_state: state,
        history_events: Default::default(),
        event_principals: Default::default(),
        request_dedupe_ops: Default::default(),
        activity_ops: Default::default(),
        timer_ops: Default::default(),
        dispatch_ops: Default::default(),
        events_numbered_at_close: 0,
        growth_limits: Some(crate::run_growth::run_growth_limits()),
    }
}

fn require_applied(result: CommitResult) -> Result<WorkflowState> {
    match result {
        CommitResult::Applied { new_state } => Ok(new_state),
        other => anyhow::bail!("outbound transition did not apply: {other:?}"),
    }
}
