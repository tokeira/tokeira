//! Rehydrate external operations from accepted history and settle only verified
//! outcomes. Transport/lookup failures leave the source operation discoverable.

use std::sync::{Arc, RwLock};

use anyhow::{Result, ensure};
use time::OffsetDateTime;
use tokeira_kernel::{
    Command, ExternalCancelResolvedRequest, ExternalCancelResult, ExternalSignalResolvedRequest,
    ExternalSignalResult, HistoryEventKind, OperationKind,
};
use tokeira_storage::{CommitResult, OperationRow, RecoveryError, RunRepository};
use tokeira_types::{ExecutionRef, RequestId};

use super::{RuntimeDispatchPublisher, authority, require_applied};
use crate::{KernelRejected, outbound::driver::Attempt, shard::ShardOwner};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminalCause {
    MissingExecution,
    MissingNamespace,
    SignalLimit,
}

impl TerminalCause {
    fn name(self) -> &'static str {
        match self {
            Self::MissingExecution => {
                crate::signal_limits::SignalRefusal::RunClosed.external_cause()
            }
            Self::MissingNamespace => "NAMESPACE_NOT_FOUND",
            Self::SignalLimit => crate::signal_limits::SignalRefusal::SignalLimit.external_cause(),
        }
    }

    fn from_error(error: &anyhow::Error, signal: bool) -> Option<Self> {
        // These are the only definitive target refusals (processSignalExecution /
        // processCancelExecution, transfer_queue_active_task_executor.go @ v1.31.0).
        // Unknown errors, including misleading text, retain the obligation.
        match error.downcast_ref::<KernelRejected>() {
            Some(KernelRejected(tokeira_kernel::Reject::MissingRun)) => {
                Some(Self::MissingExecution)
            }
            Some(KernelRejected(tokeira_kernel::Reject::RunClosed(_))) if signal => {
                Some(Self::MissingExecution)
            }
            Some(KernelRejected(tokeira_kernel::Reject::SignalLimitExceeded)) if signal => {
                Some(Self::SignalLimit)
            }
            _ => None,
        }
    }
}

impl<R: RunRepository + 'static> RuntimeDispatchPublisher<R> {
    pub(super) async fn drive_external(
        &self,
        row: OperationRow,
        attempt: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        let source = self.operation_source(&row).await?;
        let initiation = row.incarnation.key.initiating_event;
        let signal = row.incarnation.key.kind == OperationKind::ExternalSignal;
        let history = self
            .repo
            .read_history(source.run_key, initiation - 1, 1)
            .await?;
        let event = history
            .first()
            .filter(|event| event.event_id == initiation)
            .ok_or(RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "external initiation history missing",
            })?;
        let (namespace_id, workflow_id, run_id, _signal_data) = match &event.kind {
            HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                namespace_id,
                target_workflow_id,
                target_run_id,
                signal_name,
                input,
                header,
                ..
            } if signal => (
                *namespace_id,
                target_workflow_id,
                *target_run_id,
                Some((signal_name, input, header)),
            ),
            HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                namespace_id,
                target_workflow_id,
                target_run_id,
                ..
            } if !signal => (*namespace_id, target_workflow_id, *target_run_id, None),
            _ => {
                return Err(RecoveryError::InvalidIdentity {
                    run: source.run_key,
                    detail: "external initiation kind mismatch",
                }
                .into());
            }
        };
        let metadata = match &event.kind {
            HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                workflow_task_completed_event_id,
                control,
                ..
            }
            | HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                workflow_task_completed_event_id,
                control,
                ..
            } => tokeira_kernel::ExternalResolutionMetadata {
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                control: control.clone(),
            },
            _ => unreachable!("initiation kind validated above"),
        };
        let target_namespace = match &self.namespace_resolver {
            Some(resolver) => resolver.name_for_id(namespace_id).await?,
            None => None,
        };
        let request_id = RequestId(format!(
            "ext-{}-{:?}-{initiation}",
            if signal { "signal" } else { "cancel" },
            source.run_key
        ));
        let execution = ExecutionRef {
            namespace_id,
            workflow_id: workflow_id.clone(),
            run_id,
        };
        let missing_namespace = self.namespace_resolver.is_some() && target_namespace.is_none();
        let outcome = if missing_namespace {
            Some(TerminalCause::MissingNamespace)
        } else if source.namespace_id == namespace_id && source.workflow_id == *workflow_id {
            // Self-targeting fails even with a different explicit run ID. The
            // same workflow ID in another namespace is an ordinary target.
            Some(TerminalCause::MissingExecution)
        } else if self
            .external_provenance(&execution, &request_id, &row)
            .await?
        {
            None
        } else if let Some(target) = self.external_target(&execution).await? {
            attempt.validate(owner)?;
            let result = self
                .destination(
                    target,
                    (namespace_id, workflow_id),
                    crate::DeliveryPhase::External,
                    authority(&row, &source),
                    &attempt,
                    owner,
                )
                .await;
            // Read evidence after every reply, including an apparent failure.
            // Cancellation of an already-closed/cancelled target is a verified
            // success without new history (requestcancelworkflow/api.go @ v1.31.0).
            if self
                .external_provenance(&execution, &request_id, &row)
                .await?
            {
                None
            } else {
                match result {
                    Ok(CommitResult::Applied { new_state })
                        if !signal
                            && new_state.run_key == target
                            && (!new_state.is_open() || new_state.cancel_requested) =>
                    {
                        None
                    }
                    Err(error) => match TerminalCause::from_error(&error, signal) {
                        Some(cause) => Some(cause),
                        None => return Err(error),
                    },
                    _ => anyhow::bail!("external destination outcome remains unverified"),
                }
            }
        } else {
            Some(TerminalCause::MissingExecution)
        };
        let now = OffsetDateTime::now_utc();
        let command = if signal {
            Command::ExternalSignalResolvedV2 {
                metadata,
                request: ExternalSignalResolvedRequest {
                    initiated_event_id: initiation,
                    result: match outcome {
                        None => ExternalSignalResult::Signaled,
                        Some(cause) => ExternalSignalResult::Failed {
                            cause: cause.name().into(),
                        },
                    },
                    target_namespace,
                    now,
                },
            }
        } else {
            Command::ExternalCancelResolvedV2 {
                metadata,
                request: ExternalCancelResolvedRequest {
                    initiated_event_id: initiation,
                    result: match outcome {
                        None => ExternalCancelResult::CancelRequested,
                        Some(cause) => ExternalCancelResult::Failed {
                            cause: cause.name().into(),
                        },
                    },
                    target_namespace,
                    now,
                },
            }
        };
        require_applied(self.source_outcome(&row, command, &attempt, owner).await?)?;
        Ok(())
    }

    async fn external_provenance(
        &self,
        execution: &ExecutionRef,
        request: &RequestId,
        row: &OperationRow,
    ) -> Result<bool> {
        let Some(record) = self.repo.lookup_request_dedupe(execution, request).await? else {
            return Ok(false);
        };
        ensure!(
            record.namespace_id == execution.namespace_id
                && record.workflow_id == execution.workflow_id
                && execution.run_id.is_none_or(|run| record.run_id == run)
                && record
                    .outbound_origin
                    .is_some_and(|origin| origin.incarnation == row.incarnation),
            RecoveryError::InvalidIdentity {
                run: row.incarnation.key.source_run,
                detail: "external request dedupe provenance mismatch"
            }
        );
        Ok(true)
    }

    async fn external_target(
        &self,
        execution: &ExecutionRef,
    ) -> Result<Option<tokeira_types::RunKey>> {
        if execution.run_id.is_some() {
            self.repo.resolve_execution(execution).await
        } else {
            // A closed current run still answers idempotent signal requests and
            // successful cancellation. The open-run index alone loses that
            // behavior (signalworkflow/api.go and requestcancelworkflow/api.go @ v1.31.0).
            self.repo
                .find_latest_run(execution.namespace_id, &execution.workflow_id)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_typed_definitive_refusals_can_become_history() {
        let unknown = anyhow::anyhow!("kernel rejected: workflow not found / signal limit");
        assert_eq!(TerminalCause::from_error(&unknown, true), None);
        let unavailable = anyhow::Error::new(crate::NotShardOwner::local(
            tokeira_types::ShardId(0),
            tokeira_types::ShardEpoch::ZERO,
        ));
        assert_eq!(TerminalCause::from_error(&unavailable, false), None);
        let limit = anyhow::Error::new(KernelRejected(tokeira_kernel::Reject::SignalLimitExceeded))
            .context("destination");
        assert_eq!(
            TerminalCause::from_error(&limit, true),
            Some(TerminalCause::SignalLimit)
        );
        assert_eq!(TerminalCause::from_error(&limit, false), None);
    }
}
