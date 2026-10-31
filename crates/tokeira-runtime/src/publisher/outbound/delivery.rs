//! Destination-only hydration. Every accepted command is reconstructed from a
//! committed operation or immutable intent, then guarded in its effect transaction.
//! No receiver writes source outcomes, chooses child seeds, or forwards recursively.

use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
};

use anyhow::{Result, ensure};
use time::OffsetDateTime;
use tokeira_kernel::{
    CancelRequest, ChildOutcomeMetadata, ChildResolution, ChildResolvedRequest, Command,
    ExternalWorkflowExecution, HistoryEvent, HistoryEventKind, LoadedRun, OperationKind,
    ParentChildStartRecordedRequest, ParentClosePolicy, SignalRequest, TerminateRequest,
    WorkflowIdReusePolicy, WorkflowState,
};
use tokeira_storage::{CommitResult, DeliveryGuard, IntentPayload, RecoveryError, RunRepository};
use tokeira_types::{RequestContext, RequestId, RunKey, execution_home_bundle};

use super::RuntimeDispatchPublisher;
use crate::{
    DeliveryPhase, OutboundDeliveryReference, lane::OutboundSubmission, outbound::driver::Attempt,
    shard::ShardOwner,
};

impl<R: RunRepository + 'static> RuntimeDispatchPublisher<R> {
    pub(crate) async fn receive_outbound(
        &self,
        reference: &OutboundDeliveryReference,
        attempt: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<CommitResult> {
        attempt.validate(owner)?;
        ensure!(
            attempt.acquisition.shard_id == reference.destination_home,
            RecoveryError::Obsolete
        );
        let (command, guard, namespace, workflow) =
            self.hydrate_delivery(reference).await.map_err(|error| {
                // A missing recipe dependency is not a definitive external target
                // refusal. Only the admitted destination command can produce one.
                if error.downcast_ref::<crate::KernelRejected>().is_some() {
                    anyhow::anyhow!("outbound recipe dependency unavailable")
                } else {
                    error
                }
            })?;
        let home = execution_home_bundle(
            namespace.0.as_bytes(),
            workflow.0.as_bytes(),
            owner
                .read()
                .expect("shard owner lock poisoned")
                .shard_count(),
        );
        ensure!(home == reference.destination_home, RecoveryError::Obsolete);
        attempt.validate(owner)?;
        self.pick_lane(reference.destination)
            .submit_outbound(
                reference.destination,
                command,
                OutboundSubmission {
                    attempt,
                    authority: guard,
                    source_fence: reference.source_fence.clone(),
                },
            )
            .await
    }

    async fn delivery_state(&self, run: RunKey) -> Result<WorkflowState> {
        match self.repo.load_outbound_source(run).await? {
            LoadedRun::Existing(state) => Ok(state),
            LoadedRun::Absent => {
                Err(crate::KernelRejected(tokeira_kernel::Reject::MissingRun).into())
            }
        }
    }

    async fn delivery_event(&self, run: RunKey, id: i64) -> Result<HistoryEvent> {
        ensure!(id > 0, RecoveryError::Obsolete);
        self.repo
            .read_history(run, id - 1, 1)
            .await?
            .into_iter()
            .find(|event| event.event_id == id)
            .ok_or_else(|| {
                RecoveryError::InvalidIdentity {
                    run,
                    detail: "outbound recipe history missing",
                }
                .into()
            })
    }

    async fn hydrate_delivery(
        &self,
        reference: &OutboundDeliveryReference,
    ) -> Result<(
        Command,
        DeliveryGuard,
        tokeira_types::NamespaceId,
        tokeira_types::WorkflowId,
    )> {
        let now = OffsetDateTime::now_utc();
        let target = reference.destination;
        let mut guard = reference.authority;
        let (command, namespace, workflow) = match guard {
            DeliveryGuard::Operation {
                incarnation,
                source_seq,
            }
            | DeliveryGuard::ChildStart {
                incarnation,
                source_seq,
                ..
            } => {
                let LoadedRun::Existing(source) =
                    self.repo.load_run(incarnation.key.source_run).await?
                else {
                    return Err(RecoveryError::Obsolete.into());
                };
                ensure!(source.transition_seq == source_seq, RecoveryError::Obsolete);
                let home = match &reference.source_fence {
                    tokeira_storage::WriteFence::Lease(fence) => fence.home,
                    tokeira_storage::WriteFence::Unfenced => tokeira_types::ShardId(0),
                };
                ensure!(
                    tokeira_storage::derive_operations(&source, home)?
                        .iter()
                        .any(|row| row.incarnation == incarnation),
                    RecoveryError::Obsolete
                );
                let initiation = incarnation.key.initiating_event;
                match (reference.phase, incarnation.key.kind) {
                    (
                        DeliveryPhase::ChildStart | DeliveryPhase::ChildReplace,
                        OperationKind::ChildStart,
                    ) => {
                        let event = self.delivery_event(source.run_key, initiation).await?;
                        let start = super::child::child_start_request(&source, &event, now)?;
                        let current = self
                            .repo
                            .find_latest_run(start.namespace_id, &start.workflow_id)
                            .await?;
                        let expected = match guard {
                            DeliveryGuard::ChildStart {
                                expected_current, ..
                            } => expected_current,
                            _ => Some(target),
                        };
                        ensure!(current == expected, RecoveryError::Obsolete);
                        let command = if reference.phase == DeliveryPhase::ChildStart {
                            ensure!(start.run_key == target, RecoveryError::Obsolete);
                            Command::Start(start.clone())
                        } else {
                            ensure!(
                                event.kind.child_reuse_policy().or_else(|| source
                                    .outbound
                                    .as_ref()
                                    .and_then(|outbound| outbound
                                        .legacy_child_policies
                                        .get(&initiation)
                                        .copied()))
                                    == Some(WorkflowIdReusePolicy::TerminateIfRunning),
                                RecoveryError::Obsolete
                            );
                            let child = self.delivery_state(target).await?;
                            ensure!(
                                child.is_open()
                                    && child.namespace_id == start.namespace_id
                                    && child.workflow_id == start.workflow_id,
                                RecoveryError::Obsolete
                            );
                            Command::Terminate(TerminateRequest {
                                reason: "TerminateIfRunning WorkflowIdReusePolicy".into(),
                                details: None,
                                identity: "history-service".into(),
                                links: Vec::new(),
                                request: internal_request(
                                    format!(
                                        "child-replace:{}:{target:?}",
                                        start.request.request_id.0
                                    ),
                                    now,
                                ),
                                now,
                            })
                        };
                        // The accepted reuse decision and current pointer must
                        // survive until the same transaction creates/replaces it.
                        guard = DeliveryGuard::ChildStart {
                            incarnation,
                            source_seq,
                            expected_current: current,
                        };
                        (command, start.namespace_id, start.workflow_id)
                    }
                    (DeliveryPhase::ChildRelease, OperationKind::ChildRelease) => {
                        let release = source
                            .outbound
                            .as_ref()
                            .and_then(|outbound| outbound.child_releases.get(&initiation))
                            .ok_or(RecoveryError::Obsolete)?;
                        ensure!(
                            target
                                == RunKey::derive(
                                    release.namespace_id,
                                    &release.workflow_id,
                                    release.child_run_id
                                ),
                            RecoveryError::Obsolete
                        );
                        let child = self.delivery_state(target).await?;
                        let started = release
                            .parent_started_event_id
                            .ok_or(RecoveryError::Obsolete)?;
                        let confirmation = self.delivery_event(source.run_key, started).await?;
                        ensure!(
                            matches!(&confirmation.kind,
                            HistoryEventKind::ChildWorkflowExecutionStarted { child_workflow_id, child_run_id, initiated_event_id, .. }
                            if child_workflow_id == &release.workflow_id && *child_run_id == release.child_run_id && *initiated_event_id == initiation),
                            RecoveryError::Obsolete
                        );
                        ensure!(
                            child.parent_run_key == Some(source.run_key)
                                || self
                                    .reset_child_relationship(&source, &child, initiation)
                                    .await?,
                            RecoveryError::Obsolete
                        );
                        let parent_run_key = child.parent_run_key.ok_or(RecoveryError::Obsolete)?;
                        (
                            Command::ParentChildStartRecorded(ParentChildStartRecordedRequest {
                                parent_run_key,
                                initiating_event: initiation,
                                first_child_run_id: release.child_run_id,
                                parent_started_event_id: release
                                    .parent_started_event_id
                                    .ok_or(RecoveryError::Obsolete)?,
                                now,
                            }),
                            release.namespace_id,
                            release.workflow_id.clone(),
                        )
                    }
                    (
                        DeliveryPhase::External,
                        OperationKind::ExternalSignal | OperationKind::ExternalCancel,
                    ) => {
                        let event = self.delivery_event(source.run_key, initiation).await?;
                        let signal = incarnation.key.kind == OperationKind::ExternalSignal;
                        let (namespace, workflow, run_id, command) =
                            external_command(&source, &event, signal, now)?;
                        ensure!(
                            source.namespace_id != namespace || source.workflow_id != workflow,
                            RecoveryError::Obsolete
                        );
                        let selected = match run_id {
                            Some(run) => Some(RunKey::derive(namespace, &workflow, run)),
                            None => self.repo.find_latest_run(namespace, &workflow).await?,
                        };
                        ensure!(selected == Some(target), RecoveryError::Obsolete);
                        (command, namespace, workflow)
                    }
                    _ => return Err(RecoveryError::Obsolete.into()),
                }
            }
            DeliveryGuard::Intent(key) | DeliveryGuard::CurrentIntent { key, .. } => {
                let intent = self
                    .repo
                    .load_intent(key)
                    .await?
                    .ok_or(RecoveryError::Obsolete)?;
                reference.source_fence.ensure_home(intent.source_home)?;
                let source = self.delivery_state(key.source_run).await?;
                match (&intent.payload, reference.phase) {
                    (
                        IntentPayload::ParentClose {
                            namespace_id,
                            workflow_id,
                            initiating_event,
                            first_child_run,
                            policy,
                        },
                        DeliveryPhase::ParentClose,
                    ) => {
                        ensure!(
                            self.repo
                                .find_latest_run(*namespace_id, workflow_id)
                                .await?
                                == Some(target),
                            RecoveryError::Obsolete
                        );
                        let child = self.delivery_state(target).await?;
                        ensure!(
                            child.parent_run_key == Some(source.run_key)
                                && child.parent_initiated_event_id == *initiating_event
                                && first_child_run.is_some()
                                && Some(child.first_execution_run_id.unwrap_or(child.run_id))
                                    == *first_child_run,
                            RecoveryError::Obsolete
                        );
                        guard = DeliveryGuard::CurrentIntent {
                            key,
                            expected_current: Some(target),
                        };
                        let request = internal_request(
                            format!("parent-close:{}:{}", key.source_run.0, key.storage_id()),
                            now,
                        );
                        let command = match policy {
                            ParentClosePolicy::Terminate => Command::Terminate(TerminateRequest {
                                reason: "by parent close policy".into(),
                                details: None,
                                identity: "history-service".into(),
                                links: Vec::new(),
                                request,
                                now,
                            }),
                            ParentClosePolicy::RequestCancel => Command::Cancel(CancelRequest {
                                reason: String::new(),
                                external_initiator: Some(ExternalWorkflowExecution {
                                    namespace_id: source.namespace_id,
                                    workflow_id: source.workflow_id.clone(),
                                    run_id: source.run_id,
                                }),
                                external_initiated_event_id: 0,
                                links: Vec::new(),
                                request,
                                now,
                            }),
                            ParentClosePolicy::Abandon => {
                                return Err(RecoveryError::Obsolete.into());
                            }
                        };
                        (command, *namespace_id, workflow_id.clone())
                    }
                    (
                        IntentPayload::ChildResult {
                            parent_run,
                            identity,
                            terminal_event,
                        },
                        DeliveryPhase::ChildResult,
                    ) => {
                        ensure!(
                            !source.is_open()
                                && source.parent_run_key == Some(*parent_run)
                                && source.parent_initiated_event_id
                                    == identity.parent_initiated_event
                                && source.first_execution_run_id.unwrap_or(source.run_id)
                                    == identity.child_first_run
                                && source.run_id == identity.child_final_run,
                            RecoveryError::Obsolete
                        );
                        let mut next = *parent_run;
                        let mut visited = BTreeSet::new();
                        let parent = loop {
                            ensure!(
                                visited.insert(next) && visited.len() <= 100,
                                RecoveryError::Obsolete
                            );
                            let parent = self.delivery_state(next).await?;
                            if parent.is_open() {
                                break parent;
                            }
                            let reset = parent.reset_run_id.ok_or(RecoveryError::Obsolete)?;
                            next = RunKey::derive(parent.namespace_id, &parent.workflow_id, reset);
                        };
                        ensure!(parent.run_key == target, RecoveryError::Obsolete);
                        let pending = parent
                            .children
                            .get(&source.workflow_id)
                            .ok_or(RecoveryError::Obsolete)?;
                        ensure!(
                            pending.namespace_id == source.namespace_id
                                && pending.initiated_event_id == identity.parent_initiated_event
                                && pending
                                    .child_run_id
                                    .is_none_or(|first| first == identity.child_first_run),
                            RecoveryError::Obsolete
                        );
                        let event = self.delivery_event(source.run_key, *terminal_event).await?;
                        (
                            child_result_command(&source, *identity, &event, now)?,
                            parent.namespace_id,
                            parent.workflow_id,
                        )
                    }
                    _ => return Err(RecoveryError::Obsolete.into()),
                }
            }
        };
        Ok((command, guard, namespace, workflow))
    }
}

fn internal_request(id: String, now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(id),
        caller_identity: Some("history-service".into()),
        principal: None,
        received_at: now,
    }
}

pub(super) fn external_command(
    source: &WorkflowState,
    event: &HistoryEvent,
    signal: bool,
    now: OffsetDateTime,
) -> Result<(
    tokeira_types::NamespaceId,
    tokeira_types::WorkflowId,
    Option<tokeira_types::RunId>,
    Command,
)> {
    let request = internal_request(
        format!(
            "ext-{}-{:?}-{}",
            if signal { "signal" } else { "cancel" },
            source.run_key,
            event.event_id
        ),
        now,
    );
    match &event.kind {
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
            namespace_id,
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
            header,
            ..
        } if signal => Ok((
            *namespace_id,
            target_workflow_id.clone(),
            *target_run_id,
            Command::Signal(SignalRequest {
                signal_name: signal_name.clone(),
                input: input.clone(),
                header: header.clone(),
                links: Vec::new(),
                request,
                now,
            }),
        )),
        HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
            namespace_id,
            target_workflow_id,
            target_run_id,
            ..
        } if !signal => Ok((
            *namespace_id,
            target_workflow_id.clone(),
            *target_run_id,
            Command::Cancel(CancelRequest {
                reason: format!(
                    "cancel requested by external workflow {}",
                    source.workflow_id.0
                ),
                external_initiator: Some(ExternalWorkflowExecution {
                    namespace_id: source.namespace_id,
                    workflow_id: source.workflow_id.clone(),
                    run_id: source.run_id,
                }),
                external_initiated_event_id: event.event_id,
                links: Vec::new(),
                request,
                now,
            }),
        )),
        _ => Err(RecoveryError::Obsolete.into()),
    }
}

pub(super) fn child_result_command(
    source: &WorkflowState,
    identity: tokeira_kernel::ChildOutcomeIdentity,
    event: &HistoryEvent,
    now: OffsetDateTime,
) -> Result<Command> {
    let mut metadata = ChildOutcomeMetadata {
        identity,
        retry_state: None,
        cancellation_details: None,
    };
    let resolution = match &event.kind {
        HistoryEventKind::WorkflowExecutionCompleted {
            result,
            new_execution_run_id: None,
            ..
        } => ChildResolution::Completed {
            result: result.clone(),
        },
        HistoryEventKind::WorkflowExecutionFailed {
            failure,
            retry_state,
            new_execution_run_id: None,
            ..
        } => {
            metadata.retry_state = Some(retry_state.clone());
            ChildResolution::Failed {
                failure: failure.clone(),
            }
        }
        HistoryEventKind::WorkflowExecutionCanceled { details, .. } => {
            metadata.cancellation_details = details.clone();
            ChildResolution::Canceled
        }
        HistoryEventKind::WorkflowExecutionTerminated { .. } => ChildResolution::Terminated,
        HistoryEventKind::WorkflowExecutionTimedOut {
            retry_state,
            new_execution_run_id: None,
            ..
        } => {
            metadata.retry_state = Some(retry_state.clone());
            ChildResolution::TimedOut
        }
        _ => return Err(RecoveryError::Obsolete.into()),
    };
    Ok(Command::ChildResolvedV2 {
        request: ChildResolvedRequest {
            child_workflow_id: source.workflow_id.clone(),
            resolved_run_id: Some(identity.child_final_run),
            resolution,
            now,
        },
        metadata,
    })
}
