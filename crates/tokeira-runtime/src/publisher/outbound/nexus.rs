//! One durable Nexus attempt lifecycle, independent of timeout tracking. Due
//! timeouts win before I/O; backoff transitions preserve the stored deadline and
//! worker correlations retain the dispatching acquisition through the reply.

use std::sync::{Arc, RwLock};

use anyhow::{Result, ensure};
use time::OffsetDateTime;
use tokeira_kernel::{
    Command, NexusCancellationAttemptOutcome, NexusCancellationAttemptedRequest,
    NexusCancellationRetryRequest, NexusOperationResolvedRequest, NexusOperationRetryRequest,
    NexusResolution, OperationKind, PendingNexusOperation, WorkerDeploymentVersionRef,
};
use tokeira_proto::{
    conversions::common::failure_to_payload, public::temporal::api::failure::v1 as failure_proto,
};
use tokeira_storage::{OperationRow, RecoveryError, RunRepository};

use super::super::{
    RuntimeDispatchPublisher, call_error_failure, cancellation_failure_outcome,
    external_handler_error_resolution, external_handler_failure,
    terminal_nexus_call_failure_resolution,
};
use crate::{
    nexus::{
        EndpointTarget, NexusCancelResult, NexusDeliveryAuthority, NexusQueueKey, NexusStartResult,
        NexusTaskRequest, NexusWorkflowAttempt, evaluate_nexus_timeout,
    },
    outbound::driver::Attempt,
    shard::ShardOwner,
};

impl<R: RunRepository + 'static> RuntimeDispatchPublisher<R> {
    pub(super) async fn drive_nexus(
        &self,
        row: OperationRow,
        acquisition: Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        acquisition.validate(owner)?;
        let source = self.operation_source(&row).await?;
        let pending = source
            .pending_nexus_operations
            .values()
            .find(|operation| operation.scheduled_event_id == row.incarnation.key.initiating_event)
            .cloned()
            .ok_or(RecoveryError::Obsolete)?;
        let now = OffsetDateTime::now_utc();
        // Invocation timeout precedence follows executeInvocationTask in
        // components/nexusoperations/executors.go @ v1.31.0. Discovery can be
        // delayed indefinitely without granting a new timeout anchor.
        if let Some(timeout_type) = evaluate_nexus_timeout(&pending, now) {
            self.source_outcome(
                &row,
                Command::NexusOperationResolved(NexusOperationResolvedRequest {
                    expected_attempt: None,
                    operation_id: pending.operation_id,
                    scheduled_event_id: pending.scheduled_event_id,
                    resolution: NexusResolution::TimedOut { timeout_type },
                    now,
                }),
                &acquisition,
                owner,
            )
            .await?;
            return Ok(());
        }
        if row.due_at > now {
            return Ok(());
        }
        let retry = match row.incarnation.key.kind {
            OperationKind::NexusStart if pending.next_attempt_at.is_some() => {
                Some(Command::NexusOperationRetry(NexusOperationRetryRequest {
                    operation_id: pending.operation_id.clone(),
                    scheduled_event_id: pending.scheduled_event_id,
                    now,
                }))
            }
            OperationKind::NexusCancel
                if pending
                    .cancellation
                    .as_ref()
                    .is_some_and(|c| c.next_attempt_at.is_some()) =>
            {
                Some(Command::NexusCancellationRetry(
                    NexusCancellationRetryRequest {
                        operation_id: pending.operation_id.clone(),
                        scheduled_event_id: pending.scheduled_event_id,
                        requested_event_id: row.incarnation.requested_event,
                        now,
                    },
                ))
            }
            _ => None,
        };
        if let Some(command) = retry {
            self.source_outcome(&row, command, &acquisition, owner)
                .await?;
            // Reload through discovery after the retry transition; never invoke
            // using the phase snapshot that existed before it committed.
            return Ok(());
        }
        let version = source.effective_deployment().cloned();
        match row.incarnation.key.kind {
            OperationKind::NexusStart => {
                self.invoke_nexus_start(&row, pending, version, &acquisition, owner)
                    .await
            }
            OperationKind::NexusCancel => {
                self.invoke_nexus_cancel(&row, pending, version, &acquisition, owner)
                    .await
            }
            _ => Err(RecoveryError::InvalidIdentity {
                run: source.run_key,
                detail: "Nexus handler received a different operation kind",
            }
            .into()),
        }
    }

    async fn invoke_nexus_start(
        &self,
        row: &OperationRow,
        pending: PendingNexusOperation,
        originator_version: Option<WorkerDeploymentVersionRef>,
        acquisition: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        let operation_id = pending.operation_id.clone();
        let endpoint_name = pending.endpoint.clone();
        let service = pending.service.clone();
        let operation = pending.operation.clone();
        let input = pending.input.clone();
        let schedule_to_close_timeout = pending.schedule_to_close_timeout;
        let originator_run_key = row.incarnation.key.source_run;
        let scheduled_event_id = pending.scheduled_event_id;
        let scheduled_at = pending.scheduled_at;
        let delivery = NexusWorkflowAttempt {
            generation: pending.attempt,
            requested_event_id: None,
        };
        let resolution = match self.nexus_registry.resolve(&endpoint_name) {
            Some(config) => match &config.target {
                EndpointTarget::External { address } => {
                    let trace_headers = self.nexus_trace_headers();
                    let started_at = std::time::Instant::now();
                    let start_result = self
                        .nexus_client
                        .start_operation(
                            address,
                            &operation_id,
                            &service,
                            &operation,
                            &input,
                            schedule_to_close_timeout,
                            &trace_headers,
                        )
                        .await;
                    let latency = started_at.elapsed();
                    // Record the outbound StartOperation outcome (`startCallOutcomeTag @
                    // v1.31.0`). External calls do not flow through tokeira's own frontend,
                    // so the failure source stays unset → `_unknown_`
                    // (`SetFailureSourceOnContext` is worker-path only,
                    // `components/nexusoperations/fx.go @ v1.31.0`).
                    let outcome = match &start_result {
                        Ok(NexusStartResult::SyncCompleted { .. }) => "successful".to_string(),
                        Ok(NexusStartResult::AsyncAccepted { .. }) => "pending".to_string(),
                        Ok(NexusStartResult::SyncFailed { .. }) => {
                            "operation-unsuccessful:failed".to_string()
                        }
                        Ok(NexusStartResult::HandlerError { error_type, .. }) => {
                            format!("handler-error:{error_type}")
                        }
                        Err(_) => "unknown-error".to_string(),
                    };
                    self.record_external_outbound_metric(
                        originator_run_key,
                        "StartOperation",
                        &outcome,
                        latency,
                    )
                    .await;

                    // Preserve the existing External-endpoint failure classification.
                    // The source outcome below is still bound to the original
                    // acquisition and scheduled-attempt identity.
                    match start_result {
                        Ok(NexusStartResult::SyncCompleted { result, links }) => {
                            tokeira_kernel::NexusResolution::Completed { result, links }
                        }
                        Ok(NexusStartResult::AsyncAccepted {
                            operation_token,
                            links,
                        }) => tokeira_kernel::NexusResolution::Started {
                            operation_token,
                            links,
                        },
                        Ok(NexusStartResult::SyncFailed { message }) => {
                            terminal_nexus_call_failure_resolution(
                                message,
                                "NexusOperationFailure",
                                false,
                                &endpoint_name,
                                &service,
                                &operation,
                                scheduled_event_id,
                            )
                        }
                        Ok(NexusStartResult::HandlerError {
                            error_type,
                            failure,
                            ..
                        }) => external_handler_error_resolution(
                            &error_type,
                            failure.as_ref(),
                            &endpoint_name,
                            &service,
                            &operation,
                            scheduled_event_id,
                        ),
                        Err(error) => terminal_nexus_call_failure_resolution(
                            error.to_string(),
                            "CallError",
                            true,
                            &endpoint_name,
                            &service,
                            &operation,
                            scheduled_event_id,
                        ),
                    }
                }
                EndpointTarget::Worker {
                    namespace_id,
                    task_queue,
                } => {
                    // Attach a completion callback so the Worker handler delivers the
                    // eventual async outcome back to this originator's pending op. The
                    // handler POSTs that completion itself, so the callback URL must be a
                    // concrete `http(s)://…/nexus/callback` address it can reach — our own
                    // inbound listener — NOT the `temporal://system` sentinel (a Worker SDK
                    // rejects a non-HTTP scheme: "unknown scheme: temporal://system"). This
                    // is the `UseSystemCallbackURL = false` shape (the callback-URL-template
                    // mode, `components/nexusoperations/executors.go:122-160 @ v1.31.0`):
                    // tokeira delivers Worker completions over HTTP to its own listener
                    // rather than via the SDK's system-callback internal route, so it always
                    // resolves the address up front. `request_id` reuses `operation_id` — the
                    // only routing key in scope here — matching how `StartOperation.request_id`
                    // is already populated below (a tokeira convention; v1.31.0's
                    // `NexusOperationCompletion.request_id` is a distinct field, documented in
                    // nexus-async-completion 3.1).
                    let token = crate::nexus::NexusCompletionToken {
                        originator_run_key,
                        operation_id: operation_id.clone(),
                        scheduled_event_id,
                        request_id: operation_id.clone(),
                        // Carried so the inbound completion handler can wrap a failed
                        // completion in NexusOperationFailureInfo without loading the run.
                        endpoint: endpoint_name.clone(),
                        service: service.clone(),
                        operation: operation.clone(),
                    };
                    let (callback_url, callback_token) = match token.encode() {
                        Ok(encoded) => (
                            Some(crate::nexus::system_callback_post_url(
                                &self.nexus_completion_config.system_callback_url,
                            )),
                            Some(encoded),
                        ),
                        Err(error) => {
                            // Encoding a small struct cannot realistically fail; if it
                            // somehow does, dispatch without a callback rather than
                            // dropping the operation (the caller would then rely on its
                            // schedule-to-close timeout). Never abort the dispatch.
                            tracing::warn!(
                                ?error,
                                operation_id = %operation_id,
                                "failed to encode nexus completion token; dispatching without callback"
                            );
                            (None, None)
                        }
                    };
                    let request = NexusTaskRequest::StartOperation {
                        service,
                        operation,
                        request_id: operation_id.clone(),
                        payload: input.0.first().cloned(),
                        scheduled_time: Some(scheduled_at),
                        callback_url,
                        callback_token,
                    };
                    self.nexus_broker
                        .publish_recoverable_workflow(
                            NexusQueueKey::from_version(
                                *namespace_id,
                                task_queue.clone(),
                                originator_version.as_ref(),
                            ),
                            originator_run_key,
                            operation_id,
                            scheduled_event_id,
                            delivery,
                            request,
                            NexusDeliveryAuthority::new(row.clone(), acquisition.clone()),
                        )
                        .await;
                    return Ok(());
                }
            },
            None => tokeira_kernel::NexusResolution::Failed {
                failure: failure_to_payload(&failure_proto::Failure {
                    message: format!("nexus endpoint not found: {endpoint_name}"),
                    failure_info: Some(
                        failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                            failure_proto::ApplicationFailureInfo {
                                r#type: "NexusOperationFailure".to_string(),
                                non_retryable: false,
                                ..Default::default()
                            },
                        ),
                    ),
                    ..Default::default()
                }),
            },
        };

        let command = Command::NexusOperationResolved(NexusOperationResolvedRequest {
            expected_attempt: Some(pending.attempt),
            operation_id,
            scheduled_event_id,
            resolution,
            now: OffsetDateTime::now_utc(),
        });
        self.source_outcome(row, command, acquisition, owner)
            .await?;
        Ok(())
    }

    async fn invoke_nexus_cancel(
        &self,
        row: &OperationRow,
        pending: PendingNexusOperation,
        originator_version: Option<WorkerDeploymentVersionRef>,
        acquisition: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
    ) -> Result<()> {
        let operation_id = pending.operation_id.clone();
        let endpoint_name = pending.endpoint.clone();
        let service = pending.service.clone();
        let originator_run_key = row.incarnation.key.source_run;
        let scheduled_event_id = pending.scheduled_event_id;
        let requested_event_id = row.incarnation.requested_event;
        let cancellation = pending
            .cancellation
            .as_ref()
            .ok_or(RecoveryError::Obsolete)?;
        ensure!(
            cancellation.requested_event_id == requested_event_id,
            RecoveryError::Obsolete
        );
        let delivery = NexusWorkflowAttempt {
            generation: cancellation.attempt,
            requested_event_id: Some(requested_event_id),
        };
        let Some(config) = self.nexus_registry.resolve(&endpoint_name) else {
            tracing::warn!(
                endpoint = endpoint_name,
                operation_id,
                "cancel nexus operation endpoint not found"
            );
            let failure =
                call_error_failure(format!("nexus endpoint not found: {endpoint_name}"), true);
            self.persist_nexus_cancel(
                row,
                acquisition,
                owner,
                &pending,
                requested_event_id,
                NexusCancellationAttemptOutcome::NonRetryableFailure {
                    failure: failure_to_payload(&failure),
                },
            )
            .await?;
            return Ok(());
        };

        match &config.target {
            EndpointTarget::External { address } => {
                let trace_headers = self.nexus_trace_headers();
                let started_at = std::time::Instant::now();
                let result = self
                    .nexus_client
                    .cancel_operation(
                        address,
                        &service,
                        &pending.operation,
                        &pending.operation_token,
                        &trace_headers,
                    )
                    .await;
                let latency = started_at.elapsed();
                let metric_outcome = match &result {
                    Ok(NexusCancelResult::Succeeded) => "successful".to_string(),
                    Ok(NexusCancelResult::HandlerError { error_type, .. }) => {
                        format!("handler-error:{error_type}")
                    }
                    Ok(NexusCancelResult::UnexpectedResponse { .. }) | Err(_) => {
                        "unknown-error".to_string()
                    }
                };
                self.record_external_outbound_metric(
                    originator_run_key,
                    "CancelOperation",
                    &metric_outcome,
                    latency,
                )
                .await;

                let now = OffsetDateTime::now_utc();
                let outcome = match result {
                    Ok(NexusCancelResult::Succeeded) => NexusCancellationAttemptOutcome::Succeeded,
                    Ok(NexusCancelResult::HandlerError {
                        error_type,
                        failure,
                        retryable,
                    }) => cancellation_failure_outcome(
                        &pending,
                        external_handler_failure(&error_type, failure.as_ref()),
                        retryable,
                        now,
                    ),
                    Ok(NexusCancelResult::UnexpectedResponse { message }) => {
                        cancellation_failure_outcome(
                            &pending,
                            call_error_failure(message, false),
                            true,
                            now,
                        )
                    }
                    Err(error) => cancellation_failure_outcome(
                        &pending,
                        call_error_failure(error.to_string(), false),
                        true,
                        now,
                    ),
                };
                self.persist_nexus_cancel(
                    row,
                    acquisition,
                    owner,
                    &pending,
                    requested_event_id,
                    outcome,
                )
                .await?;
            }
            EndpointTarget::Worker {
                namespace_id,
                task_queue,
            } => {
                let request = NexusTaskRequest::CancelOperation {
                    service,
                    operation_id: operation_id.clone(),
                    operation: pending.operation.clone(),
                    operation_token: pending.operation_token.clone(),
                };
                self.nexus_broker
                    .publish_recoverable_workflow(
                        NexusQueueKey::from_version(
                            *namespace_id,
                            task_queue.clone(),
                            originator_version.as_ref(),
                        ),
                        originator_run_key,
                        operation_id,
                        scheduled_event_id,
                        delivery,
                        request,
                        NexusDeliveryAuthority::new(row.clone(), acquisition.clone()),
                    )
                    .await;
            }
        }
        Ok(())
    }

    async fn persist_nexus_cancel(
        &self,
        row: &OperationRow,
        acquisition: &Attempt,
        owner: &Arc<RwLock<ShardOwner>>,
        pending: &PendingNexusOperation,
        requested_event_id: i64,
        outcome: NexusCancellationAttemptOutcome,
    ) -> Result<()> {
        self.source_outcome(
            row,
            Command::NexusCancellationAttempted(NexusCancellationAttemptedRequest {
                expected_attempt: row.incarnation.attempt,
                operation_id: pending.operation_id.clone(),
                scheduled_event_id: pending.scheduled_event_id,
                requested_event_id,
                outcome,
                now: OffsetDateTime::now_utc(),
            }),
            acquisition,
            owner,
        )
        .await?;
        Ok(())
    }
}
