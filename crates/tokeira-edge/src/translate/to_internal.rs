use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{
    ResetRequest, SignalRequest, SignalWithStartRequest, StartRequest, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::VersioningRuleStore;
use tokeira_types::{
    BuildId, DeploymentId, NamespaceId, QueueKey, RequestContext, RequestId as CoreRequestId,
    RunId, RunKey, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken,
    WorkflowType,
};
use uuid::Uuid;

use crate::{
    request_id::RequestId,
    translate::{
        PollWorkflowTaskQueueRequest, ResetWorkflowExecutionRequest,
        RespondWorkflowTaskCompletedRequest, SignalWorkflowExecutionRequest,
        StartWorkflowExecutionRequest, VersioningOverride,
    },
};

#[derive(Clone, Debug)]
pub struct PollInternalRequest {
    pub queue: QueueKey,
    pub worker_identity: WorkerIdentity,
    pub timeout: std::time::Duration,
}

pub fn namespace_id_for(name: &str) -> NamespaceId {
    let mut bytes = *b"tokeira-edge-ns!";
    for (idx, byte) in name.as_bytes().iter().enumerate() {
        let slot = idx % 16;
        bytes[slot] = bytes[slot]
            .wrapping_add(*byte)
            .rotate_left((idx % 8) as u32);
    }
    NamespaceId(Uuid::from_bytes(bytes))
}

pub fn start_request(
    req: StartWorkflowExecutionRequest,
    request_id: &RequestId,
    versioning_rules: Option<&VersioningRuleStore>,
) -> StartRequest {
    let now = req.now.unwrap_or_else(OffsetDateTime::now_utc);
    let run_id = req.run_id.unwrap_or_default();
    let namespace_id = namespace_id_for(&req.namespace);
    let workflow_id = WorkflowId(req.workflow_id);
    let task_queue = TaskQueueName(req.task_queue);
    let (deployment, build_id) = start_versioning(
        namespace_id,
        &workflow_id,
        &task_queue,
        req.versioning_override.as_ref(),
        versioning_rules,
    );
    let run_key = req
        .run_key
        .unwrap_or_else(|| RunKey::derive(namespace_id, &workflow_id, run_id));
    StartRequest {
        run_key,
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType(req.workflow_type),
        task_queue,
        input: req.input,
        header: req.header,
        memo: req.memo,
        search_attributes: req.search_attributes,
        workflow_execution_timeout: req.workflow_execution_timeout,
        workflow_run_timeout: req.workflow_run_timeout,
        workflow_task_timeout: req
            .workflow_task_timeout
            .unwrap_or(time::Duration::seconds(10)),
        retry_policy: req.retry_policy,
        conflict_policy: req.conflict_policy,
        reuse_policy: req.reuse_policy,
        deployment,
        build_id,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
        cron_schedule: None,
        reserved_poller_identity: None,
    }
}

pub fn signal_with_start_request(
    req: crate::translate::SignalWithStartWorkflowExecutionRequest,
    request_id: &RequestId,
    versioning_rules: Option<&VersioningRuleStore>,
) -> SignalWithStartRequest {
    let now = OffsetDateTime::now_utc();
    let run_id = RunId::new();
    let namespace_id = namespace_id_for(&req.namespace);
    let workflow_id = WorkflowId(req.workflow_id);
    let task_queue = TaskQueueName(req.task_queue);
    let (deployment, build_id) = start_versioning(
        namespace_id,
        &workflow_id,
        &task_queue,
        req.versioning_override.as_ref(),
        versioning_rules,
    );
    let run_key = RunKey::derive(namespace_id, &workflow_id, run_id);
    SignalWithStartRequest {
        run_key,
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType(req.workflow_type),
        task_queue,
        deployment,
        build_id,
        input: req.input,
        memo: req.memo,
        search_attributes: req.search_attributes,
        workflow_execution_timeout: req.workflow_execution_timeout,
        workflow_run_timeout: req.workflow_run_timeout,
        workflow_task_timeout: req
            .workflow_task_timeout
            .unwrap_or(time::Duration::seconds(10)),
        retry_policy: req.retry_policy,
        conflict_policy: req.conflict_policy,
        reuse_policy: req.reuse_policy,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
        signal_name: req.signal_name,
        signal_input: req.signal_input,
    }
}

fn start_versioning(
    namespace_id: NamespaceId,
    workflow_id: &WorkflowId,
    task_queue: &TaskQueueName,
    override_: Option<&VersioningOverride>,
    versioning_rules: Option<&VersioningRuleStore>,
) -> (Option<DeploymentId>, Option<BuildId>) {
    match override_ {
        Some(VersioningOverride::Pinned {
            deployment_series,
            build_id,
        }) => (
            Some(DeploymentId(deployment_series.clone())),
            Some(BuildId(build_id.clone())),
        ),
        Some(VersioningOverride::AutoUpgrade) | None => {
            let build_id = versioning_rules
                .and_then(|rules| rules.evaluate_assignment(namespace_id, task_queue, workflow_id));
            (None, build_id)
        }
    }
}

pub fn signal_request(
    req: SignalWorkflowExecutionRequest,
    request_id: &RequestId,
) -> SignalRequest {
    let now = req.now.unwrap_or_else(OffsetDateTime::now_utc);
    SignalRequest {
        signal_name: req.signal_name,
        input: req.input,
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
    }
}

pub fn poll_request(req: PollWorkflowTaskQueueRequest) -> PollInternalRequest {
    PollInternalRequest {
        queue: QueueKey {
            namespace_id: namespace_id_for(&req.namespace),
            task_queue: TaskQueueName(req.task_queue),
            task_kind: TaskKind::Workflow,
            deployment: req.deployment,
            build_id: req.build_id,
        },
        worker_identity: WorkerIdentity(req.worker_identity),
        timeout: req.timeout,
    }
}

pub fn workflow_task_completed_request(
    req: RespondWorkflowTaskCompletedRequest,
) -> Result<WorkflowTaskCompletedRequest> {
    let token: WorkflowTaskToken = serde_json::from_slice(&req.task_token)?;
    Ok(WorkflowTaskCompletedRequest {
        token,
        identity: WorkerIdentity(req.identity),
        sdk_metadata: req.sdk_metadata,
        worker_version: req.worker_version,
        commands: req.commands,
        force_new_workflow_task: req.force_create_new_workflow_task,
        now: OffsetDateTime::now_utc(),
    })
}

pub fn poll_activity_request(
    req: crate::translate::PollActivityTaskQueueRequest,
) -> PollInternalRequest {
    PollInternalRequest {
        queue: QueueKey {
            namespace_id: namespace_id_for(&req.namespace),
            task_queue: TaskQueueName(req.task_queue),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        },
        worker_identity: WorkerIdentity(req.worker_identity),
        timeout: req.timeout,
    }
}

pub fn terminate_request(
    req: crate::translate::TerminateWorkflowExecutionRequest,
    request_id: &RequestId,
) -> tokeira_kernel::TerminateRequest {
    let now = OffsetDateTime::now_utc();
    tokeira_kernel::TerminateRequest {
        reason: req.reason,
        details: req.details,
        identity: req.identity,
        request: RequestContext {
            request_id: CoreRequestId(request_id.as_str().to_string()),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}

pub fn pause_request(
    req: crate::translate::PauseWorkflowExecutionRequest,
    request_id: &RequestId,
) -> tokeira_kernel::PauseWorkflowRequest {
    let now = OffsetDateTime::now_utc();
    // Prefer the client-supplied request_id so idempotent pause retries are
    // recognised by the kernel's request-id-gated pause check; fall back to the
    // edge-assigned request id when the client omits one.
    let effective_request_id = req
        .request_id
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| request_id.as_str().to_string());
    tokeira_kernel::PauseWorkflowRequest {
        identity: req.identity,
        reason: req.reason,
        request: RequestContext {
            request_id: CoreRequestId(effective_request_id),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}

pub fn unpause_request(
    req: crate::translate::UnpauseWorkflowExecutionRequest,
    request_id: &RequestId,
) -> tokeira_kernel::UnpauseWorkflowRequest {
    let now = OffsetDateTime::now_utc();
    let effective_request_id = req
        .request_id
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| request_id.as_str().to_string());
    tokeira_kernel::UnpauseWorkflowRequest {
        identity: req.identity,
        reason: req.reason,
        request: RequestContext {
            request_id: CoreRequestId(effective_request_id),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}

pub fn cancel_request(
    req: crate::translate::RequestCancelWorkflowExecutionRequest,
    request_id: &RequestId,
) -> tokeira_kernel::CancelRequest {
    let now = OffsetDateTime::now_utc();
    tokeira_kernel::CancelRequest {
        reason: req.reason,
        external_initiator: None,
        request: RequestContext {
            request_id: CoreRequestId(request_id.as_str().to_string()),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;
    use tokeira_kernel::{WorkflowIdConflictPolicy, WorkflowIdReusePolicy};
    use tokeira_runtime::{AssignmentRule, VersioningMutation, VersioningRuleStore};
    use tokeira_types::{
        BuildId, DeploymentId, LogicalTaskSeq, Memo, Payloads, SearchAttributes, ShardEpoch,
    };

    use super::*;

    fn start_dto() -> StartWorkflowExecutionRequest {
        StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            workflow_type: "workflow-type".to_string(),
            task_queue: "queue".to_string(),
            input: Payloads::default(),
            request_id: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            identity: None,
            request_eager_execution: false,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: None,
            retry_policy: None,
            conflict_policy: WorkflowIdConflictPolicy::Fail,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            header: None,
            versioning_override: None,
            run_key: None,
            run_id: None,
            now: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    fn signal_with_start_dto() -> crate::translate::SignalWithStartWorkflowExecutionRequest {
        crate::translate::SignalWithStartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            workflow_type: "workflow-type".to_string(),
            task_queue: "queue".to_string(),
            input: Payloads::default(),
            request_id: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            identity: None,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: None,
            retry_policy: None,
            conflict_policy: WorkflowIdConflictPolicy::Fail,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            header: None,
            versioning_override: None,
            signal_name: "signal".to_string(),
            signal_input: Payloads::default(),
        }
    }

    fn store_with_assignment() -> VersioningRuleStore {
        let store = VersioningRuleStore::default();
        let namespace_id = namespace_id_for("default");
        let task_queue = TaskQueueName("queue".to_string());
        let token = store.get_rules(namespace_id, &task_queue).conflict_token;
        store
            .apply_mutation(
                namespace_id,
                &task_queue,
                token,
                VersioningMutation::InsertAssignmentRule {
                    rule: AssignmentRule {
                        target_build_id: "rule-build".to_string(),
                        percentage_ramp: None,
                        create_time: OffsetDateTime::UNIX_EPOCH,
                    },
                    index: 0,
                },
                OffsetDateTime::UNIX_EPOCH,
            )
            .unwrap();
        store
    }

    #[test]
    fn workflow_task_completed_preserves_shard_epoch_from_token() {
        let token = WorkflowTaskToken {
            run_key: RunKey::new(),
            logical_seq: LogicalTaskSeq(7),
            started_event_id: 11,
            attempt: 3,
            shard_epoch: ShardEpoch(42),
        };
        let req = RespondWorkflowTaskCompletedRequest {
            task_token: serde_json::to_vec(&token).unwrap(),
            identity: "worker-a".to_string(),
            sdk_metadata: None,
            worker_version: None,
            client_discards_speculative_with_events: false,
            commands: Vec::new(),
            return_new_workflow_task: false,
            force_create_new_workflow_task: false,
            query_results: std::collections::HashMap::new(),
            messages: Vec::new(),
        };

        let internal = workflow_task_completed_request(req).unwrap();

        assert_eq!(internal.token.shard_epoch, token.shard_epoch);
        assert_eq!(internal.token, token);
    }

    #[test]
    fn start_request_pinned_override_skips_assignment_rules() {
        let store = store_with_assignment();
        let mut dto = start_dto();
        dto.versioning_override = Some(VersioningOverride::Pinned {
            deployment_series: "series-a".to_string(),
            build_id: "pinned-build".to_string(),
        });

        let internal = start_request(dto, &RequestId::new("req"), Some(&store));

        assert_eq!(
            internal.deployment,
            Some(DeploymentId("series-a".to_string()))
        );
        assert_eq!(internal.build_id, Some(BuildId("pinned-build".to_string())));
    }

    #[test]
    fn start_request_evaluates_assignment_rules_without_pinned_override() {
        let store = store_with_assignment();

        let internal = start_request(start_dto(), &RequestId::new("req"), Some(&store));

        assert_eq!(internal.deployment, None);
        assert_eq!(internal.build_id, Some(BuildId("rule-build".to_string())));
    }

    #[test]
    fn signal_with_start_evaluates_assignment_rules() {
        let store = store_with_assignment();

        let internal = signal_with_start_request(
            signal_with_start_dto(),
            &RequestId::new("req"),
            Some(&store),
        );

        assert_eq!(internal.deployment, None);
        assert_eq!(internal.build_id, Some(BuildId("rule-build".to_string())));
    }
}

pub fn reset_request(req: ResetWorkflowExecutionRequest, request_id: &RequestId) -> ResetRequest {
    let now = OffsetDateTime::now_utc();
    ResetRequest {
        fork_event_id: req.workflow_task_finish_event_id,
        new_run_id: RunId::new(),
        reason: req.reason,
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}
