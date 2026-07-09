use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{
    ResetRequest, SignalRequest, SignalWithStartRequest, StartRequest,
    WorkflowTaskCompletedRequest,
    state::{VersioningOverride as KernelVersioningOverride, WorkerDeploymentVersionRef},
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
        CompletionCallback as EdgeCompletionCallback, Link as EdgeLink, LinkWorkflowEventReference,
        OnConflictOptions as EdgeOnConflictOptions, PollWorkflowTaskQueueRequest,
        Priority as EdgePriority, ResetWorkflowExecutionRequest,
        RespondWorkflowTaskCompletedRequest, SignalWorkflowExecutionRequest,
        StartWorkflowExecutionRequest, UserMetadata as EdgeUserMetadata, VersioningOverride,
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
    // `eager_worker_deployment_options` is an eager-delivery hint, not a
    // general start-routing override. Temporal only applies it when the client
    // actually asks for eager execution (`StartWorkflowExecutionRequest` field
    // 28 @ Temporal API v1.62.11).
    let (deployment, build_id) = if req.request_eager_execution {
        req.eager_worker_deployment_options
            .as_ref()
            .map(|version| {
                (
                    Some(DeploymentId(version.deployment_name.clone())),
                    Some(BuildId(version.build_id.clone())),
                )
            })
            .unwrap_or((deployment, build_id))
    } else {
        (deployment, build_id)
    };
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
        // A fresh client start is UNSPECIFIED unless it carries a cron schedule,
        // in which case its own WorkflowExecutionStarted.Initiator is
        // CRON_SCHEDULE (v1.31.0 first cron run; TestCronWorkflowCompletionStates
        // asserts `Initiator:3` on the initial run). Successors set their own.
        initiator: req
            .cron_schedule
            .as_deref()
            .filter(|cron| !cron.is_empty())
            .map(|_| tokeira_kernel::ContinueAsNewInitiator::CronSchedule),
        deployment,
        build_id,
        versioning_override: req
            .versioning_override
            .as_ref()
            .map(versioning_override_to_kernel),
        workflow_start_delay: req.workflow_start_delay,
        completion_callbacks: req
            .completion_callbacks
            .into_iter()
            .map(completion_callback_to_kernel)
            .collect(),
        user_metadata: req.user_metadata.map(user_metadata_to_kernel),
        links: req.links.into_iter().map(link_to_kernel).collect(),
        on_conflict_options: req.on_conflict_options.map(on_conflict_options_to_kernel),
        priority: req.priority.map(priority_to_kernel),
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
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
        client_cron_schedule: req.cron_schedule.clone(),
        cron_schedule: req.cron_schedule,
        reserved_poller_identity: None,
        // The runtime owns final admission (pinned enable + effective
        // first-WFT backoff); the edge only preserves the caller's candidate.
        eager_execution_accepted: req.request_eager_execution,
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
        versioning_override: req
            .versioning_override
            .as_ref()
            .map(versioning_override_to_kernel),
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
        // A fresh client start is UNSPECIFIED unless it carries a cron schedule,
        // in which case its own WorkflowExecutionStarted.Initiator is
        // CRON_SCHEDULE (v1.31.0 first cron run; TestCronWorkflowCompletionStates
        // asserts `Initiator:3` on the initial run). Successors set their own.
        initiator: req
            .cron_schedule
            .as_deref()
            .filter(|cron| !cron.is_empty())
            .map(|_| tokeira_kernel::ContinueAsNewInitiator::CronSchedule),
        header: req.header,
        attempt: 1,
        workflow_start_delay: req.workflow_start_delay,
        user_metadata: req.user_metadata.map(user_metadata_to_kernel),
        links: req.links.into_iter().map(link_to_kernel).collect(),
        priority: req.priority.map(priority_to_kernel),
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
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
        client_cron_schedule: req.cron_schedule.clone(),
        cron_schedule: req.cron_schedule,
        signal_name: req.signal_name,
        signal_input: req.signal_input,
    }
}

pub(crate) fn versioning_override_to_kernel(
    override_: &VersioningOverride,
) -> KernelVersioningOverride {
    match override_ {
        VersioningOverride::Pinned {
            deployment_series,
            build_id,
        } => KernelVersioningOverride::Pinned {
            version: WorkerDeploymentVersionRef {
                deployment_name: deployment_series.clone(),
                build_id: build_id.clone(),
            },
        },
        VersioningOverride::AutoUpgrade => KernelVersioningOverride::AutoUpgrade,
    }
}

fn user_metadata_to_kernel(metadata: EdgeUserMetadata) -> tokeira_kernel::state::UserMetadata {
    tokeira_kernel::state::UserMetadata {
        summary: metadata.summary,
        details: metadata.details,
    }
}

fn link_to_kernel(link: EdgeLink) -> tokeira_kernel::state::Link {
    match link {
        EdgeLink::WorkflowEvent {
            namespace,
            workflow_id,
            run_id,
            reference,
        } => tokeira_kernel::state::Link::WorkflowEvent {
            namespace,
            workflow_id,
            run_id,
            reference: reference.map(link_reference_to_kernel),
        },
        EdgeLink::BatchJob { job_id } => tokeira_kernel::state::Link::BatchJob { job_id },
        EdgeLink::Activity {
            namespace,
            activity_id,
            run_id,
        } => tokeira_kernel::state::Link::Activity {
            namespace,
            activity_id,
            run_id,
        },
        EdgeLink::NexusOperation {
            namespace,
            operation_id,
            run_id,
        } => tokeira_kernel::state::Link::NexusOperation {
            namespace,
            operation_id,
            run_id,
        },
    }
}

fn link_reference_to_kernel(
    reference: LinkWorkflowEventReference,
) -> tokeira_kernel::state::LinkWorkflowEventReference {
    match reference {
        LinkWorkflowEventReference::Event {
            event_id,
            event_type,
        } => tokeira_kernel::state::LinkWorkflowEventReference::Event {
            event_id,
            event_type,
        },
        LinkWorkflowEventReference::RequestId {
            request_id,
            event_type,
        } => tokeira_kernel::state::LinkWorkflowEventReference::RequestId {
            request_id,
            event_type,
        },
    }
}

fn completion_callback_to_kernel(
    callback: EdgeCompletionCallback,
) -> tokeira_kernel::state::CompletionCallback {
    tokeira_kernel::state::CompletionCallback {
        spec: tokeira_kernel::state::CallbackSpec::Nexus {
            url: callback.url,
            header: callback.header,
        },
        links: callback.links.into_iter().map(link_to_kernel).collect(),
        trigger: tokeira_kernel::state::CallbackTrigger::WorkflowClosed,
        registration_time: None,
        state: tokeira_kernel::state::CallbackState::Standby,
        attempt: 0,
        last_attempt_failure: None,
        next_attempt_at: None,
    }
}

fn priority_to_kernel(priority: EdgePriority) -> tokeira_kernel::state::Priority {
    tokeira_kernel::state::Priority {
        priority_key: priority.priority_key,
        fairness_key: priority.fairness_key,
        fairness_weight: priority.fairness_weight,
    }
}

fn on_conflict_options_to_kernel(
    options: EdgeOnConflictOptions,
) -> tokeira_kernel::state::OnConflictOptions {
    tokeira_kernel::state::OnConflictOptions {
        attach_request_id: options.attach_request_id,
        attach_completion_callbacks: options.attach_completion_callbacks,
        attach_links: options.attach_links,
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
        header: req.header,
        links: req.links.into_iter().map(link_to_kernel).collect(),
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
        metering_metadata: req.metering_metadata,
        worker_version: req.worker_version,
        versioning_behavior: req.versioning_behavior,
        deployment_version: req.deployment_version,
        worker_deployment_name: req.worker_deployment_name,
        sticky: req.sticky,
        commands: req.commands,
        client_discards_speculative_with_events: req.client_discards_speculative_with_events,
        force_new_workflow_task: req.force_create_new_workflow_task,
        // Filled by the runtime from its update registry before the commit
        // (server-side RejectUnprocessed, Req 9); the edge cannot see the
        // Sent set.
        delivered_update_ids: Vec::new(),
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
        external_initiated_event_id: 0,
        request: RequestContext {
            request_id: CoreRequestId(request_id.as_str().to_string()),
            // The request identity lands on the WorkflowExecutionCancelRequested
            // event verbatim (`Identity: request.CancelRequest.Identity`,
            // event_factory.go:578-590 @ v1.31.0).
            caller_identity: Some(req.identity),
            received_at: now,
        },
        now,
    }
}

pub fn reset_request(req: ResetWorkflowExecutionRequest, request_id: &RequestId) -> ResetRequest {
    let now = OffsetDateTime::now_utc();
    ResetRequest {
        fork_event_id: req.workflow_task_finish_event_id,
        new_run_id: RunId::new(),
        reapply_exclude_signal: req.reapply_exclude_signal,
        reapply_exclude_update: req.reapply_exclude_update,
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_kernel::{WorkflowIdConflictPolicy, WorkflowIdReusePolicy};
    use tokeira_runtime::{AssignmentRule, VersioningMutation, VersioningRuleStore};
    use tokeira_types::{
        BuildId, DeploymentId, Headers, LogicalTaskSeq, Memo, Payload, Payloads, RetryPolicy,
        SearchAttrValue, SearchAttributes, ShardEpoch,
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
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            eager_worker_deployment_options: None,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: None,
            retry_policy: None,
            conflict_policy: WorkflowIdConflictPolicy::Fail,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            header: None,
            versioning_override: None,
            on_conflict_options: None,
            priority: None,
            cron_schedule: None,
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
            workflow_start_delay: None,
            user_metadata: None,
            links: Vec::new(),
            versioning_override: None,
            priority: None,
            cron_schedule: None,
            signal_name: "signal".to_string(),
            signal_input: Payloads::default(),
        }
    }

    fn small_string() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9-]{0,16}".prop_map(String::from)
    }

    fn payload_strategy() -> impl Strategy<Value = Payload> {
        prop::collection::vec(any::<u8>(), 0..16).prop_map(Payload::new)
    }

    fn payloads_strategy() -> impl Strategy<Value = Payloads> {
        prop::collection::vec(payload_strategy(), 0..3).prop_map(Payloads)
    }

    fn memo_strategy() -> impl Strategy<Value = Memo> {
        prop::collection::btree_map(small_string(), payload_strategy(), 0..3).prop_map(Memo)
    }

    fn search_attributes_strategy() -> impl Strategy<Value = SearchAttributes> {
        prop::collection::btree_map(
            small_string(),
            small_string().prop_map(SearchAttrValue::Keyword),
            0..3,
        )
        .prop_map(SearchAttributes)
    }

    fn headers_strategy() -> impl Strategy<Value = Option<Headers>> {
        prop::option::of(
            prop::collection::btree_map(small_string(), payload_strategy(), 0..3).prop_map(Headers),
        )
    }

    fn retry_policy_strategy() -> impl Strategy<Value = Option<RetryPolicy>> {
        prop::option::of(
            (
                1i64..60,
                1.0f64..5.0,
                prop::option::of(1i64..120),
                1u32..10,
                prop::collection::vec(small_string(), 0..3),
            )
                .prop_map(
                    |(
                        initial_seconds,
                        backoff_coefficient,
                        max_seconds,
                        maximum_attempts,
                        non_retryable_error_types,
                    )| RetryPolicy {
                        initial_interval: time::Duration::seconds(initial_seconds),
                        backoff_coefficient,
                        maximum_interval: max_seconds.map(time::Duration::seconds),
                        maximum_attempts,
                        non_retryable_error_types,
                    },
                ),
        )
    }

    fn user_metadata_strategy() -> impl Strategy<Value = Option<EdgeUserMetadata>> {
        prop::option::of(
            (
                prop::option::of(payload_strategy()),
                prop::option::of(payload_strategy()),
            )
                .prop_map(|(summary, details)| EdgeUserMetadata { summary, details }),
        )
    }

    fn links_strategy() -> impl Strategy<Value = Vec<EdgeLink>> {
        prop::collection::vec(
            small_string().prop_map(|job_id| EdgeLink::BatchJob { job_id }),
            0..3,
        )
    }

    fn callback_strategy() -> impl Strategy<Value = EdgeCompletionCallback> {
        (
            small_string(),
            prop::collection::btree_map(small_string(), small_string(), 0..3),
            links_strategy(),
        )
            .prop_map(|(path, header, links)| EdgeCompletionCallback {
                url: format!("https://callback.example/{path}"),
                header,
                links,
            })
    }

    fn versioning_override_strategy() -> impl Strategy<Value = Option<VersioningOverride>> {
        prop::option::of(prop_oneof![
            (small_string(), small_string()).prop_map(|(deployment_series, build_id)| {
                VersioningOverride::Pinned {
                    deployment_series,
                    build_id,
                }
            }),
            Just(VersioningOverride::AutoUpgrade),
        ])
    }

    fn priority_strategy() -> impl Strategy<Value = Option<EdgePriority>> {
        prop::option::of((0i32..100, small_string(), 0.1f32..10.0).prop_map(
            |(priority_key, fairness_key, fairness_weight)| EdgePriority {
                priority_key,
                fairness_key,
                fairness_weight,
            },
        ))
    }

    fn delay_strategy() -> impl Strategy<Value = Option<time::Duration>> {
        prop::option::of(0i64..120).prop_map(|seconds| seconds.map(time::Duration::seconds))
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
    fn start_field_policy_table_covers_v1_62_11_proto_surface() {
        // Feature: api-conformance-start-fields, task 1.1.
        // The policy table is deliberately anchored to the vendored proto
        // fields rather than UNSUPPORTED_FIELDS.md so v1.62 additions cannot be
        // silently dropped by translation.
        const START_FIELDS: &[&str] = &[
            "namespace",
            "workflow_id",
            "workflow_type",
            "task_queue",
            "input",
            "workflow_execution_timeout",
            "workflow_run_timeout",
            "workflow_task_timeout",
            "identity",
            "request_id",
            "workflow_id_reuse_policy",
            "retry_policy",
            "cron_schedule",
            "memo",
            "search_attributes",
            "header",
            "request_eager_execution",
            "continued_failure",
            "last_completion_result",
            "workflow_start_delay",
            "completion_callbacks",
            "workflow_id_conflict_policy",
            "user_metadata",
            "links",
            "versioning_override",
            "on_conflict_options",
            "priority",
            "eager_worker_deployment_options",
            "time_skipping_config",
        ];
        const SIGNAL_WITH_START_FIELDS: &[&str] = &[
            "namespace",
            "workflow_id",
            "workflow_type",
            "task_queue",
            "input",
            "workflow_execution_timeout",
            "workflow_run_timeout",
            "workflow_task_timeout",
            "identity",
            "request_id",
            "workflow_id_reuse_policy",
            "signal_name",
            "signal_input",
            "control",
            "retry_policy",
            "cron_schedule",
            "memo",
            "search_attributes",
            "header",
            "workflow_start_delay",
            "workflow_id_conflict_policy",
            "user_metadata",
            "links",
            "versioning_override",
            "priority",
            "time_skipping_config",
        ];

        let start_policy = start_field_policy();
        let signal_policy = signal_with_start_field_policy();
        assert_field_policy_matches("StartWorkflowExecutionRequest", START_FIELDS, &start_policy);
        assert_field_policy_matches(
            "SignalWithStartWorkflowExecutionRequest",
            SIGNAL_WITH_START_FIELDS,
            &signal_policy,
        );
    }

    fn start_field_policy() -> BTreeSet<&'static str> {
        [
            "namespace",
            "workflow_id",
            "workflow_type",
            "task_queue",
            "input",
            "workflow_execution_timeout",
            "workflow_run_timeout",
            "workflow_task_timeout",
            "identity",
            "request_id",
            "workflow_id_reuse_policy",
            "retry_policy",
            "cron_schedule",
            "memo",
            "search_attributes",
            "header",
            "request_eager_execution",
            "continued_failure",
            "last_completion_result",
            "workflow_start_delay",
            "completion_callbacks",
            "workflow_id_conflict_policy",
            "user_metadata",
            "links",
            "versioning_override",
            "on_conflict_options",
            "priority",
            "eager_worker_deployment_options",
            "time_skipping_config",
        ]
        .into_iter()
        .collect()
    }

    fn signal_with_start_field_policy() -> BTreeSet<&'static str> {
        [
            "namespace",
            "workflow_id",
            "workflow_type",
            "task_queue",
            "input",
            "workflow_execution_timeout",
            "workflow_run_timeout",
            "workflow_task_timeout",
            "identity",
            "request_id",
            "workflow_id_reuse_policy",
            "signal_name",
            "signal_input",
            "control",
            "retry_policy",
            "cron_schedule",
            "memo",
            "search_attributes",
            "header",
            "workflow_start_delay",
            "workflow_id_conflict_policy",
            "user_metadata",
            "links",
            "versioning_override",
            "priority",
            "time_skipping_config",
        ]
        .into_iter()
        .collect()
    }

    fn assert_field_policy_matches(
        message: &str,
        proto_fields: &[&'static str],
        policy_fields: &BTreeSet<&'static str>,
    ) {
        let proto_fields = proto_fields.iter().copied().collect::<BTreeSet<_>>();
        let missing = proto_fields
            .difference(policy_fields)
            .copied()
            .collect::<Vec<_>>();
        let stale = policy_fields
            .difference(&proto_fields)
            .copied()
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty() && stale.is_empty(),
            "{message} field policy mismatch; missing={missing:?}, stale={stale:?}"
        );
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
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::state::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            resource_id: String::new(),
            worker_instance_key: String::new(),
            worker_control_task_queue: String::new(),
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
    fn start_request_applies_eager_deployment_options_only_for_eager_execution() {
        let store = store_with_assignment();
        let mut dto = start_dto();
        dto.eager_worker_deployment_options = Some(WorkerDeploymentVersionRef {
            deployment_name: "eager-deployment".to_string(),
            build_id: "eager-build".to_string(),
        });

        let non_eager = start_request(dto.clone(), &RequestId::new("req"), Some(&store));
        assert_eq!(non_eager.deployment, None);
        assert_eq!(non_eager.build_id, Some(BuildId("rule-build".to_string())));
        assert!(!non_eager.eager_execution_accepted);

        dto.request_eager_execution = true;
        let eager = start_request(dto, &RequestId::new("req"), Some(&store));
        assert_eq!(
            eager.deployment,
            Some(DeploymentId("eager-deployment".to_string()))
        );
        assert_eq!(eager.build_id, Some(BuildId("eager-build".to_string())));
        assert!(eager.eager_execution_accepted);
    }

    #[test]
    fn start_request_preserves_extended_start_fields() {
        let mut dto = start_dto();
        let mut callback_header = BTreeMap::new();
        callback_header.insert("x-callback".to_string(), "value".to_string());
        dto.workflow_start_delay = Some(time::Duration::seconds(7));
        dto.completion_callbacks = vec![EdgeCompletionCallback {
            url: "https://callback.example/run".to_string(),
            header: callback_header.clone(),
            links: vec![EdgeLink::BatchJob {
                job_id: "batch-1".to_string(),
            }],
        }];
        dto.user_metadata = Some(EdgeUserMetadata {
            summary: Some(Payload::new(b"summary".to_vec())),
            details: Some(Payload::new(b"details".to_vec())),
        });
        dto.links = vec![EdgeLink::WorkflowEvent {
            namespace: "default".to_string(),
            workflow_id: "source-workflow".to_string(),
            run_id: "source-run".to_string(),
            reference: Some(LinkWorkflowEventReference::RequestId {
                request_id: "source-request".to_string(),
                event_type: 3,
            }),
        }];
        dto.on_conflict_options = Some(EdgeOnConflictOptions {
            attach_request_id: true,
            attach_completion_callbacks: true,
            attach_links: true,
        });
        dto.priority = Some(EdgePriority {
            priority_key: 2,
            fairness_key: "tenant-a".to_string(),
            fairness_weight: 1.5,
        });

        let internal = start_request(dto, &RequestId::new("req"), None);

        assert_eq!(
            internal.workflow_start_delay,
            Some(time::Duration::seconds(7))
        );
        assert_eq!(internal.completion_callbacks.len(), 1);
        match &internal.completion_callbacks[0].spec {
            tokeira_kernel::state::CallbackSpec::Nexus { url, header } => {
                assert_eq!(url, "https://callback.example/run");
                assert_eq!(header, &callback_header);
            }
        }
        assert_eq!(internal.completion_callbacks[0].links.len(), 1);
        assert_eq!(
            internal
                .user_metadata
                .as_ref()
                .and_then(|metadata| metadata.summary.as_ref())
                .map(|payload| payload.data.as_slice()),
            Some(&b"summary"[..])
        );
        assert_eq!(internal.links.len(), 1);
        assert_eq!(
            internal.on_conflict_options,
            Some(tokeira_kernel::state::OnConflictOptions {
                attach_request_id: true,
                attach_completion_callbacks: true,
                attach_links: true,
            })
        );
        assert_eq!(
            internal.priority,
            Some(tokeira_kernel::state::Priority {
                priority_key: 2,
                fairness_key: "tenant-a".to_string(),
                fairness_weight: 1.5,
            })
        );
    }

    #[test]
    fn signal_request_preserves_header_and_links() {
        let mut header = BTreeMap::new();
        header.insert("x-signal".to_string(), Payload::new(b"metadata".to_vec()));
        let dto = SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            run_id: None,
            signal_name: "poke".to_string(),
            input: Payloads::default(),
            header: Some(Headers(header.clone())),
            links: vec![EdgeLink::BatchJob {
                job_id: "batch-1".to_string(),
            }],
            request_id: Some("signal-request".to_string()),
            identity: Some("tester".to_string()),
            now: Some(OffsetDateTime::UNIX_EPOCH),
        };

        let internal = signal_request(dto, &RequestId::new("fallback"));

        assert_eq!(internal.header, Some(Headers(header)));
        assert_eq!(internal.links.len(), 1);
        assert_eq!(internal.request.request_id.0, "signal-request");
    }

    proptest! {
        #[test]
        fn property_no_silent_drop_for_supported_start_fields(
            input in payloads_strategy(),
            memo in memo_strategy(),
            search_attributes in search_attributes_strategy(),
            identity in prop::option::of(small_string()),
            header in headers_strategy(),
            workflow_start_delay in delay_strategy(),
            callbacks in prop::collection::vec(callback_strategy(), 0..3),
            user_metadata in user_metadata_strategy(),
            links in links_strategy(),
            versioning_override in versioning_override_strategy(),
            priority in priority_strategy(),
            workflow_execution_timeout in delay_strategy(),
            workflow_run_timeout in delay_strategy(),
            workflow_task_timeout in delay_strategy(),
            retry_policy in retry_policy_strategy(),
        ) {
            // Feature: api-conformance-start-fields, Property 1: No Silent Drop.
            let mut dto = start_dto();
            dto.input = input.clone();
            dto.memo = memo.clone();
            dto.search_attributes = search_attributes.clone();
            dto.identity = identity.clone();
            dto.header = header.clone();
            dto.workflow_start_delay = workflow_start_delay;
            dto.completion_callbacks = callbacks.clone();
            dto.user_metadata = user_metadata.clone();
            dto.links = links.clone();
            dto.versioning_override = versioning_override.clone();
            dto.priority = priority.clone();
            dto.workflow_execution_timeout = workflow_execution_timeout;
            dto.workflow_run_timeout = workflow_run_timeout;
            dto.workflow_task_timeout = workflow_task_timeout;
            dto.retry_policy = retry_policy.clone();

            let internal = start_request(dto, &RequestId::new("prop-start"), None);

            prop_assert_eq!(internal.input, input);
            prop_assert_eq!(internal.memo, memo);
            prop_assert_eq!(internal.search_attributes, search_attributes);
            prop_assert_eq!(internal.request.caller_identity, identity);
            prop_assert_eq!(internal.header, header);
            prop_assert_eq!(internal.workflow_start_delay, workflow_start_delay);
            prop_assert_eq!(internal.completion_callbacks.len(), callbacks.len());
            for (actual, expected) in internal.completion_callbacks.iter().zip(callbacks.iter()) {
                match &actual.spec {
                    tokeira_kernel::state::CallbackSpec::Nexus { url, header } => {
                        prop_assert_eq!(url, &expected.url);
                        prop_assert_eq!(header, &expected.header);
                    }
                }
                prop_assert_eq!(actual.links.len(), expected.links.len());
            }
            prop_assert_eq!(internal.user_metadata.is_some(), user_metadata.is_some());
            prop_assert_eq!(internal.links.len(), links.len());
            prop_assert_eq!(internal.versioning_override.is_some(), versioning_override.is_some());
            prop_assert_eq!(internal.priority.as_ref().map(|p| p.priority_key), priority.as_ref().map(|p| p.priority_key));
            prop_assert_eq!(internal.priority.as_ref().map(|p| p.fairness_key.clone()), priority.as_ref().map(|p| p.fairness_key.clone()));
            prop_assert_eq!(internal.priority.as_ref().map(|p| p.fairness_weight), priority.as_ref().map(|p| p.fairness_weight));
            prop_assert_eq!(internal.workflow_execution_timeout, workflow_execution_timeout);
            prop_assert_eq!(internal.workflow_run_timeout, workflow_run_timeout);
            prop_assert_eq!(internal.workflow_task_timeout, workflow_task_timeout.unwrap_or(time::Duration::seconds(10)));
            prop_assert_eq!(internal.retry_policy, retry_policy);
        }

        #[test]
        fn property_start_signal_with_start_common_field_parity(
            input in payloads_strategy(),
            memo in memo_strategy(),
            search_attributes in search_attributes_strategy(),
            identity in prop::option::of(small_string()),
            header in headers_strategy(),
            workflow_start_delay in delay_strategy(),
            user_metadata in user_metadata_strategy(),
            links in links_strategy(),
            versioning_override in versioning_override_strategy(),
            priority in priority_strategy(),
            workflow_execution_timeout in delay_strategy(),
            workflow_run_timeout in delay_strategy(),
            workflow_task_timeout in delay_strategy(),
            retry_policy in retry_policy_strategy(),
        ) {
            // Feature: api-conformance-start-fields, Property 2: Start/SignalWithStart Parity.
            let mut start = start_dto();
            start.input = input.clone();
            start.memo = memo.clone();
            start.search_attributes = search_attributes.clone();
            start.identity = identity.clone();
            start.header = header.clone();
            start.workflow_start_delay = workflow_start_delay;
            start.user_metadata = user_metadata.clone();
            start.links = links.clone();
            start.versioning_override = versioning_override.clone();
            start.priority = priority.clone();
            start.workflow_execution_timeout = workflow_execution_timeout;
            start.workflow_run_timeout = workflow_run_timeout;
            start.workflow_task_timeout = workflow_task_timeout;
            start.retry_policy = retry_policy.clone();

            let mut signal = signal_with_start_dto();
            signal.input = input;
            signal.memo = memo;
            signal.search_attributes = search_attributes;
            signal.identity = identity;
            signal.header = header;
            signal.workflow_start_delay = workflow_start_delay;
            signal.user_metadata = user_metadata;
            signal.links = links;
            signal.versioning_override = versioning_override;
            signal.priority = priority;
            signal.workflow_execution_timeout = workflow_execution_timeout;
            signal.workflow_run_timeout = workflow_run_timeout;
            signal.workflow_task_timeout = workflow_task_timeout;
            signal.retry_policy = retry_policy;

            let start_internal = start_request(start, &RequestId::new("prop-start"), None);
            let signal_internal = signal_with_start_request(signal, &RequestId::new("prop-signal"), None);

            prop_assert_eq!(start_internal.workflow_type, signal_internal.workflow_type);
            prop_assert_eq!(start_internal.task_queue, signal_internal.task_queue);
            prop_assert_eq!(start_internal.input, signal_internal.input);
            prop_assert_eq!(start_internal.memo, signal_internal.memo);
            prop_assert_eq!(start_internal.search_attributes, signal_internal.search_attributes);
            prop_assert_eq!(start_internal.request.caller_identity, signal_internal.request.caller_identity);
            prop_assert_eq!(start_internal.header, signal_internal.header);
            prop_assert_eq!(start_internal.workflow_start_delay, signal_internal.workflow_start_delay);
            prop_assert_eq!(start_internal.user_metadata, signal_internal.user_metadata);
            prop_assert_eq!(start_internal.links, signal_internal.links);
            prop_assert_eq!(start_internal.versioning_override, signal_internal.versioning_override);
            prop_assert_eq!(start_internal.priority, signal_internal.priority);
            prop_assert_eq!(start_internal.workflow_execution_timeout, signal_internal.workflow_execution_timeout);
            prop_assert_eq!(start_internal.workflow_run_timeout, signal_internal.workflow_run_timeout);
            prop_assert_eq!(start_internal.workflow_task_timeout, signal_internal.workflow_task_timeout);
            prop_assert_eq!(start_internal.retry_policy, signal_internal.retry_policy);
        }
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
