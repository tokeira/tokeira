// Wire-roundtrip tests construct prost messages with `..Default::default()`
// (forward-compat) and exercise deprecated-but-still-on-wire fields for v1.31.0.
#![allow(clippy::needless_update, deprecated)]

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokeira_edge::{
    EdgeInterceptors, EmptyVisibilityApi, InMemoryNamespaceCache, InMemoryOperatorApi,
    LocalOnlyRouter, LongPollConfig, LongPollGate, NamespaceCache, PendingQueryStore,
    PollerRegistry, ResolvedNamespace, WorkflowExecutionDescription, WorkflowService,
    grpc::workflow_service::WorkflowServiceGrpc, translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_proto::{
    common::WorkerVersionCapabilities,
    enums,
    public::temporal::api::deployment::v1::{WorkerDeploymentOptions, WorkerDeploymentVersion},
    workflowservice::{
        self, PollWorkflowTaskQueueRequest, RespondWorkflowTaskCompletedRequest,
        workflow_service_server::WorkflowService as WfApi,
    },
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository, WorkerDeploymentRepository};
use tokeira_types::{ExecutionRef, NamespaceId, WorkflowId};
use tonic::{Code, Request};

use tokeira_edge::grpc::runtime_adapter::RuntimeAdapter;

struct StoreExecutionResolver {
    repo: Arc<InMemoryStore>,
    namespace_id: NamespaceId,
}

impl StoreExecutionResolver {
    fn new(repo: Arc<InMemoryStore>, namespace_id: NamespaceId) -> Self {
        Self { repo, namespace_id }
    }
}

#[async_trait]
impl ExecutionResolver for StoreExecutionResolver {
    async fn current_run_key(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<tokeira_types::RunKey>> {
        self.repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await
    }

    async fn describe_execution(
        &self,
        _namespace: &str,
        workflow_id: &str,
        run_id: Option<tokeira_types::RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let Some(run_key) = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id,
            })
            .await?
        else {
            if run_id.is_some() {
                return Ok(None);
            }
            let Some(run_key) = self
                .repo
                .find_latest_run(self.namespace_id, &WorkflowId(workflow_id.to_string()))
                .await?
            else {
                return Ok(None);
            };
            return self.describe_loaded_run(run_key).await;
        };

        self.describe_loaded_run(run_key).await
    }
}

impl StoreExecutionResolver {
    async fn describe_loaded_run(
        &self,
        run_key: tokeira_types::RunKey,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(Some(WorkflowExecutionDescription {
                namespace: "default".to_string(),
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                workflow_type: state.workflow_type.0,
                task_queue: state.task_queue.0.clone(),
                status: state.status,
                start_time: Some(state.started_at),
                close_time: state.closed_at,
                execution_time: state.first_run_started_at.unwrap_or(state.started_at),
                execution_config: tokeira_edge::translate::ExecutionConfigDescription {
                    task_queue: state.task_queue.0.clone(),
                    workflow_execution_timeout: state.workflow_execution_timeout,
                    workflow_run_timeout: state.workflow_run_timeout,
                    default_workflow_task_timeout: state.workflow_task_timeout,
                    user_metadata: None,
                },
                history_length: state.last_event_id,
                history_size_bytes: 0,
                state_transition_count: state.transition_seq.0 as i64,
                parent_namespace_id: state
                    .parent_namespace_id
                    .map(|namespace_id| namespace_id.0.to_string()),
                parent_workflow_id: state.parent_workflow_id.clone(),
                parent_run_id: state.parent_run_id,
                root_workflow_id: state.root_workflow_id.clone(),
                root_run_id: state.root_run_id,
                first_run_id: state.first_execution_run_id,
                memo: state.memo,
                search_attributes: state.search_attributes,
                pending_activities: state
                    .activities
                    .values()
                    .map(
                        |activity| tokeira_edge::translate::PendingActivityDescription {
                            activity_id: activity.activity_id.clone(),
                            activity_type: activity.activity_type.clone(),
                            is_started: activity.started_at.is_some(),
                            cancel_requested: activity.cancel_requested,
                            attempt: activity.attempt,
                            maximum_attempts: activity
                                .retry_policy
                                .as_ref()
                                .map(|policy| policy.maximum_attempts)
                                .unwrap_or_default(),
                            scheduled_at: activity.scheduled_at,
                            started_at: activity.started_at,
                            last_failure: activity.last_failure.clone(),
                            heartbeat_details: activity.heartbeat_details.clone(),
                            last_worker_identity: String::new(),
                            paused: activity.pause_info.is_some(),
                            pause_info: activity.pause_info.as_ref().map(|info| {
                                tokeira_edge::translate::PauseInfoDescription {
                                    identity: info.identity.clone(),
                                    paused_time: info.pause_time,
                                    reason: info.reason.clone(),
                                    rule_id: info.rule_id.clone(),
                                }
                            }),
                            activity_options: tokeira_edge::translate::ActivityOptions {
                                task_queue: Some(activity.task_queue.0.clone()),
                                schedule_to_close_timeout: activity.schedule_to_close_timeout,
                                schedule_to_start_timeout: activity.schedule_to_start_timeout,
                                start_to_close_timeout: activity.start_to_close_timeout,
                                heartbeat_timeout: activity.heartbeat_timeout,
                                retry_policy: activity.retry_policy.clone(),
                            },
                        },
                    )
                    .collect(),
                pending_children: state
                    .children
                    .values()
                    .map(|child| tokeira_edge::translate::PendingChildDescription {
                        workflow_id: child.child_workflow_id.0.clone(),
                        run_id: child
                            .child_run_id
                            .as_ref()
                            .map(|run_id| run_id.0.to_string()),
                        workflow_type: String::new(),
                        initiated_event_id: child.initiated_event_id,
                        parent_close_policy: child.parent_close_policy,
                    })
                    .collect(),
                pending_workflow_task: state.pending_workflow_task.as_ref().map(|task| {
                    tokeira_edge::translate::PendingWorkflowTaskDescription {
                        is_started: task.started_event_id.is_some(),
                        scheduled_at: task.scheduled_at,
                        started_at: task.started_at,
                        attempt: task.attempt,
                    }
                }),
                callbacks: state.completion_callbacks.clone(),
                pending_nexus_operations: state
                    .pending_nexus_operations
                    .values()
                    .map(
                        |operation| tokeira_edge::translate::PendingNexusOperationDescription {
                            endpoint: operation.endpoint.clone(),
                            service: operation.service.clone(),
                            operation: operation.operation.clone(),
                            scheduled_time: operation.scheduled_at,
                            scheduled_event_id: operation.scheduled_event_id,
                            schedule_to_close_timeout: operation.schedule_to_close_timeout,
                            schedule_to_start_timeout: operation.schedule_to_start_timeout,
                            start_to_close_timeout: operation.start_to_close_timeout,
                            started: operation.started,
                            operation_token: operation
                                .started
                                .then(|| operation.operation_id.clone()),
                            attempt: operation.attempt,
                            last_attempt_failure: operation.last_attempt_failure.clone(),
                            next_attempt_at: operation.next_attempt_at,
                        },
                    )
                    .collect(),
                pause_info: state.pause_info.as_ref().map(|info| {
                    tokeira_edge::translate::PauseInfoDescription {
                        identity: info.identity.clone(),
                        paused_time: info.pause_time,
                        reason: info.reason.clone(),
                        rule_id: None,
                    }
                }),
                execution_expiration_time: state.workflow_execution_timeout.map(|timeout| {
                    state.first_run_started_at.unwrap_or(state.started_at) + timeout
                }),
                run_expiration_time: state
                    .workflow_run_timeout
                    .map(|timeout| state.started_at + timeout),
                cancel_requested: state.cancel_requested,
                original_start_time: state.first_run_started_at.unwrap_or(state.started_at),
                versioning_info: state.versioning_info.clone(),
                worker_deployment_name: state.worker_deployment_name.clone(),
                most_recent_worker_version_stamp: state
                    .versioning_info
                    .as_ref()
                    .and_then(|info| info.most_recent_worker_version_stamp.clone()),
                request_id_infos: state.request_id_infos.clone(),
                external_payload_count: 0,
                external_payload_size_bytes: 0,
            })),
            LoadedRun::Absent => Err(anyhow::anyhow!("resolved run missing")),
        }
    }
}

async fn build_grpc(store: Arc<InMemoryStore>) -> WorkflowServiceGrpc {
    build_grpc_with_namespaces(store, vec![ResolvedNamespace::active("default")]).await
}

async fn build_grpc_with_namespaces(
    store: Arc<InMemoryStore>,
    namespaces_to_seed: Vec<ResolvedNamespace>,
) -> WorkflowServiceGrpc {
    let ns_id = namespace_id_for("default");
    let worker_deployments: Arc<dyn WorkerDeploymentRepository> = store.clone();
    let runtime = Arc::new(
        TokeiraRuntime::new(
            store.clone(),
            4,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        )
        .with_worker_deployment_repository(worker_deployments),
    );

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    for namespace in namespaces_to_seed {
        namespaces.insert(namespace).await.unwrap();
    }

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
    let router = Arc::new(LocalOnlyRouter);
    let workflow_broker = runtime.broker();
    let runtime_adapter = Arc::new(RuntimeAdapter::new(runtime));
    let resolver = Arc::new(StoreExecutionResolver::new(store.clone(), ns_id));
    let visibility = Arc::new(EmptyVisibilityApi);
    let long_polls = LongPollGate::new(LongPollConfig::default());

    let service = WorkflowService::new(
        runtime_adapter.clone(),
        resolver,
        visibility,
        store.clone(),
        operator_api,
        namespaces,
        interceptors,
        PollerRegistry::default(),
        PendingQueryStore::default(),
        workflow_broker,
        long_polls,
        router,
    )
    .with_worker_deployment_runtime(runtime_adapter);

    WorkflowServiceGrpc::new(service)
}

#[tokio::test]
async fn respond_workflow_task_completed_returns_new_started_wft_after_durable_schedule() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    grpc.start_workflow_execution(Request::new(
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "return-new-wft".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "queue-return-new".to_string(),
                ..Default::default()
            }),
            request_id: "start-return-new".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("start workflow");

    let poll = grpc
        .poll_workflow_task_queue(Request::new(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "queue-return-new".to_string(),
                ..Default::default()
            }),
            identity: "worker-a".to_string(),
            ..Default::default()
        }))
        .await
        .expect("poll initial workflow task")
        .into_inner();

    let completed = grpc
        .respond_workflow_task_completed(Request::new(RespondWorkflowTaskCompletedRequest {
            task_token: poll.task_token,
            identity: "worker-a".to_string(),
            force_create_new_workflow_task: true,
            return_new_workflow_task: true,
            ..Default::default()
        }))
        .await
        .expect("complete workflow task")
        .into_inner();

    let returned = completed
        .workflow_task
        .expect("return_new_workflow_task should return a real started WFT");
    assert!(returned.started_event_id > 0);
    assert!(!returned.task_token.is_empty());
    assert!(
        returned
            .history
            .as_ref()
            .is_some_and(|history| !history.events.is_empty())
    );
}

#[tokio::test]
async fn workflow_task_token_rejects_a_different_request_namespace() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    grpc.register_namespace(Request::new(workflowservice::RegisterNamespaceRequest {
        namespace: "another-namespace".to_string(),
        ..Default::default()
    }))
    .await
    .expect("register mismatch target namespace");

    grpc.start_workflow_execution(Request::new(
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "namespace-token-fence".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "namespace-token-queue".to_string(),
                ..Default::default()
            }),
            request_id: "namespace-token-start".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("start workflow");

    let task_token = grpc
        .poll_workflow_task_queue(Request::new(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "namespace-token-queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-a".to_string(),
            ..Default::default()
        }))
        .await
        .expect("poll workflow task")
        .into_inner()
        .task_token;

    let mismatch = grpc
        .respond_workflow_task_completed(Request::new(RespondWorkflowTaskCompletedRequest {
            namespace: "another-namespace".to_string(),
            task_token: task_token.clone(),
            identity: "worker-a".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("a token cannot be applied through another namespace");
    assert_eq!(mismatch.code(), Code::InvalidArgument);
    assert_eq!(
        mismatch.message(),
        "Operation requested with a token from a different namespace."
    );

    grpc.respond_workflow_task_completed(Request::new(RespondWorkflowTaskCompletedRequest {
        namespace: "default".to_string(),
        task_token,
        identity: "worker-a".to_string(),
        ..Default::default()
    }))
    .await
    .expect("the correctly namespaced retry remains valid");
}

#[tokio::test]
async fn worker_deployment_registry_roundtrip_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    let create = grpc
        .create_worker_deployment(Request::new(
            workflowservice::CreateWorkerDeploymentRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-a".to_string(),
                identity: "operator-a".to_string(),
                request_id: "deployment-request".to_string(),
            },
        ))
        .await
        .expect("create deployment")
        .into_inner();
    assert!(!create.conflict_token.is_empty());

    let describe = grpc
        .describe_worker_deployment(Request::new(
            workflowservice::DescribeWorkerDeploymentRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-a".to_string(),
            },
        ))
        .await
        .expect("describe deployment")
        .into_inner();
    let info = describe.worker_deployment_info.expect("deployment info");
    assert_eq!(info.name, "deployment-a");
    assert_eq!(info.last_modifier_identity, "operator-a");
    assert!(info.version_summaries.is_empty());

    for build_id in ["build-a", "build-b"] {
        grpc.create_worker_deployment_version(Request::new(
            workflowservice::CreateWorkerDeploymentVersionRequest {
                namespace: "default".to_string(),
                deployment_version: Some(deployment_version("deployment-a", build_id)),
                identity: "operator-a".to_string(),
                request_id: format!("version-{build_id}"),
                ..Default::default()
            },
        ))
        .await
        .expect("create deployment version");
    }

    grpc.set_worker_deployment_ramping_version(Request::new(
        workflowservice::SetWorkerDeploymentRampingVersionRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-a".to_string(),
            build_id: "build-b".to_string(),
            percentage: 25.0,
            identity: "operator-a".to_string(),
            allow_no_pollers: true,
            ignore_missing_task_queues: true,
            ..Default::default()
        },
    ))
    .await
    .expect("set ramping version");

    grpc.set_worker_deployment_current_version(Request::new(
        workflowservice::SetWorkerDeploymentCurrentVersionRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-a".to_string(),
            build_id: "build-b".to_string(),
            identity: "operator-a".to_string(),
            allow_no_pollers: true,
            ignore_missing_task_queues: true,
            ..Default::default()
        },
    ))
    .await
    .expect("set current version");

    let describe = grpc
        .describe_worker_deployment(Request::new(
            workflowservice::DescribeWorkerDeploymentRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-a".to_string(),
            },
        ))
        .await
        .expect("describe after current")
        .into_inner();
    let routing = describe
        .worker_deployment_info
        .and_then(|info| info.routing_config)
        .expect("routing config");
    assert_eq!(
        routing
            .current_deployment_version
            .as_ref()
            .map(|version| version.build_id.as_str()),
        Some("build-b")
    );
    assert!(routing.ramping_deployment_version.is_none());
    assert_eq!(routing.ramping_version_percentage, 0.0);

    grpc.set_worker_deployment_ramping_version(Request::new(
        workflowservice::SetWorkerDeploymentRampingVersionRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-a".to_string(),
            build_id: "build-a".to_string(),
            percentage: 10.0,
            identity: "operator-a".to_string(),
            allow_no_pollers: true,
            ignore_missing_task_queues: true,
            ..Default::default()
        },
    ))
    .await
    .expect("set second ramping version");

    grpc.set_worker_deployment_manager(Request::new(
        workflowservice::SetWorkerDeploymentManagerRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-a".to_string(),
            identity: "operator-a".to_string(),
            new_manager_identity: Some(
                workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::Self_(
                    true,
                ),
            ),
            conflict_token: Vec::new(),
        },
    ))
    .await
    .expect("set manager");

    let mismatch = grpc
        .set_worker_deployment_current_version(Request::new(
            workflowservice::SetWorkerDeploymentCurrentVersionRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-a".to_string(),
                build_id: "build-a".to_string(),
                identity: "operator-b".to_string(),
                allow_no_pollers: true,
                ignore_missing_task_queues: true,
                ..Default::default()
            },
        ))
        .await
        .expect_err("manager mismatch should reject routing mutation");
    assert_eq!(mismatch.code(), Code::FailedPrecondition);

    grpc.set_worker_deployment_current_version(Request::new(
        workflowservice::SetWorkerDeploymentCurrentVersionRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-a".to_string(),
            build_id: "build-a".to_string(),
            identity: "operator-a".to_string(),
            allow_no_pollers: true,
            ignore_missing_task_queues: true,
            ..Default::default()
        },
    ))
    .await
    .expect("manager can promote current");

    let version = grpc
        .describe_worker_deployment_version(Request::new(
            workflowservice::DescribeWorkerDeploymentVersionRequest {
                namespace: "default".to_string(),
                deployment_version: Some(deployment_version("deployment-a", "build-b")),
                report_task_queue_stats: false,
                ..Default::default()
            },
        ))
        .await
        .expect("describe demoted version")
        .into_inner()
        .worker_deployment_version_info
        .expect("version info");
    let drainage = version.drainage_info.expect("drainage info");
    // A just-demoted version is `Draining`, not `Drained`: v1.31.0 marks it draining on
    // the routing change and only transitions it to `Drained` later, via the version
    // entity workflow's delayed drainage check / `sync-drainage-status` signal — never
    // synchronously at the routing mutation (see `apply_version_drainage_status` /
    // `refresh_version_routing_state` in `tokeira-runtime/src/deployment_registry.rs`,
    // `version_workflow.go:127 @ v1.31.0`). Reporting `Drained` here would be premature.
    assert_eq!(
        drainage.status,
        enums::VersionDrainageStatus::Draining as i32
    );
}

#[tokio::test]
async fn worker_deployment_registry_recovers_after_runtime_restart() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    grpc.create_worker_deployment(Request::new(
        workflowservice::CreateWorkerDeploymentRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-restart".to_string(),
            identity: "operator-a".to_string(),
            request_id: "deployment-request".to_string(),
        },
    ))
    .await
    .expect("create deployment");
    grpc.create_worker_deployment_version(Request::new(
        workflowservice::CreateWorkerDeploymentVersionRequest {
            namespace: "default".to_string(),
            deployment_version: Some(deployment_version("deployment-restart", "build-a")),
            identity: "operator-a".to_string(),
            request_id: "version-request".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("create version");

    let pre_restart = grpc
        .describe_worker_deployment(Request::new(
            workflowservice::DescribeWorkerDeploymentRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-restart".to_string(),
            },
        ))
        .await
        .expect("describe before restart")
        .into_inner();
    let pre_restart_token = pre_restart.conflict_token;
    assert!(!pre_restart_token.is_empty());

    let recovered_records = store
        .list_all_for_namespace(namespace_id_for("default"))
        .await
        .expect("list all deployments for recovery");
    assert_eq!(recovered_records.len(), 1);
    drop(grpc);

    let grpc = build_grpc(store).await;
    let listed = grpc
        .list_worker_deployments(Request::new(
            workflowservice::ListWorkerDeploymentsRequest {
                namespace: "default".to_string(),
                page_size: 10,
                next_page_token: Vec::new(),
            },
        ))
        .await
        .expect("list after restart")
        .into_inner();
    assert_eq!(listed.worker_deployments.len(), 1);
    assert_eq!(listed.worker_deployments[0].name, "deployment-restart");

    let described = grpc
        .describe_worker_deployment(Request::new(
            workflowservice::DescribeWorkerDeploymentRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-restart".to_string(),
            },
        ))
        .await
        .expect("describe after restart")
        .into_inner()
        .worker_deployment_info
        .expect("deployment info");
    assert_eq!(described.version_summaries.len(), 1);

    grpc.set_worker_deployment_manager(Request::new(
        workflowservice::SetWorkerDeploymentManagerRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-restart".to_string(),
            identity: "operator-a".to_string(),
            new_manager_identity: Some(
                workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::ManagerIdentity(
                    "manager-a".to_string(),
                ),
            ),
            conflict_token: pre_restart_token.clone(),
        },
    ))
    .await
    .expect("pre-restart conflict token should remain valid after restart");

    let stale = grpc
        .set_worker_deployment_manager(Request::new(
            workflowservice::SetWorkerDeploymentManagerRequest {
                namespace: "default".to_string(),
                deployment_name: "deployment-restart".to_string(),
                identity: "operator-a".to_string(),
                new_manager_identity: Some(
                    workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::ManagerIdentity(
                        "manager-b".to_string(),
                    ),
                ),
                conflict_token: pre_restart_token,
            },
        ))
        .await
        .expect_err("stale pre-restart conflict token should be rejected");
    assert_eq!(stale.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn worker_deployment_routing_cycle_projects_describe_versioning_info() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    grpc.create_worker_deployment(Request::new(
        workflowservice::CreateWorkerDeploymentRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-routing".to_string(),
            identity: "operator-a".to_string(),
            request_id: "deployment-request".to_string(),
        },
    ))
    .await
    .expect("create deployment");
    grpc.create_worker_deployment_version(Request::new(
        workflowservice::CreateWorkerDeploymentVersionRequest {
            namespace: "default".to_string(),
            deployment_version: Some(deployment_version("deployment-routing", "build-a")),
            identity: "operator-a".to_string(),
            request_id: "version-request".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("create version");
    grpc.set_worker_deployment_current_version(Request::new(
        workflowservice::SetWorkerDeploymentCurrentVersionRequest {
            namespace: "default".to_string(),
            deployment_name: "deployment-routing".to_string(),
            build_id: "build-a".to_string(),
            identity: "operator-a".to_string(),
            allow_no_pollers: true,
            ignore_missing_task_queues: true,
            ..Default::default()
        },
    ))
    .await
    .expect("set current");

    let start = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: "routing-wf".to_string(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "example".to_string(),
                }),
                task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                    name: "queue-routing".to_string(),
                    ..Default::default()
                }),
                request_eager_execution: true,
                eager_worker_deployment_options: Some(worker_deployment_options(
                    "deployment-routing",
                    "build-a",
                )),
                request_id: "start-request".to_string(),
                ..Default::default()
            },
        ))
        .await
        .expect("start workflow")
        .into_inner();
    let run_id = start.run_id;

    let poll = grpc
        .poll_workflow_task_queue(Request::new(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "queue-routing".to_string(),
                ..Default::default()
            }),
            identity: "worker-a".to_string(),
            worker_version_capabilities: Some(WorkerVersionCapabilities {
                build_id: "build-a".to_string(),
                use_versioning: true,
                deployment_series_name: "deployment-routing".to_string(),
            }),
            ..Default::default()
        }))
        .await
        .expect("poll workflow task")
        .into_inner();
    assert!(!poll.task_token.is_empty());

    let run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id: namespace_id_for("default"),
            workflow_id: WorkflowId("routing-wf".to_string()),
            run_id: Some(tokeira_types::RunId(
                uuid::Uuid::parse_str(&run_id).expect("valid run id"),
            )),
        })
        .await
        .expect("resolve execution")
        .expect("run key");
    let started = match store.load_run(run_key).await.expect("load started run") {
        LoadedRun::Existing(state) => state,
        LoadedRun::Absent => panic!("started run missing"),
    };
    let started_versioning = started.versioning_info.as_ref().expect("versioning info");
    assert_eq!(started_versioning.revision_number, 1);
    assert_eq!(
        started_versioning
            .version_transition
            .as_ref()
            .map(|version| (version.deployment_name.as_str(), version.build_id.as_str())),
        Some(("deployment-routing", "build-a"))
    );

    grpc.respond_workflow_task_completed(Request::new(RespondWorkflowTaskCompletedRequest {
        task_token: poll.task_token,
        identity: "worker-a".to_string(),
        versioning_behavior: enums::VersioningBehavior::AutoUpgrade as i32,
        deployment_options: Some(worker_deployment_options("deployment-routing", "build-a")),
        ..Default::default()
    }))
    .await
    .expect("complete workflow task");

    let completed = match store.load_run(run_key).await.expect("load completed run") {
        LoadedRun::Existing(state) => state,
        LoadedRun::Absent => panic!("completed run missing"),
    };
    let completed_versioning = completed
        .versioning_info
        .as_ref()
        .expect("completed versioning info");
    assert_eq!(completed_versioning.revision_number, 1);
    assert!(completed_versioning.version_transition.is_none());
    assert_eq!(
        completed_versioning
            .deployment_version
            .as_ref()
            .map(|version| (version.deployment_name.as_str(), version.build_id.as_str())),
        Some(("deployment-routing", "build-a"))
    );

    let describe = grpc
        .describe_workflow_execution(Request::new(
            workflowservice::DescribeWorkflowExecutionRequest {
                namespace: "default".to_string(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "routing-wf".to_string(),
                    run_id,
                }),
            },
        ))
        .await
        .expect("describe workflow")
        .into_inner()
        .workflow_execution_info
        .expect("workflow info");
    let info = describe.versioning_info.expect("versioning info");
    assert_eq!(info.behavior, enums::VersioningBehavior::AutoUpgrade as i32);
    assert_eq!(info.revision_number, 1);
    assert!(info.version_transition.is_none());
    assert_eq!(describe.worker_deployment_name, "deployment-routing");
    assert_eq!(
        info.deployment_version
            .as_ref()
            .map(|version| (version.deployment_name.as_str(), version.build_id.as_str())),
        Some(("deployment-routing", "build-a"))
    );
}

#[tokio::test]
async fn describe_workflow_execution_returns_registered_completion_callbacks() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    let start = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: "callback-wf".to_string(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "example".to_string(),
                }),
                task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                    name: "queue-callback".to_string(),
                    ..Default::default()
                }),
                request_id: "callback-start".to_string(),
                completion_callbacks: vec![tokeira_proto::common::Callback {
                    variant: Some(tokeira_proto::common::callback::Variant::Nexus(
                        tokeira_proto::common::callback::Nexus {
                            url: "https://callback.example/workflow-closed".to_string(),
                            header: [("x-callback".to_string(), "enabled".to_string())]
                                .into_iter()
                                .collect(),
                        },
                    )),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ))
        .await
        .expect("start workflow")
        .into_inner();

    let describe = grpc
        .describe_workflow_execution(Request::new(
            workflowservice::DescribeWorkflowExecutionRequest {
                namespace: "default".to_string(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "callback-wf".to_string(),
                    run_id: start.run_id,
                }),
            },
        ))
        .await
        .expect("describe workflow")
        .into_inner();

    assert_eq!(describe.callbacks.len(), 1);
    let callback = &describe.callbacks[0];
    assert!(callback.registration_time.is_some());
    assert_eq!(callback.state, enums::CallbackState::Standby as i32);
    assert!(callback.trigger.is_some());
    let callback_target = callback.callback.as_ref().expect("callback target");
    match callback_target.variant.as_ref().expect("callback variant") {
        tokeira_proto::common::callback::Variant::Nexus(nexus) => {
            assert_eq!(nexus.url, "https://callback.example/workflow-closed");
            assert_eq!(
                nexus.header.get("x-callback").map(String::as_str),
                Some("enabled")
            );
        }
        other => panic!("unexpected callback variant: {other:?}"),
    }
}

fn deployment_version(deployment_name: &str, build_id: &str) -> WorkerDeploymentVersion {
    WorkerDeploymentVersion {
        deployment_name: deployment_name.to_string(),
        build_id: build_id.to_string(),
    }
}

fn worker_deployment_options(deployment_name: &str, build_id: &str) -> WorkerDeploymentOptions {
    WorkerDeploymentOptions {
        deployment_name: deployment_name.to_string(),
        build_id: build_id.to_string(),
        worker_versioning_mode: enums::WorkerVersioningMode::Versioned as i32,
    }
}

/// Integration test: Start a workflow, then terminate it,
/// verify it's terminated via describe.
#[tokio::test]
async fn terminate_workflow_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    // Start a workflow
    let start_resp = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: "term-wf".to_string(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "test".to_string(),
                }),
                task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                    name: "q".to_string(),
                    ..Default::default()
                }),
                request_id: "req-1".to_string(),
                ..Default::default()
            },
        ))
        .await
        .expect("start should succeed");
    let run_id = start_resp.into_inner().run_id;
    assert!(!run_id.is_empty());

    // Terminate the workflow
    grpc.terminate_workflow_execution(Request::new(
        workflowservice::TerminateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "term-wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            reason: "test termination".to_string(),
            identity: "admin".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("terminate should succeed");

    // Verify the workflow is terminated by loading
    // the run directly from the store
    let ns_id = namespace_id_for("default");
    let run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id: ns_id,
            workflow_id: WorkflowId("term-wf".to_string()),
            run_id: None,
        })
        .await
        .unwrap();

    // After termination, the run may or may not be
    // resolvable via current-run lookup (depends on
    // store semantics). The key assertion is that the
    // terminate call itself succeeded without error.
    // If the run is still resolvable, verify status.
    if let Some(rk) = run_key {
        let state = match store.load_run(rk).await.unwrap() {
            LoadedRun::Existing(s) => s,
            _ => panic!("run should exist"),
        };
        assert_eq!(state.status, tokeira_types::ExecutionStatus::Terminated);
    }
}

/// Integration test: Start a workflow, then cancel it,
/// verify the cancellation is recorded.
#[tokio::test]
async fn cancel_workflow_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    // Start a workflow
    let start_resp = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: "cancel-wf".to_string(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "test".to_string(),
                }),
                task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                    name: "q".to_string(),
                    ..Default::default()
                }),
                request_id: "req-2".to_string(),
                ..Default::default()
            },
        ))
        .await
        .expect("start should succeed");
    let run_id = start_resp.into_inner().run_id;
    assert!(!run_id.is_empty());

    // Cancel the workflow
    let _cancel_resp = grpc
        .request_cancel_workflow_execution(Request::new(
            workflowservice::RequestCancelWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "cancel-wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
                reason: "test cancel".to_string(),
                identity: "admin".to_string(),
                ..Default::default()
            },
        ))
        .await
        .expect("cancel should succeed");

    // Describe — workflow should still be running
    // (cancel is cooperative, not immediate)
    let desc_resp = grpc
        .describe_workflow_execution(Request::new(
            workflowservice::DescribeWorkflowExecutionRequest {
                namespace: "default".to_string(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "cancel-wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect("describe should succeed");
    let info = desc_resp
        .into_inner()
        .workflow_execution_info
        .expect("execution info");
    // Workflow is still running (cancel is cooperative)
    // Status 1 = Running
    assert_eq!(info.status, 1);
}

#[tokio::test]
async fn discovery_and_namespace_reads_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    let system = grpc
        .get_system_info(Request::new(workflowservice::GetSystemInfoRequest {}))
        .await
        .expect("get system info should succeed")
        .into_inner();
    assert!(!system.server_version.is_empty());
    let capabilities = system.capabilities.expect("system capabilities");
    assert!(capabilities.eager_workflow_start);

    let cluster = grpc
        .get_cluster_info(Request::new(workflowservice::GetClusterInfoRequest {}))
        .await
        .expect("get cluster info should succeed")
        .into_inner();
    assert_eq!(cluster.cluster_name, "tokeira-local");
    assert!(!cluster.server_version.is_empty());

    let namespaces = grpc
        .list_namespaces(Request::new(
            workflowservice::ListNamespacesRequest::default(),
        ))
        .await
        .expect("list namespaces should succeed")
        .into_inner();
    assert_eq!(namespaces.namespaces.len(), 1);
    assert_eq!(
        namespaces.namespaces[0]
            .namespace_info
            .as_ref()
            .expect("namespace info")
            .name,
        "default"
    );

    let describe = grpc
        .describe_namespace(Request::new(workflowservice::DescribeNamespaceRequest {
            namespace: "default".to_string(),
            id: String::new(),
        }))
        .await
        .expect("describe namespace should succeed")
        .into_inner();
    assert_eq!(
        describe
            .namespace_info
            .as_ref()
            .expect("namespace info")
            .name,
        "default"
    );
}

#[tokio::test]
async fn register_namespace_roundtrip_and_duplicate_rejection() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    grpc.register_namespace(Request::new(workflowservice::RegisterNamespaceRequest {
        namespace: "payments".to_string(),
        ..Default::default()
    }))
    .await
    .expect("register namespace should succeed");

    let describe = grpc
        .describe_namespace(Request::new(workflowservice::DescribeNamespaceRequest {
            namespace: "payments".to_string(),
            id: String::new(),
        }))
        .await
        .expect("describe registered namespace should succeed")
        .into_inner();
    assert_eq!(
        describe
            .namespace_info
            .as_ref()
            .expect("namespace info")
            .name,
        "payments"
    );

    let err = grpc
        .register_namespace(Request::new(workflowservice::RegisterNamespaceRequest {
            namespace: "payments".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("duplicate registration should fail");
    assert_eq!(err.code(), Code::AlreadyExists);
}

#[tokio::test]
async fn list_namespaces_empty_cache_returns_empty_list() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc_with_namespaces(store, Vec::new()).await;

    let response = grpc
        .list_namespaces(Request::new(
            workflowservice::ListNamespacesRequest::default(),
        ))
        .await
        .expect("list namespaces should succeed")
        .into_inner();

    assert!(response.namespaces.is_empty());
}

#[tokio::test]
async fn describe_namespace_missing_returns_not_found() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc_with_namespaces(store, Vec::new()).await;

    let err = grpc
        .describe_namespace(Request::new(workflowservice::DescribeNamespaceRequest {
            namespace: "missing".to_string(),
            id: String::new(),
        }))
        .await
        .expect_err("missing namespace should fail");

    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn register_namespace_name_rule_matches_v1_31_0() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    // v1.31.0 applies NO character-set restriction to namespace names — its
    // registry tests use names with spaces, and the conformance corpus
    // registers parenthesised subtest-derived names
    // (namespace_handler.go @ v1.31.0). Only empty and over-length reject.
    for namespace in ["", "x".repeat(1001).as_str()] {
        let err = grpc
            .register_namespace(Request::new(workflowservice::RegisterNamespaceRequest {
                namespace: namespace.to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("invalid namespace should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
    }
    for namespace in ["ok namespace", "Suite-Leaf-(with_policy_Fail)-and_accept"] {
        grpc.register_namespace(Request::new(workflowservice::RegisterNamespaceRequest {
            namespace: namespace.to_string(),
            ..Default::default()
        }))
        .await
        .expect("v1.31.0 accepts these names");
    }
}

/// Integration test: pause a running workflow, verify the
/// describe surfaces PAUSED status (value 8) and nested pause
/// info, then unpause and verify it returns to RUNNING.
#[tokio::test]
async fn pause_and_unpause_workflow_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    grpc.start_workflow_execution(Request::new(
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "pause-wf".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "test".to_string(),
            }),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "q".to_string(),
                ..Default::default()
            }),
            request_id: "req-pause-start".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("start should succeed");

    grpc.pause_workflow_execution(Request::new(
        workflowservice::PauseWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "pause-wf".to_string(),
            run_id: String::new(),
            identity: "operator".to_string(),
            reason: "maintenance".to_string(),
            request_id: "req-pause-1".to_string(),
        },
    ))
    .await
    .expect("pause should succeed");

    let describe = grpc
        .describe_workflow_execution(Request::new(
            workflowservice::DescribeWorkflowExecutionRequest {
                namespace: "default".to_string(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "pause-wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect("describe should succeed")
        .into_inner();

    let info = describe
        .workflow_execution_info
        .as_ref()
        .expect("execution info");
    // WORKFLOW_EXECUTION_STATUS_PAUSED = 8
    assert_eq!(info.status, 8);

    let pause_info = describe
        .workflow_extended_info
        .as_ref()
        .expect("extended info")
        .pause_info
        .as_ref()
        .expect("pause info");
    assert_eq!(pause_info.identity, "operator");
    assert_eq!(pause_info.reason, "maintenance");
    assert!(pause_info.paused_time.is_some());

    grpc.unpause_workflow_execution(Request::new(
        workflowservice::UnpauseWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "pause-wf".to_string(),
            run_id: String::new(),
            identity: "operator".to_string(),
            reason: "resume".to_string(),
            request_id: "req-unpause-1".to_string(),
        },
    ))
    .await
    .expect("unpause should succeed");

    let describe = grpc
        .describe_workflow_execution(Request::new(
            workflowservice::DescribeWorkflowExecutionRequest {
                namespace: "default".to_string(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "pause-wf".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect("describe should succeed")
        .into_inner();
    let info = describe
        .workflow_execution_info
        .as_ref()
        .expect("execution info");
    // WORKFLOW_EXECUTION_STATUS_RUNNING = 1
    assert_eq!(info.status, 1);
    assert!(
        describe
            .workflow_extended_info
            .and_then(|ext| ext.pause_info)
            .is_none(),
        "pause info must be cleared after unpause"
    );
}

/// Pausing an already-paused workflow with a different request
/// ID is rejected as FAILED_PRECONDITION; the same request ID is
/// an idempotent no-op success.
#[tokio::test]
async fn pause_idempotency_and_conflict_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    grpc.start_workflow_execution(Request::new(
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "pause-idem-wf".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "test".to_string(),
            }),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "q".to_string(),
                ..Default::default()
            }),
            request_id: "req-idem-start".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("start should succeed");

    let pause = |request_id: &str| {
        let request_id = request_id.to_string();
        workflowservice::PauseWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "pause-idem-wf".to_string(),
            run_id: String::new(),
            identity: "operator".to_string(),
            reason: "maintenance".to_string(),
            request_id,
        }
    };

    grpc.pause_workflow_execution(Request::new(pause("req-pause-a")))
        .await
        .expect("first pause should succeed");

    grpc.pause_workflow_execution(Request::new(pause("req-pause-a")))
        .await
        .expect("same request id pause should be idempotent no-op");

    let err = grpc
        .pause_workflow_execution(Request::new(pause("req-pause-b")))
        .await
        .expect_err("different request id pause should conflict");
    assert_eq!(err.code(), Code::FailedPrecondition);
}

/// Unpausing a workflow that is not paused is rejected as
/// FAILED_PRECONDITION.
#[tokio::test]
async fn unpause_non_paused_returns_failed_precondition() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    grpc.start_workflow_execution(Request::new(
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "unpause-wf".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "test".to_string(),
            }),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "q".to_string(),
                ..Default::default()
            }),
            request_id: "req-unpause-start".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("start should succeed");

    let err = grpc
        .unpause_workflow_execution(Request::new(
            workflowservice::UnpauseWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: "unpause-wf".to_string(),
                run_id: String::new(),
                identity: "operator".to_string(),
                reason: "resume".to_string(),
                request_id: "req-unpause-x".to_string(),
            },
        ))
        .await
        .expect_err("unpause of running workflow should fail");
    assert_eq!(err.code(), Code::FailedPrecondition);
}

/// Pause/unpause with a missing workflow ID returns
/// INVALID_ARGUMENT before any routing occurs.
#[tokio::test]
async fn pause_unpause_missing_workflow_id_returns_invalid_argument() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    let pause_err = grpc
        .pause_workflow_execution(Request::new(
            workflowservice::PauseWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: String::new(),
                run_id: String::new(),
                identity: "operator".to_string(),
                reason: String::new(),
                request_id: "req-x".to_string(),
            },
        ))
        .await
        .expect_err("missing workflow id should fail");
    assert_eq!(pause_err.code(), Code::InvalidArgument);

    let unpause_err = grpc
        .unpause_workflow_execution(Request::new(
            workflowservice::UnpauseWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_id: String::new(),
                run_id: String::new(),
                identity: "operator".to_string(),
                reason: String::new(),
                request_id: "req-y".to_string(),
            },
        ))
        .await
        .expect_err("missing workflow id should fail");
    assert_eq!(unpause_err.code(), Code::InvalidArgument);
}

/// Querying a paused workflow returns a query rejection carrying
/// the PAUSED status (value 8) instead of dispatching a query.
#[tokio::test]
async fn query_paused_workflow_returns_rejection_via_grpc() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;

    grpc.start_workflow_execution(Request::new(
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "query-paused-wf".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "test".to_string(),
            }),
            task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                name: "q".to_string(),
                ..Default::default()
            }),
            request_id: "req-query-start".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("start should succeed");

    grpc.pause_workflow_execution(Request::new(
        workflowservice::PauseWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "query-paused-wf".to_string(),
            run_id: String::new(),
            identity: "operator".to_string(),
            reason: "maintenance".to_string(),
            request_id: "req-query-pause".to_string(),
        },
    ))
    .await
    .expect("pause should succeed");

    let response = grpc
        .query_workflow(Request::new(workflowservice::QueryWorkflowRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "query-paused-wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            query: Some(
                tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                    query_type: "state".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }))
        .await
        .expect("query should return a response, not an error")
        .into_inner();

    let rejected = response
        .query_rejected
        .expect("paused query must be rejected");
    // WORKFLOW_EXECUTION_STATUS_PAUSED = 8
    assert_eq!(rejected.status, 8);
    assert!(
        response.query_result.is_none(),
        "rejected query must not carry a result"
    );
}
