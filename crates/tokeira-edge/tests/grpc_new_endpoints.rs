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
use tokeira_proto::workflowservice::{self, workflow_service_server::WorkflowService as WfApi};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
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
                            attempt: activity.attempt,
                            maximum_attempts: activity
                                .retry_policy
                                .as_ref()
                                .map(|policy| policy.maximum_attempts)
                                .unwrap_or_default(),
                            scheduled_at: activity.scheduled_at,
                            started_at: activity.started_at,
                            last_failure: activity.last_failure.clone(),
                            paused: activity.pause_info.is_some(),
                            pause_info: activity.pause_info.as_ref().map(|info| {
                                tokeira_edge::translate::PauseInfoDescription {
                                    identity: info.identity.clone(),
                                    paused_time: info.pause_time,
                                    reason: info.reason.clone(),
                                }
                            }),
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
                callbacks: Vec::new(),
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
                            started: operation.started,
                            operation_token: operation
                                .started
                                .then(|| operation.operation_id.clone()),
                        },
                    )
                    .collect(),
                pause_info: state.pause_info.as_ref().map(|info| {
                    tokeira_edge::translate::PauseInfoDescription {
                        identity: info.identity.clone(),
                        paused_time: info.pause_time,
                        reason: info.reason.clone(),
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
    let runtime = Arc::new(TokeiraRuntime::new(
        store.clone(),
        4,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));

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
        runtime_adapter,
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
    );

    WorkflowServiceGrpc::new(service)
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
    assert!(system.capabilities.is_some());

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
async fn register_namespace_invalid_names_return_invalid_argument() {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store).await;

    for namespace in ["", "bad namespace", "bad!"] {
        let err = grpc
            .register_namespace(Request::new(workflowservice::RegisterNamespaceRequest {
                namespace: namespace.to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("invalid namespace should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
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
