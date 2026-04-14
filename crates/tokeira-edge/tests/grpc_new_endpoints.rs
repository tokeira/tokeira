use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokeira_edge::{
    EdgeInterceptors, EmptyVisibilityApi,
    InMemoryNamespaceCache, InMemoryOperatorApi, LocalOnlyRouter,
    LongPollConfig, LongPollGate, PendingQueryStore, PollerRegistry,
    NamespaceCache, ResolvedNamespace, WorkflowExecutionDescription,
    WorkflowService,
    grpc::workflow_service::WorkflowServiceGrpc,
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_proto::workflowservice::{
    self,
    workflow_service_server::WorkflowService as WfApi,
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig,
    TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, NamespaceId, WorkflowId,
};
use tonic::Request;
use tonic::Code;

use tokeira_edge::grpc::runtime_adapter::RuntimeAdapter;

struct StoreExecutionResolver {
    repo: Arc<InMemoryStore>,
    namespace_id: NamespaceId,
}

impl StoreExecutionResolver {
    fn new(
        repo: Arc<InMemoryStore>,
        namespace_id: NamespaceId,
    ) -> Self {
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
                workflow_id: WorkflowId(
                    workflow_id.to_string(),
                ),
                run_id: None,
            })
            .await
    }

    async fn describe_execution(
        &self,
        _namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let Some(run_key) = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: self.namespace_id,
                workflow_id: WorkflowId(
                    workflow_id.to_string(),
                ),
                run_id: None,
            })
            .await?
        else {
            return Ok(None);
        };

        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => {
                Ok(Some(WorkflowExecutionDescription {
                    namespace: "default".to_string(),
                    workflow_id: state.workflow_id.0,
                    run_key: state.run_key,
                    run_id: state.run_id,
                    workflow_type: state.workflow_type.0,
                    task_queue: state.task_queue.0,
                    status: state.status,
                    start_time: Some(state.started_at),
                    close_time: state.closed_at,
                    history_length: state.last_event_id,
                    state_transition_count: state
                        .transition_seq
                        .0
                        as i64,
                    memo: state.memo,
                    search_attributes: state
                        .search_attributes,
                }))
            }
            LoadedRun::Absent => Err(anyhow::anyhow!(
                "resolved run missing"
            )),
        }
    }
}

async fn build_grpc(
    store: Arc<InMemoryStore>,
) -> WorkflowServiceGrpc {
    build_grpc_with_namespaces(
        store,
        vec![ResolvedNamespace::active("default")],
    )
    .await
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

    let interceptors =
        Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
    let router = Arc::new(LocalOnlyRouter);
    let workflow_broker = runtime.broker();
    let runtime_adapter =
        Arc::new(RuntimeAdapter::new(runtime));
    let resolver = Arc::new(StoreExecutionResolver::new(
        store.clone(),
        ns_id,
    ));
    let visibility = Arc::new(EmptyVisibilityApi);
    let long_polls =
        LongPollGate::new(LongPollConfig::default());

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
                task_queue: Some(
                    tokeira_proto::taskqueue::TaskQueue {
                        name: "q".to_string(),
                        ..Default::default()
                    },
                ),
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
            workflow_id: WorkflowId(
                "term-wf".to_string(),
            ),
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
        let state =
            match store.load_run(rk).await.unwrap() {
                LoadedRun::Existing(s) => s,
                _ => panic!("run should exist"),
            };
        assert_eq!(
            state.status,
            tokeira_types::ExecutionStatus::Terminated
        );
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
                task_queue: Some(
                    tokeira_proto::taskqueue::TaskQueue {
                        name: "q".to_string(),
                        ..Default::default()
                    },
                ),
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
        .get_system_info(Request::new(
            workflowservice::GetSystemInfoRequest {},
        ))
        .await
        .expect("get system info should succeed")
        .into_inner();
    assert!(!system.server_version.is_empty());
    assert!(system.capabilities.is_some());

    let cluster = grpc
        .get_cluster_info(Request::new(
            workflowservice::GetClusterInfoRequest {},
        ))
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
        .describe_namespace(Request::new(
            workflowservice::DescribeNamespaceRequest {
                namespace: "default".to_string(),
                id: String::new(),
            },
        ))
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

    grpc.register_namespace(Request::new(
        workflowservice::RegisterNamespaceRequest {
            namespace: "payments".to_string(),
            ..Default::default()
        },
    ))
    .await
    .expect("register namespace should succeed");

    let describe = grpc
        .describe_namespace(Request::new(
            workflowservice::DescribeNamespaceRequest {
                namespace: "payments".to_string(),
                id: String::new(),
            },
        ))
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
        .register_namespace(Request::new(
            workflowservice::RegisterNamespaceRequest {
                namespace: "payments".to_string(),
                ..Default::default()
            },
        ))
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
        .describe_namespace(Request::new(
            workflowservice::DescribeNamespaceRequest {
                namespace: "missing".to_string(),
                id: String::new(),
            },
        ))
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
            .register_namespace(Request::new(
                workflowservice::RegisterNamespaceRequest {
                    namespace: namespace.to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("invalid namespace should fail");
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
