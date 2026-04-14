use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, transport::Server};
use tonic::Code;
use tonic_reflection::pb::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
    server_reflection_response::MessageResponse,
};

use tokeira_edge::{
    EdgeInterceptors, InMemoryNamespaceCache, InMemoryOperatorApi,
    LocalOnlyRouter, LongPollConfig, LongPollGate, NamespaceCache, OperatorService,
    PendingQueryStore, PollerRegistry, ResolvedNamespace, WorkflowExecutionDescription,
    WorkflowService,
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_projection::{
    InMemoryVisibilityStore, ProjectionWorker, VisibilityQueryService, VisibilitySink,
};
use tokeira_proto::{
    common::{Memo, Payloads, SearchAttributes},
    taskqueue::TaskQueue,
    workflowservice::{
        DescribeWorkflowExecutionRequest, ListWorkflowExecutionsRequest,
        PollWorkflowTaskQueueRequest, ResetWorkflowExecutionRequest,
        RespondWorkflowTaskCompletedRequest, StartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionRequest,
        TerminateWorkflowExecutionRequest,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{ExecutionRef, ProjectionCursor, WorkflowId};

#[tokio::test]
async fn grpc_roundtrip_start_describe_and_reflection() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint.clone()).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-1".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            input: Some(Payloads::default()),
            request_id: "req-1".to_string(),
            memo: Some(Memo::default()),
            search_attributes: Some(SearchAttributes::default()),
            ..Default::default()
        })
        .await?
        .into_inner();

    assert!(!start.run_id.is_empty());

    let describe = workflow
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-1".to_string(),
                run_id: start.run_id.clone(),
                ..Default::default()
            }),
        })
        .await?
        .into_inner();

    let info = describe.workflow_execution_info.expect("execution info should exist");
    let execution = info.execution.expect("execution should exist");
    assert_eq!(execution.workflow_id, "workflow-1");
    assert_eq!(execution.run_id, start.run_id);
    assert_eq!(
        info.r#type.expect("workflow type").name,
        "example"
    );
    assert_eq!(info.task_queue, "queue-a");
    assert!(info.start_time.is_some());
    assert_eq!(
        info.status,
        tokeira_proto::enums::WorkflowExecutionStatus::Running as i32
    );
    assert!(info.history_length > 0);
    assert!(info.state_transition_count > 0);
    assert!(info.memo.is_some());
    assert!(info.search_attributes.is_some());

    let list = loop {
        let response = workflow
            .list_workflow_executions(ListWorkflowExecutionsRequest {
                namespace: "default".to_string(),
                page_size: 10,
                ..Default::default()
            })
            .await?
            .into_inner();
        if !response.executions.is_empty() {
            break response;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert_eq!(list.executions.len(), 1);
    assert_eq!(
        list.executions[0]
            .execution
            .as_ref()
            .expect("execution")
            .workflow_id,
        "workflow-1"
    );

    let reflection_channel = tonic::transport::Endpoint::new(endpoint)?.connect().await?;
    let mut reflection = ServerReflectionClient::new(reflection_channel);
    let mut stream = reflection
        .server_reflection_info(Request::new(tokio_stream::once(
            ServerReflectionRequest {
                host: String::new(),
                message_request: Some(MessageRequest::ListServices(String::new())),
            },
        )))
        .await?
        .into_inner();

    let response = stream
        .next()
        .await
        .ok_or_else(|| anyhow!("missing reflection response"))??;
    let services = match response.message_response {
        Some(MessageResponse::ListServicesResponse(services)) => services.service,
        other => return Err(anyhow!("unexpected reflection response: {other:?}")),
    };

    let names: Vec<_> = services.into_iter().map(|svc| svc.name).collect();
    assert!(
        names
            .iter()
            .any(|name| name == "temporal.api.workflowservice.v1.WorkflowService")
    );
    assert!(
        names
            .iter()
            .any(|name| name == "temporal.api.operatorservice.v1.OperatorService")
    );

    let _ = shutdown_tx.send(());
    server.await??;

    Ok(())
}

#[tokio::test]
async fn grpc_roundtrip_reset_returns_successor_and_updates_current_run() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-reset".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            input: Some(Payloads::default()),
            request_id: "reset-start".to_string(),
            memo: Some(Memo::default()),
            search_attributes: Some(SearchAttributes::default()),
            ..Default::default()
        })
        .await?
        .into_inner();

    let poll = workflow
        .poll_workflow_task_queue(PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();
    assert!(!poll.task_token.is_empty());

    workflow
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            task_token: poll.task_token,
            identity: "worker-1".to_string(),
            commands: Vec::new(),
            ..Default::default()
        })
        .await?;

    let reset = workflow
        .reset_workflow_execution(ResetWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-reset".to_string(),
                run_id: start.run_id.clone(),
            }),
            reason: "operator reset".to_string(),
            workflow_task_finish_event_id: 4,
            request_id: "reset-req-1".to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();

    assert!(!reset.run_id.is_empty());
    assert_ne!(reset.run_id, start.run_id);

    let describe = workflow
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-reset".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
        })
        .await?
        .into_inner();

    let info = describe.workflow_execution_info.expect("execution info should exist");
    let execution = info.execution.expect("execution should exist");
    assert_eq!(execution.workflow_id, "workflow-reset");
    assert_eq!(execution.run_id, reset.run_id);
    assert_ne!(execution.run_id, start.run_id);

    let _ = shutdown_tx.send(());
    server.await??;

    Ok(())
}

#[tokio::test]
async fn grpc_roundtrip_signal_with_start_starts_new_run() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint).await?;

    let response = workflow
        .signal_with_start_workflow_execution(SignalWithStartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-sws-new".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            signal_name: "sig".to_string(),
            signal_input: Some(Payloads::default()),
            request_id: "sws-new".to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();

    assert!(response.started);
    assert!(!response.run_id.is_empty());

    let describe = workflow
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-sws-new".to_string(),
                run_id: response.run_id.clone(),
                ..Default::default()
            }),
        })
        .await?
        .into_inner();
    let info = describe.workflow_execution_info.expect("execution info");
    assert_eq!(
        info.execution.expect("execution").run_id,
        response.run_id
    );

    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn grpc_roundtrip_signal_with_start_uses_existing_run() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-sws-existing".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            request_id: "sws-existing-start".to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();

    let response = workflow
        .signal_with_start_workflow_execution(SignalWithStartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-sws-existing".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            signal_name: "sig".to_string(),
            signal_input: Some(Payloads::default()),
            request_id: "sws-existing-signal".to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();

    assert!(!response.started);
    assert_eq!(response.run_id, start.run_id);

    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn grpc_roundtrip_terminate_does_not_create_reset_successor() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-terminate".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            input: Some(Payloads::default()),
            request_id: "terminate-start".to_string(),
            memo: Some(Memo::default()),
            search_attributes: Some(SearchAttributes::default()),
            ..Default::default()
        })
        .await?
        .into_inner();

    workflow
        .terminate_workflow_execution(TerminateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-terminate".to_string(),
                run_id: start.run_id.clone(),
            }),
            reason: "operator terminate".to_string(),
            identity: "tester".to_string(),
            ..Default::default()
        })
        .await?;

    let error = workflow
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-terminate".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
        })
        .await
        .expect_err("terminated workflow should not have a replacement current run");

    assert_eq!(error.code(), Code::NotFound);

    let _ = shutdown_tx.send(());
    server.await??;

    Ok(())
}

async fn spawn_test_server() -> Result<(
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let store = InMemoryStore::default();
    let runtime = Arc::new(TokeiraRuntime::new(
        Arc::new(store.clone()),
        4,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await?;

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
    let visibility_store = InMemoryVisibilityStore::default();
    for partition_id in 0..16 {
        let projection_worker = ProjectionWorker {
            log: store.clone(),
            sink: VisibilitySink::new(
                visibility_store.clone(),
                format!("visibility-{partition_id}"),
            ),
            batch_size: 256,
        };
        tokio::spawn(async move {
            let cancel = CancellationToken::new();
            let _ = projection_worker
                .run_from_cursor(
                    &format!("visibility-{partition_id}"),
                    cancel,
                    ProjectionCursor::beginning(partition_id, 1),
                )
                .await;
        });
    }
    let workflow_broker = runtime.broker();
    let workflow_service = WorkflowService::new(
        Arc::new(RuntimeAdapter::new(runtime)),
        Arc::new(StoreExecutionResolver::new(Arc::new(store.clone()))),
        Arc::new(VisibilityQueryService::new(visibility_store)),
        Arc::new(store.clone()),
        operator_api.clone(),
        namespaces,
        interceptors.clone(),
        PollerRegistry::default(),
        PendingQueryStore::default(),
        workflow_broker,
        LongPollGate::new(LongPollConfig::default()),
        Arc::new(LocalOnlyRouter),
    );
    let operator_service = OperatorService::new(operator_api, interceptors);

    let workflow_grpc = WorkflowServiceGrpc::new(workflow_service);
    let operator_grpc = OperatorServiceGrpc::new(operator_service);
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .build()?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(workflow_grpc.into_service())
            .add_service(operator_grpc.into_service())
            .add_service(reflection)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await?;
        Ok(())
    });

    Ok((addr, shutdown_tx, server))
}

struct StoreExecutionResolver<R> {
    repo: Arc<R>,
}

impl<R> StoreExecutionResolver<R> {
    fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }
}

#[async_trait]
impl<R> ExecutionResolver for StoreExecutionResolver<R>
where
    R: RunRepository + 'static,
{
    async fn current_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<tokeira_types::RunKey>> {
        self.repo
            .resolve_execution(&ExecutionRef {
                namespace_id: namespace_id_for(namespace),
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await
    }

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let Some(run_key) = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: namespace_id_for(namespace),
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: None,
            })
            .await?
        else {
            return Ok(None);
        };

        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(Some(WorkflowExecutionDescription {
                namespace: namespace.to_string(),
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                workflow_type: state.workflow_type.0,
                task_queue: state.task_queue.0,
                status: state.status,
                start_time: Some(state.started_at),
                close_time: state.closed_at,
                history_length: state.last_event_id,
                state_transition_count: state.transition_seq.0 as i64,
                memo: state.memo,
                search_attributes: state.search_attributes,
            })),
            LoadedRun::Absent => {
                Err(anyhow!("resolved run missing from storage: {:?}", run_key))
            }
        }
    }
}
