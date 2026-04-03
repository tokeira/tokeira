use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::{Request, transport::Server};
use tonic_reflection::pb::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
    server_reflection_response::MessageResponse,
};

use tokeira_edge::{
    EdgeInterceptors, EmptyVisibilityApi, InMemoryNamespaceCache, InMemoryOperatorApi,
    LocalOnlyRouter, LongPollConfig, LongPollGate, OperatorService, ResolvedNamespace,
    WorkflowExecutionDescription, WorkflowService,
    grpc::{
        operator_service::OperatorServiceGrpc, runtime_adapter::RuntimeAdapter,
        workflow_service::WorkflowServiceGrpc,
    },
    translate::to_internal::namespace_id_for,
    workflow_service::ExecutionResolver,
};
use tokeira_kernel::LoadedRun;
use tokeira_proto::{
    common::{Memo, Payloads, SearchAttributes, TaskQueue},
    operatorservice::GetClusterInfoRequest,
    workflowservice::{
        DescribeWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_runtime::{
    LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{InMemoryStore, RunRepository};
use tokeira_types::{ExecutionRef, WorkflowId};

#[tokio::test]
async fn grpc_roundtrip_start_describe_and_reflection() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint.clone()).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-1".to_string(),
            workflow_type: "example".to_string(),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
            }),
            input: Some(Payloads::default()),
            request_id: "req-1".to_string(),
            memo: Some(Memo::default()),
            search_attributes: Some(SearchAttributes::default()),
        })
        .await?
        .into_inner();

    assert!(!start.run_id.is_empty());

    let describe = workflow
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-1".to_string(),
            run_id: start.run_id.clone(),
        })
        .await?
        .into_inner();

    let execution = describe.execution.expect("execution should exist");
    assert_eq!(execution.workflow_id, "workflow-1");
    assert_eq!(execution.run_id, start.run_id);

    let mut operator =
        tokeira_proto::operatorservice::operator_service_client::OperatorServiceClient::connect(
            endpoint.clone(),
        )
        .await?;
    let cluster = operator
        .get_cluster_info(GetClusterInfoRequest {})
        .await?
        .into_inner();
    assert_eq!(cluster.cluster_name, "tokeira-local");

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

async fn spawn_test_server() -> Result<(
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(TokeiraRuntime::new(
        store.clone(),
        4,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    ));

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await;

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces));
    let workflow_service = WorkflowService::new(
        Arc::new(RuntimeAdapter::new(runtime)),
        Arc::new(StoreExecutionResolver::new(store.clone())),
        Arc::new(EmptyVisibilityApi),
        interceptors.clone(),
        LongPollGate::new(LongPollConfig::default()),
        Arc::new(LocalOnlyRouter),
    );
    let operator_service = OperatorService::new(
        Arc::new(InMemoryOperatorApi::new("tokeira-local")),
        interceptors,
    );

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
