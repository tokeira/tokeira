// Roundtrip tests construct prost messages with `..Default::default()` for
// forward-compat against upstream proto field additions.
#![allow(clippy::needless_update)]

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, codec::CompressionEncoding, transport::Server};
use tonic_reflection::pb::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
};

use tokeira_edge::{
    EdgeInterceptors, InMemoryNamespaceCache, InMemoryOperatorApi, LocalOnlyRouter, LongPollConfig,
    LongPollGate, NamespaceCache, OperatorService, PendingQueryStore, PollerRegistry,
    ResolvedNamespace, WorkflowExecutionDescription, WorkflowService,
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
        PollWorkflowTaskQueueRequest, QueryWorkflowRequest, ResetWorkflowExecutionRequest,
        RespondWorkflowTaskCompletedRequest, SignalWithStartWorkflowExecutionRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
        TerminateWorkflowExecutionRequest, UpdateWorkflowExecutionRequest,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
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

    let info = describe
        .workflow_execution_info
        .expect("execution info should exist");
    let execution = info.execution.expect("execution should exist");
    assert_eq!(execution.workflow_id, "workflow-1");
    assert_eq!(execution.run_id, start.run_id);
    assert_eq!(info.r#type.expect("workflow type").name, "example");
    assert_eq!(info.task_queue, "queue-a");
    assert!(info.start_time.is_some());
    assert_eq!(
        info.status,
        tokeira_proto::enums::WorkflowExecutionStatus::Running as i32
    );
    assert!(info.history_length > 0);
    assert!(info.history_size_bytes > 0);
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
        .server_reflection_info(Request::new(tokio_stream::once(ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        })))
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
async fn grpc_roundtrip_gzip_compressed_request_is_accepted() -> Result<()> {
    // The Temporal SDKs compress requests by default (the Python SDK defaults to
    // GrpcCompression.GZIP). A server that does not negotiate gzip rejects unmodified
    // SDK traffic with "Content is compressed with 'gzip' which isn't supported".
    // Enabling gzip on the client here reproduces that SDK behaviour and proves the
    // server accepts it (and may respond compressed).
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint)
        .await?
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "gzip-workflow".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            input: Some(Payloads::default()),
            request_id: "gzip-req-1".to_string(),
            ..Default::default()
        })
        .await?
        .into_inner();

    assert!(
        !start.run_id.is_empty(),
        "a gzip-compressed StartWorkflowExecution must be accepted"
    );

    shutdown_tx.send(()).ok();
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

    let info = describe
        .workflow_execution_info
        .expect("execution info should exist");
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
    assert_eq!(info.execution.expect("execution").run_id, response.run_id);

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
async fn grpc_roundtrip_signal_then_query_returns_latest_buffered_result() -> Result<()> {
    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint.clone()).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-query-order".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            input: Some(Payloads::default()),
            request_id: "query-order-start".to_string(),
            memo: Some(Memo::default()),
            search_attributes: Some(SearchAttributes::default()),
            ..Default::default()
        })
        .await?
        .into_inner();

    let initial = workflow
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
    workflow
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            task_token: initial.task_token,
            identity: "worker-1".to_string(),
            commands: Vec::new(),
            ..Default::default()
        })
        .await?;

    workflow
        .signal_workflow_execution(SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow-query-order".to_string(),
                run_id: start.run_id.clone(),
            }),
            signal_name: "set_counter".to_string(),
            input: Some(payloads("5")),
            request_id: "query-order-signal".to_string(),
            ..Default::default()
        })
        .await?;

    let endpoint_for_query = endpoint.clone();
    let run_id_for_query = start.run_id.clone();
    let query_handle = tokio::spawn(async move {
        let mut client = WorkflowServiceClient::connect(endpoint_for_query).await?;
        client
            .query_workflow(QueryWorkflowRequest {
                namespace: "default".to_string(),
                execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "workflow-query-order".to_string(),
                    run_id: run_id_for_query,
                }),
                query: Some(
                    tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                        query_type: "get_counter".to_string(),
                        query_args: Some(Payloads::default()),
                        header: None,
                    },
                ),
                ..Default::default()
            })
            .await
            .map(|resp| resp.into_inner())
            .map_err(anyhow::Error::from)
    });

    tokio::time::sleep(std::time::Duration::from_millis(25)).await;

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
    assert_eq!(poll.queries.len(), 1);
    let query_id = poll
        .queries
        .keys()
        .next()
        .cloned()
        .expect("buffered query should be attached");

    let mut query_results = std::collections::BTreeMap::new();
    query_results.insert(
        query_id,
        tokeira_proto::public::temporal::api::query::v1::WorkflowQueryResult {
            result_type: tokeira_proto::enums::QueryResultType::Answered as i32,
            answer: Some(payloads("5")),
            error_message: String::new(),
            failure: None,
        },
    );

    workflow
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            task_token: poll.task_token,
            identity: "worker-1".to_string(),
            commands: Vec::new(),
            query_results,
            ..Default::default()
        })
        .await?;

    let query = query_handle.await??;
    assert_eq!(
        query.query_result.expect("query result should be present"),
        payloads("5")
    );

    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn grpc_roundtrip_update_completed_through_protocol_messages() -> Result<()> {
    use prost::Message as _;
    use tokeira_proto::public::temporal::api::{
        command::v1::{
            Command, ProtocolMessageCommandAttributes, command::Attributes as CommandAttributes,
        },
        protocol::v1::Message as ProtocolMessage,
        update::v1::{
            Acceptance as UpdateAcceptance, Outcome as UpdateOutcome, Response as UpdateResponse,
            outcome,
        },
    };

    let (addr, shutdown_tx, server) = spawn_test_server().await?;

    let endpoint = format!("http://{addr}");
    let mut workflow = WorkflowServiceClient::connect(endpoint.clone()).await?;

    let start = workflow
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-update-transport".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "example".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: "queue-a".to_string(),
                ..Default::default()
            }),
            input: Some(Payloads::default()),
            request_id: "update-start".to_string(),
            memo: Some(Memo::default()),
            search_attributes: Some(SearchAttributes::default()),
            ..Default::default()
        })
        .await?
        .into_inner();

    let initial = workflow
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
    workflow
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            task_token: initial.task_token,
            identity: "worker-1".to_string(),
            commands: Vec::new(),
            ..Default::default()
        })
        .await?;

    let endpoint_for_update = endpoint.clone();
    let run_id_for_update = start.run_id.clone();
    let update_handle = tokio::spawn(async move {
        let mut client = WorkflowServiceClient::connect(endpoint_for_update).await?;
        client
            .update_workflow_execution(UpdateWorkflowExecutionRequest {
                namespace: "default".to_string(),
                workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: "workflow-update-transport".to_string(),
                    run_id: run_id_for_update,
                }),
                request: Some(tokeira_proto::public::temporal::api::update::v1::Request {
                    meta: Some(tokeira_proto::public::temporal::api::update::v1::Meta {
                        update_id: "update-1".to_string(),
                        identity: "starter".to_string(),
                    }),
                    input: Some(tokeira_proto::public::temporal::api::update::v1::Input {
                        header: None,
                        name: "set_counter".to_string(),
                        args: Some(payloads("10")),
                    }),
                }),
                wait_policy: Some(
                    tokeira_proto::public::temporal::api::update::v1::WaitPolicy {
                        lifecycle_stage: 3,
                    },
                ),
                ..Default::default()
            })
            .await
            .map(|resp| resp.into_inner())
            .map_err(anyhow::Error::from)
    });

    let poll = loop {
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

        if poll.task_token.is_empty() {
            continue;
        }
        if !poll.messages.is_empty() {
            break poll;
        }

        workflow
            .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
                task_token: poll.task_token,
                identity: "worker-1".to_string(),
                commands: Vec::new(),
                ..Default::default()
            })
            .await?;

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    assert_eq!(poll.messages.len(), 1);
    let update_id = poll.messages[0].protocol_instance_id.clone();
    assert_eq!(update_id, "update-1");

    let acceptance = UpdateAcceptance {
        accepted_request_message_id: poll.messages[0].id.clone(),
        accepted_request_sequencing_event_id: poll.messages[0]
            .sequencing_id
            .as_ref()
            .and_then(|id| match id {
                tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::EventId(event_id) => {
                    Some(*event_id)
                }
                tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::CommandIndex(_) => None,
            })
            .unwrap_or_default(),
        accepted_request: None,
    };
    let acceptance_body = prost_types::Any {
        type_url: "type.googleapis.com/temporal.api.update.v1.Acceptance".to_string(),
        value: acceptance.encode_to_vec(),
    };
    let response = UpdateResponse {
        meta: None,
        outcome: Some(UpdateOutcome {
            value: Some(outcome::Value::Success(payloads("5"))),
        }),
    };
    let body = prost_types::Any {
        type_url: "type.googleapis.com/temporal.api.update.v1.Response".to_string(),
        value: response.encode_to_vec(),
    };

    workflow
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            task_token: poll.task_token,
            identity: "worker-1".to_string(),
            commands: vec![
                Command {
                    command_type: tokeira_proto::enums::CommandType::ProtocolMessage as i32,
                    user_metadata: None,
                    attributes: Some(CommandAttributes::ProtocolMessageCommandAttributes(
                        ProtocolMessageCommandAttributes {
                            message_id: format!("{update_id}/acceptance"),
                        },
                    )),
                },
                Command {
                    command_type: tokeira_proto::enums::CommandType::ProtocolMessage as i32,
                    user_metadata: None,
                    attributes: Some(CommandAttributes::ProtocolMessageCommandAttributes(
                        ProtocolMessageCommandAttributes {
                            message_id: format!("{update_id}/response"),
                        },
                    )),
                },
            ],
            messages: vec![
                ProtocolMessage {
                    id: format!("{update_id}/acceptance"),
                    protocol_instance_id: update_id.clone(),
                    body: Some(acceptance_body),
                    sequencing_id: None,
                },
                ProtocolMessage {
                    id: format!("{update_id}/response"),
                    protocol_instance_id: update_id,
                    body: Some(body),
                    sequencing_id: None,
                },
            ],
            ..Default::default()
        })
        .await?;

    let update = update_handle.await??;
    let outcome = update.outcome.expect("update outcome should be present");
    match outcome.value {
        Some(tokeira_proto::public::temporal::api::update::v1::outcome::Value::Success(result)) => {
            assert_eq!(result, payloads("5"))
        }
        other => panic!("unexpected update outcome: {other:?}"),
    }

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
    let buffered_queries = runtime.buffered_queries();

    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await?;

    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local", "0.1.0+test0001"));
    let visibility_store = InMemoryVisibilityStore::default();
    for partition_id in 0..16 {
        let projection_worker = ProjectionWorker {
            log: store.clone(),
            sink: VisibilitySink::new(visibility_store.clone()),
            batch_size: 256,
        };
        tokio::spawn(async move {
            let cancel = CancellationToken::new();
            let _ = projection_worker
                .run_from_cursor(cancel, ProjectionCursor::beginning(partition_id, 1))
                .await;
        });
    }
    let workflow_broker = runtime.broker();
    let workflow_service = WorkflowService::new_with_buffered_queries(
        Arc::new(RuntimeAdapter::new(runtime)),
        Arc::new(StoreExecutionResolver::new(Arc::new(store.clone()))),
        Arc::new(VisibilityQueryService::new(visibility_store)),
        Arc::new(store.clone()),
        operator_api.clone(),
        namespaces,
        interceptors.clone(),
        PollerRegistry::default(),
        PendingQueryStore::default(),
        buffered_queries,
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
        run_id: Option<tokeira_types::RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        let Some(run_key) = self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id: namespace_id_for(namespace),
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id,
            })
            .await?
        else {
            return Ok(None);
        };

        let history = self.repo.read_history(run_key, 0, usize::MAX).await?;
        let history_size_bytes =
            tokeira_edge::translate::history_serializer::serialized_history_size_bytes(&history);
        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => {
                let pending_nexus_operations =
                    tokeira_edge::translate::describe_pending_nexus_operations(&state);
                Ok(Some(WorkflowExecutionDescription {
                    namespace: namespace.to_string(),
                    workflow_id: state.workflow_id.0.clone(),
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
                    history_size_bytes,
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
                                    priority: activity.priority.as_ref().map(|priority| {
                                        tokeira_edge::translate::Priority {
                                            priority_key: priority.priority_key,
                                            fairness_key: priority.fairness_key.clone(),
                                            fairness_weight: priority.fairness_weight,
                                        }
                                    }),
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
                    pending_nexus_operations,
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
                    priority: state.priority.as_ref().map(|priority| {
                        tokeira_edge::translate::Priority {
                            priority_key: priority.priority_key,
                            fairness_key: priority.fairness_key.clone(),
                            fairness_weight: priority.fairness_weight,
                        }
                    }),
                    auto_reset_points: state.auto_reset_points.clone(),
                    most_recent_worker_version_stamp: state
                        .versioning_info
                        .as_ref()
                        .and_then(|info| info.most_recent_worker_version_stamp.clone()),
                    request_id_infos: state.request_id_infos.clone(),
                    external_payload_count: 0,
                    external_payload_size_bytes: 0,
                }))
            }
            LoadedRun::Absent => Err(anyhow!("resolved run missing from storage: {:?}", run_key)),
        }
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads {
        payloads: vec![tokeira_proto::common::Payload {
            data: value.as_bytes().to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        }],
    }
}
