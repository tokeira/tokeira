//! Host-attached listener coverage: one engine behind two transports.
//!
//! Every scenario drives the engine through a transport-neutral [`Transport`]
//! so the same RPC sequence runs over the in-process endpoint and over a bound
//! listener, and the two are compared. The worker side is a raw-proto loop
//! (poll, respond), the same shape the edge tests use, so the engine's
//! dev-dependencies stay free of the SDK worker.

mod listener_support;

use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener as StdTcpListener},
    time::Duration,
};

use anyhow::{Context as _, Result, bail, ensure};
use listener_support::{
    STEP, Transport, WORKFLOW_SERVICE, execution, payloads, runtime, seconds,
    start_engine_with_listener, task_queue, wait_until_refused,
};
use proptest::prelude::*;
use prost::Message;
use tokeira_engine::{Engine, EngineListenError, EngineListener};
use tokeira_proto::{
    common::{ActivityType, WorkflowType},
    enums::{
        CommandType, EventType, QueryResultType, WorkflowExecutionStatus, WorkflowIdConflictPolicy,
        WorkflowIdReusePolicy,
    },
    public::temporal::api::{
        command::v1::{
            Command, CompleteWorkflowExecutionCommandAttributes, ProtocolMessageCommandAttributes,
            RequestCancelActivityTaskCommandAttributes, ScheduleActivityTaskCommandAttributes,
            command::Attributes as CommandAttributes,
        },
        history::v1::history_event::Attributes as EventAttributes,
        protocol::v1::{Message as ProtocolMessage, message::SequencingId},
        query::v1::{WorkflowQuery, WorkflowQueryResult},
        update::v1::{
            Acceptance as UpdateAcceptance, Input as UpdateInput, Meta as UpdateMeta,
            Outcome as UpdateOutcome, Request as UpdateRequest, Response as UpdateResponse,
            WaitPolicy, outcome,
        },
    },
    workflowservice::{
        DescribeTaskQueueRequest, DescribeTaskQueueResponse, DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse, ExecuteMultiOperationRequest,
        ExecuteMultiOperationResponse, GetWorkflowExecutionHistoryRequest,
        GetWorkflowExecutionHistoryResponse, PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        QueryWorkflowRequest, QueryWorkflowResponse, RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse, RegisterNamespaceRequest, RegisterNamespaceResponse,
        RespondActivityTaskCanceledRequest, RespondActivityTaskCanceledResponse,
        RespondActivityTaskCompletedRequest, RespondActivityTaskCompletedResponse,
        RespondQueryTaskCompletedRequest, RespondQueryTaskCompletedResponse,
        RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
        SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
        UpdateWorkflowExecutionRequest, UpdateWorkflowExecutionResponse,
        execute_multi_operation_request, execute_multi_operation_response,
    },
};
use tonic::{Code, Status};

// ---------------------------------------------------------------------------
// Worker loop over a transport-neutral surface
// ---------------------------------------------------------------------------

async fn poll_wft(
    worker: &Transport,
    namespace: &str,
    queue: &str,
    identity: &str,
) -> Result<PollWorkflowTaskQueueResponse> {
    tokio::time::timeout(
        STEP,
        worker.unary(
            "PollWorkflowTaskQueue",
            PollWorkflowTaskQueueRequest {
                namespace: namespace.to_owned(),
                task_queue: Some(task_queue(queue)),
                identity: identity.to_owned(),
                ..Default::default()
            },
            &[],
        ),
    )
    .await
    .context("workflow task poll did not return")?
    .context("workflow task poll failed")
}

async fn respond_wft(
    worker: &Transport,
    namespace: &str,
    task_token: Vec<u8>,
    identity: &str,
    commands: Vec<Command>,
    messages: Vec<ProtocolMessage>,
    query_results: BTreeMap<String, WorkflowQueryResult>,
) -> Result<RespondWorkflowTaskCompletedResponse> {
    worker
        .unary(
            "RespondWorkflowTaskCompleted",
            RespondWorkflowTaskCompletedRequest {
                task_token,
                identity: identity.to_owned(),
                namespace: namespace.to_owned(),
                commands,
                messages,
                query_results,
                ..Default::default()
            },
            &[],
        )
        .await
        .context("workflow task completion failed")
}

async fn poll_activity(
    worker: &Transport,
    namespace: &str,
    queue: &str,
    identity: &str,
) -> Result<PollActivityTaskQueueResponse> {
    tokio::time::timeout(
        STEP,
        worker.unary(
            "PollActivityTaskQueue",
            PollActivityTaskQueueRequest {
                namespace: namespace.to_owned(),
                task_queue: Some(task_queue(queue)),
                identity: identity.to_owned(),
                ..Default::default()
            },
            &[],
        ),
    )
    .await
    .context("activity task poll did not return")?
    .context("activity task poll failed")
}

fn schedule_activity(activity_id: &str, queue: &str) -> Command {
    Command {
        command_type: CommandType::ScheduleActivityTask as i32,
        user_metadata: None,
        attributes: Some(CommandAttributes::ScheduleActivityTaskCommandAttributes(
            ScheduleActivityTaskCommandAttributes {
                activity_id: activity_id.to_owned(),
                activity_type: Some(ActivityType {
                    name: "echo".to_owned(),
                }),
                task_queue: Some(task_queue(queue)),
                input: Some(payloads(activity_id)),
                schedule_to_close_timeout: Some(seconds(120)),
                start_to_close_timeout: Some(seconds(60)),
                heartbeat_timeout: Some(seconds(30)),
                ..Default::default()
            },
        )),
    }
}

fn complete_workflow(result: &str) -> Command {
    Command {
        command_type: CommandType::CompleteWorkflowExecution as i32,
        user_metadata: None,
        attributes: Some(
            CommandAttributes::CompleteWorkflowExecutionCommandAttributes(
                CompleteWorkflowExecutionCommandAttributes {
                    result: Some(payloads(result)),
                },
            ),
        ),
    }
}

fn history_has(task: &PollWorkflowTaskQueueResponse, event_type: EventType) -> bool {
    task.history.as_ref().is_some_and(|history| {
        history
            .events
            .iter()
            .any(|event| event.event_type == event_type as i32)
    })
}

fn scheduled_event_id(task: &PollWorkflowTaskQueueResponse, activity_id: &str) -> Option<i64> {
    task.history
        .as_ref()?
        .events
        .iter()
        .find_map(|event| match &event.attributes {
            Some(EventAttributes::ActivityTaskScheduledEventAttributes(attributes))
                if attributes.activity_id == activity_id =>
            {
                Some(event.event_id)
            }
            _ => None,
        })
}

/// Accept and complete the single update carried by `task`, optionally closing
/// the workflow in the same completion.
async fn respond_update(
    worker: &Transport,
    namespace: &str,
    identity: &str,
    task: PollWorkflowTaskQueueResponse,
    result: &str,
    also_complete: bool,
) -> Result<()> {
    let message = task
        .messages
        .first()
        .context("update task carries no protocol message")?;
    let update_id = message.protocol_instance_id.clone();
    // v1.31.0 rejects an acceptance whose sequencing event id is unset, so
    // echo the id the server attached to the request message.
    let accepted_request_sequencing_event_id = match message.sequencing_id.as_ref() {
        Some(SequencingId::EventId(event_id)) => *event_id,
        Some(SequencingId::CommandIndex(_)) | None => 0,
    };
    let acceptance = UpdateAcceptance {
        accepted_request_message_id: message.id.clone(),
        accepted_request_sequencing_event_id,
        accepted_request: None,
    };
    let response = UpdateResponse {
        meta: None,
        outcome: Some(UpdateOutcome {
            value: Some(outcome::Value::Success(payloads(result))),
        }),
    };
    let mut commands = vec![
        Command {
            command_type: CommandType::ProtocolMessage as i32,
            user_metadata: None,
            attributes: Some(CommandAttributes::ProtocolMessageCommandAttributes(
                ProtocolMessageCommandAttributes {
                    message_id: format!("{update_id}/acceptance"),
                },
            )),
        },
        Command {
            command_type: CommandType::ProtocolMessage as i32,
            user_metadata: None,
            attributes: Some(CommandAttributes::ProtocolMessageCommandAttributes(
                ProtocolMessageCommandAttributes {
                    message_id: format!("{update_id}/response"),
                },
            )),
        },
    ];
    if also_complete {
        commands.push(complete_workflow("closed-by-update"));
    }
    let messages = vec![
        ProtocolMessage {
            id: format!("{update_id}/acceptance"),
            protocol_instance_id: update_id.clone(),
            body: Some(prost_types::Any {
                type_url: "type.googleapis.com/temporal.api.update.v1.Acceptance".to_owned(),
                value: acceptance.encode_to_vec(),
            }),
            sequencing_id: None,
        },
        ProtocolMessage {
            id: format!("{update_id}/response"),
            protocol_instance_id: update_id,
            body: Some(prost_types::Any {
                type_url: "type.googleapis.com/temporal.api.update.v1.Response".to_owned(),
                value: response.encode_to_vec(),
            }),
            sequencing_id: None,
        },
    ];
    respond_wft(
        worker,
        namespace,
        task.task_token,
        identity,
        commands,
        messages,
        BTreeMap::new(),
    )
    .await?;
    Ok(())
}

/// Poll until a task for `workflow_id` carrying protocol messages arrives,
/// completing any unrelated task on the way.
async fn poll_update_task(
    worker: &Transport,
    namespace: &str,
    queue: &str,
    identity: &str,
    workflow_id: &str,
) -> Result<PollWorkflowTaskQueueResponse> {
    tokio::time::timeout(STEP, async {
        loop {
            let task = poll_wft(worker, namespace, queue, identity).await?;
            if task.task_token.is_empty() {
                continue;
            }
            let matches = task
                .workflow_execution
                .as_ref()
                .is_some_and(|execution| execution.workflow_id == workflow_id);
            if matches && !task.messages.is_empty() {
                return Ok::<_, anyhow::Error>(task);
            }
            respond_wft(
                worker,
                namespace,
                task.task_token,
                identity,
                Vec::new(),
                Vec::new(),
                BTreeMap::new(),
            )
            .await?;
        }
    })
    .await
    .context("update task did not arrive")?
}

// ---------------------------------------------------------------------------
// The shared scenario: start, activity with heartbeat, signal, query, update,
// update-with-start, activity cancellation, completion, then both transports
// read the result.
// ---------------------------------------------------------------------------

async fn run_scenario(
    client: &Transport,
    worker: &Transport,
    namespace: &str,
    label: &str,
) -> Result<()> {
    let queue = format!("queue-{label}");
    let workflow_id = format!("wf-{label}");
    let identity = format!("worker-{label}");
    let client_id = "scenario-client";

    if namespace != "default" {
        let registered: Result<RegisterNamespaceResponse, Status> = client
            .unary(
                "RegisterNamespace",
                RegisterNamespaceRequest {
                    namespace: namespace.to_owned(),
                    workflow_execution_retention_period: Some(seconds(86_400)),
                    ..Default::default()
                },
                &[],
            )
            .await;
        match registered {
            Ok(_) => {}
            Err(status) if status.code() == Code::AlreadyExists => {}
            Err(status) => bail!("namespace registration failed: {status}"),
        }
    }

    // Start through the client transport.
    let started: StartWorkflowExecutionResponse = client
        .unary(
            "StartWorkflowExecution",
            StartWorkflowExecutionRequest {
                namespace: namespace.to_owned(),
                workflow_id: workflow_id.clone(),
                workflow_type: Some(WorkflowType {
                    name: "listener-scenario".to_owned(),
                }),
                task_queue: Some(task_queue(&queue)),
                request_id: format!("start-{label}"),
                identity: client_id.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await?;
    let run_id = started.run_id;

    // First task: schedule an activity.
    let task = poll_wft(worker, namespace, &queue, &identity).await?;
    ensure!(
        !task.task_token.is_empty(),
        "first workflow task must be delivered"
    );
    ensure!(history_has(&task, EventType::WorkflowExecutionStarted));
    respond_wft(
        worker,
        namespace,
        task.task_token,
        &identity,
        vec![schedule_activity("act-1", &queue)],
        Vec::new(),
        BTreeMap::new(),
    )
    .await?;

    // The activity heartbeats and completes.
    let activity = poll_activity(worker, namespace, &queue, &identity).await?;
    ensure!(
        activity.activity_id == "act-1",
        "unexpected activity {}",
        activity.activity_id
    );
    let heartbeat: RecordActivityTaskHeartbeatResponse = worker
        .unary(
            "RecordActivityTaskHeartbeat",
            RecordActivityTaskHeartbeatRequest {
                task_token: activity.task_token.clone(),
                details: Some(payloads("progress")),
                identity: identity.clone(),
                namespace: namespace.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await?;
    ensure!(!heartbeat.cancel_requested, "act-1 was never cancelled");
    let _: RespondActivityTaskCompletedResponse = worker
        .unary(
            "RespondActivityTaskCompleted",
            RespondActivityTaskCompletedRequest {
                task_token: activity.task_token,
                result: Some(payloads("act-1-done")),
                identity: identity.clone(),
                namespace: namespace.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await?;

    // A signal lands while the post-activity task is scheduled; the task
    // carries both events.
    let _: SignalWorkflowExecutionResponse = client
        .unary(
            "SignalWorkflowExecution",
            SignalWorkflowExecutionRequest {
                namespace: namespace.to_owned(),
                workflow_execution: Some(execution(&workflow_id, &run_id)),
                signal_name: "go".to_owned(),
                input: Some(payloads("1")),
                identity: client_id.to_owned(),
                request_id: format!("signal-{label}-go"),
                ..Default::default()
            },
            &[],
        )
        .await?;
    let task = poll_wft(worker, namespace, &queue, &identity).await?;
    ensure!(history_has(&task, EventType::ActivityTaskCompleted));
    ensure!(history_has(&task, EventType::WorkflowExecutionSignaled));
    respond_wft(
        worker,
        namespace,
        task.task_token,
        &identity,
        Vec::new(),
        Vec::new(),
        BTreeMap::new(),
    )
    .await?;

    // A query is answered by the worker.
    let query = {
        let client = client.clone();
        let namespace = namespace.to_owned();
        let execution = execution(&workflow_id, &run_id);
        tokio::spawn(async move {
            client
                .unary::<_, QueryWorkflowResponse>(
                    "QueryWorkflow",
                    QueryWorkflowRequest {
                        namespace,
                        execution: Some(execution),
                        query: Some(WorkflowQuery {
                            query_type: "state".to_owned(),
                            query_args: Some(payloads("")),
                            header: None,
                        }),
                        ..Default::default()
                    },
                    &[],
                )
                .await
        })
    };
    let task = poll_wft(worker, namespace, &queue, &identity).await?;
    if let Some(_legacy) = task.query.as_ref() {
        let _: RespondQueryTaskCompletedResponse = worker
            .unary(
                "RespondQueryTaskCompleted",
                RespondQueryTaskCompletedRequest {
                    task_token: task.task_token.clone(),
                    completed_type: tokeira_proto::enums::QueryResultType::Answered as i32,
                    query_result: Some(payloads("answered")),
                    namespace: namespace.to_owned(),
                    ..Default::default()
                },
                &[],
            )
            .await?;
    } else {
        ensure!(
            !task.queries.is_empty(),
            "the query must ride the workflow task"
        );
        let query_results = task
            .queries
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    WorkflowQueryResult {
                        result_type: QueryResultType::Answered as i32,
                        answer: Some(payloads("answered")),
                        error_message: String::new(),
                        failure: None,
                    },
                )
            })
            .collect();
        respond_wft(
            worker,
            namespace,
            task.task_token,
            &identity,
            Vec::new(),
            Vec::new(),
            query_results,
        )
        .await?;
    }
    let answer = tokio::time::timeout(STEP, query)
        .await
        .context("query did not return")??
        .context("query failed")?;
    ensure!(
        answer.query_result == Some(payloads("answered")),
        "query answer must round-trip: {answer:?}"
    );

    // An update is accepted and completed through protocol messages.
    let update = {
        let client = client.clone();
        let namespace = namespace.to_owned();
        let execution = execution(&workflow_id, &run_id);
        let update_id = format!("update-{label}");
        tokio::spawn(async move {
            client
                .unary::<_, UpdateWorkflowExecutionResponse>(
                    "UpdateWorkflowExecution",
                    UpdateWorkflowExecutionRequest {
                        namespace,
                        workflow_execution: Some(execution),
                        request: Some(UpdateRequest {
                            meta: Some(UpdateMeta {
                                update_id,
                                identity: "scenario-client".to_owned(),
                            }),
                            input: Some(UpdateInput {
                                header: None,
                                name: "set".to_owned(),
                                args: Some(payloads("10")),
                            }),
                        }),
                        wait_policy: Some(WaitPolicy { lifecycle_stage: 3 }),
                        ..Default::default()
                    },
                    &[],
                )
                .await
        })
    };
    let task = poll_update_task(worker, namespace, &queue, &identity, &workflow_id).await?;
    respond_update(worker, namespace, &identity, task, "5", false).await?;
    let updated = tokio::time::timeout(STEP, update)
        .await
        .context("update did not return")??
        .context("update failed")?;
    let Some(outcome::Value::Success(result)) = updated.outcome.and_then(|outcome| outcome.value)
    else {
        bail!("update must complete successfully");
    };
    ensure!(result == payloads("5"));

    // Update-with-start on a second workflow, closed by the same completion.
    let second_id = format!("wf-{label}-uws");
    let multi = {
        let client = client.clone();
        let namespace = namespace.to_owned();
        let queue = queue.clone();
        let second_id = second_id.clone();
        let label = label.to_owned();
        tokio::spawn(async move {
            client
                .unary::<_, ExecuteMultiOperationResponse>(
                    "ExecuteMultiOperation",
                    ExecuteMultiOperationRequest {
                        namespace: namespace.clone(),
                        operations: vec![
                            execute_multi_operation_request::Operation {
                                operation: Some(
                                    execute_multi_operation_request::operation::Operation::StartWorkflow(
                                        StartWorkflowExecutionRequest {
                                            namespace: namespace.clone(),
                                            workflow_id: second_id.clone(),
                                            workflow_type: Some(WorkflowType {
                                                name: "listener-scenario".to_owned(),
                                            }),
                                            task_queue: Some(task_queue(&queue)),
                                            request_id: format!("uws-start-{label}"),
                                            identity: "scenario-client".to_owned(),
                                            workflow_id_conflict_policy:
                                                WorkflowIdConflictPolicy::Fail as i32,
                                            ..Default::default()
                                        },
                                    ),
                                ),
                            },
                            execute_multi_operation_request::Operation {
                                operation: Some(
                                    execute_multi_operation_request::operation::Operation::UpdateWorkflow(
                                        UpdateWorkflowExecutionRequest {
                                            namespace,
                                            workflow_execution: Some(execution(&second_id, "")),
                                            request: Some(UpdateRequest {
                                                meta: Some(UpdateMeta {
                                                    update_id: format!("uws-update-{label}"),
                                                    identity: "scenario-client".to_owned(),
                                                }),
                                                input: Some(UpdateInput {
                                                    header: None,
                                                    name: "init".to_owned(),
                                                    args: Some(payloads("1")),
                                                }),
                                            }),
                                            wait_policy: Some(WaitPolicy { lifecycle_stage: 3 }),
                                            ..Default::default()
                                        },
                                    ),
                                ),
                            },
                        ],
                        ..Default::default()
                    },
                    &[],
                )
                .await
        })
    };
    let task = poll_update_task(worker, namespace, &queue, &identity, &second_id).await?;
    respond_update(worker, namespace, &identity, task, "init-done", true).await?;
    let multi = tokio::time::timeout(STEP, multi)
        .await
        .context("update-with-start did not return")??
        .context("update-with-start failed")?;
    ensure!(
        multi.responses.len() == 2,
        "update-with-start returns both responses"
    );
    let Some(execute_multi_operation_response::response::Response::UpdateWorkflow(uws_update)) =
        multi.responses[1].response.clone()
    else {
        bail!("second multi-operation response must be the update");
    };
    ensure!(
        matches!(
            uws_update.outcome.and_then(|outcome| outcome.value),
            Some(outcome::Value::Success(_))
        ),
        "update-with-start update must succeed"
    );

    // Activity cancellation: schedule, request cancel, observe through the
    // heartbeat, report cancelled, then complete the workflow.
    let _: SignalWorkflowExecutionResponse = client
        .unary(
            "SignalWorkflowExecution",
            SignalWorkflowExecutionRequest {
                namespace: namespace.to_owned(),
                workflow_execution: Some(execution(&workflow_id, &run_id)),
                signal_name: "schedule-cancel".to_owned(),
                identity: client_id.to_owned(),
                request_id: format!("signal-{label}-schedule"),
                ..Default::default()
            },
            &[],
        )
        .await?;
    let task = poll_wft(worker, namespace, &queue, &identity).await?;
    respond_wft(
        worker,
        namespace,
        task.task_token,
        &identity,
        vec![schedule_activity("act-2", &queue)],
        Vec::new(),
        BTreeMap::new(),
    )
    .await?;
    let cancelled_activity = poll_activity(worker, namespace, &queue, &identity).await?;
    ensure!(cancelled_activity.activity_id == "act-2");
    let _: SignalWorkflowExecutionResponse = client
        .unary(
            "SignalWorkflowExecution",
            SignalWorkflowExecutionRequest {
                namespace: namespace.to_owned(),
                workflow_execution: Some(execution(&workflow_id, &run_id)),
                signal_name: "cancel".to_owned(),
                identity: client_id.to_owned(),
                request_id: format!("signal-{label}-cancel"),
                ..Default::default()
            },
            &[],
        )
        .await?;
    let task = poll_wft(worker, namespace, &queue, &identity).await?;
    let act_2 = scheduled_event_id(&task, "act-2").context("act-2 scheduled event id")?;
    respond_wft(
        worker,
        namespace,
        task.task_token,
        &identity,
        vec![Command {
            command_type: CommandType::RequestCancelActivityTask as i32,
            user_metadata: None,
            attributes: Some(
                CommandAttributes::RequestCancelActivityTaskCommandAttributes(
                    RequestCancelActivityTaskCommandAttributes {
                        scheduled_event_id: act_2,
                    },
                ),
            ),
        }],
        Vec::new(),
        BTreeMap::new(),
    )
    .await?;
    let heartbeat: RecordActivityTaskHeartbeatResponse = worker
        .unary(
            "RecordActivityTaskHeartbeat",
            RecordActivityTaskHeartbeatRequest {
                task_token: cancelled_activity.task_token.clone(),
                details: Some(payloads("still-running")),
                identity: identity.clone(),
                namespace: namespace.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await?;
    ensure!(
        heartbeat.cancel_requested,
        "the heartbeat must report the cancel request"
    );
    let _: RespondActivityTaskCanceledResponse = worker
        .unary(
            "RespondActivityTaskCanceled",
            RespondActivityTaskCanceledRequest {
                task_token: cancelled_activity.task_token,
                details: Some(payloads("cancelled")),
                identity: identity.clone(),
                namespace: namespace.to_owned(),
                ..Default::default()
            },
            &[],
        )
        .await?;
    let task = poll_wft(worker, namespace, &queue, &identity).await?;
    ensure!(history_has(&task, EventType::ActivityTaskCanceled));
    respond_wft(
        worker,
        namespace,
        task.task_token,
        &identity,
        vec![complete_workflow("finished")],
        Vec::new(),
        BTreeMap::new(),
    )
    .await?;

    // Both transports observe the same closed execution.
    let mut descriptions = Vec::new();
    let mut histories = Vec::new();
    for transport in [client, worker] {
        let described: DescribeWorkflowExecutionResponse = transport
            .unary(
                "DescribeWorkflowExecution",
                DescribeWorkflowExecutionRequest {
                    namespace: namespace.to_owned(),
                    execution: Some(execution(&workflow_id, &run_id)),
                },
                &[],
            )
            .await?;
        let status = described
            .workflow_execution_info
            .as_ref()
            .map(|info| info.status)
            .context("describe carries execution info")?;
        ensure!(
            status == WorkflowExecutionStatus::Completed as i32,
            "{} describe must show completion, got {status}",
            transport.label()
        );
        descriptions.push(described);
        let history: GetWorkflowExecutionHistoryResponse = transport
            .unary(
                "GetWorkflowExecutionHistory",
                GetWorkflowExecutionHistoryRequest {
                    namespace: namespace.to_owned(),
                    execution: Some(execution(&workflow_id, &run_id)),
                    maximum_page_size: 1000,
                    ..Default::default()
                },
                &[],
            )
            .await?;
        histories.push(history);
    }
    ensure!(
        descriptions[0] == descriptions[1],
        "describe must be identical on both transports"
    );
    ensure!(
        histories[0] == histories[1],
        "history must be identical on both transports"
    );
    let events: Vec<i32> = histories[0]
        .history
        .as_ref()
        .context("history present")?
        .events
        .iter()
        .map(|event| event.event_type)
        .collect();
    for expected in [
        EventType::WorkflowExecutionStarted,
        EventType::ActivityTaskScheduled,
        EventType::ActivityTaskStarted,
        EventType::ActivityTaskCompleted,
        EventType::WorkflowExecutionSignaled,
        EventType::WorkflowExecutionUpdateAccepted,
        EventType::WorkflowExecutionUpdateCompleted,
        EventType::ActivityTaskCancelRequested,
        EventType::ActivityTaskCanceled,
        EventType::WorkflowExecutionCompleted,
    ] {
        ensure!(
            events.contains(&(expected as i32)),
            "history must contain {expected:?}: {events:?}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Example-based coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ephemeral_and_unspecified_binds_report_the_concrete_port() -> Result<()> {
    let engine = Engine::start().await?;
    let loopback = engine.listen("127.0.0.1:0".parse()?).await?;
    assert_ne!(loopback.bound_addr().port(), 0);
    assert_eq!(loopback.bound_addr().ip().to_string(), "127.0.0.1");

    let unspecified = engine.listen("0.0.0.0:0".parse()?).await?;
    assert_ne!(unspecified.bound_addr().port(), 0);
    assert!(
        unspecified.bound_addr().ip().is_unspecified(),
        "an unspecified bind is reported unspecified; the host substitutes the reachable address"
    );
    // Reachable through the loopback interface the host would substitute.
    let via_loopback: SocketAddr =
        format!("127.0.0.1:{}", unspecified.bound_addr().port()).parse()?;
    Transport::network(via_loopback)
        .await?
        .system_info()
        .await?;

    loopback.shutdown().await?;
    unspecified.shutdown().await?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn listener_mounts_the_reflection_service() -> Result<()> {
    use tonic_reflection::pb::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request::MessageRequest, server_reflection_response::MessageResponse,
    };

    let (engine, listener, _in_process, _network) = start_engine_with_listener().await?;
    let channel =
        tonic::transport::Channel::from_shared(format!("http://{}", listener.bound_addr()))?
            .connect()
            .await?;
    let mut reflection = ServerReflectionClient::new(channel);
    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut responses = reflection
        .server_reflection_info(tokio_stream::once(request))
        .await?
        .into_inner();
    let response = tokio::time::timeout(STEP, responses.message())
        .await
        .context("reflection did not answer")??
        .context("reflection stream ended without a response")?;
    let Some(MessageResponse::ListServicesResponse(services)) = response.message_response else {
        bail!("reflection must list services");
    };
    ensure!(
        services
            .service
            .iter()
            .any(|service| service.name == WORKFLOW_SERVICE),
        "reflection must list the Workflow service"
    );
    listener.shutdown().await?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn occupied_port_fails_to_bind_and_the_engine_keeps_serving() -> Result<()> {
    let occupied = StdTcpListener::bind("127.0.0.1:0")?;
    let addr = occupied.local_addr()?;
    let engine = Engine::start().await?;

    let error = engine
        .listen(addr)
        .await
        .expect_err("an occupied port must not bind");
    assert!(
        matches!(error, EngineListenError::Bind { addr: failed, .. } if failed == addr),
        "unexpected error: {error}"
    );
    assert!(error.to_string().contains(&addr.to_string()));

    Transport::InProcess(engine.endpoint())
        .system_info()
        .await?;
    let recovered = engine.listen("127.0.0.1:0".parse()?).await?;
    Transport::network(recovered.bound_addr())
        .await?
        .system_info()
        .await?;
    recovered.shutdown().await?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn dropped_listener_releases_its_port_and_the_engine_keeps_serving() -> Result<()> {
    let (engine, listener, in_process, network) = start_engine_with_listener().await?;
    let addr = listener.bound_addr();
    network.system_info().await?;

    drop(listener);
    wait_until_refused(addr).await?;
    in_process.system_info().await?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn statuses_and_details_match_across_transports() -> Result<()> {
    let (engine, listener, in_process, network) = start_engine_with_listener().await?;

    // NOT_FOUND on a missing execution.
    let missing = DescribeWorkflowExecutionRequest {
        namespace: "default".to_owned(),
        execution: Some(execution("no-such-workflow", "")),
    };
    let in_process_status = in_process
        .unary::<_, DescribeWorkflowExecutionResponse>(
            "DescribeWorkflowExecution",
            missing.clone(),
            &[],
        )
        .await
        .expect_err("missing execution is NOT_FOUND");
    let network_status = network
        .unary::<_, DescribeWorkflowExecutionResponse>("DescribeWorkflowExecution", missing, &[])
        .await
        .expect_err("missing execution is NOT_FOUND");
    assert_eq!(in_process_status.code(), Code::NotFound);
    assert_eq!(network_status.code(), in_process_status.code());
    assert_eq!(network_status.message(), in_process_status.message());

    // ALREADY_EXISTS with rich details on a rejected duplicate start.
    let start = StartWorkflowExecutionRequest {
        namespace: "default".to_owned(),
        workflow_id: "duplicate".to_owned(),
        workflow_type: Some(WorkflowType {
            name: "listener-scenario".to_owned(),
        }),
        task_queue: Some(task_queue("duplicate-queue")),
        request_id: "duplicate-1".to_owned(),
        workflow_id_reuse_policy: WorkflowIdReusePolicy::RejectDuplicate as i32,
        ..Default::default()
    };
    let _: StartWorkflowExecutionResponse = in_process
        .unary("StartWorkflowExecution", start.clone(), &[])
        .await?;
    let duplicate = StartWorkflowExecutionRequest {
        request_id: "duplicate-2".to_owned(),
        ..start
    };
    let in_process_status = in_process
        .unary::<_, StartWorkflowExecutionResponse>(
            "StartWorkflowExecution",
            duplicate.clone(),
            &[],
        )
        .await
        .expect_err("duplicate start is rejected");
    let network_status = network
        .unary::<_, StartWorkflowExecutionResponse>("StartWorkflowExecution", duplicate, &[])
        .await
        .expect_err("duplicate start is rejected");
    assert_eq!(in_process_status.code(), Code::AlreadyExists);
    assert_eq!(network_status.code(), in_process_status.code());
    assert_eq!(network_status.message(), in_process_status.message());
    assert!(
        !in_process_status.details().is_empty(),
        "the rejected start carries a rich status detail"
    );
    assert_eq!(network_status.details(), in_process_status.details());

    listener.shutdown().await?;
    engine.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Property 5: engine shutdown drains listeners first and within the deadline
// ---------------------------------------------------------------------------

// Feature: embedded-engine-listener, Property 5: engine shutdown drains listeners first
#[tokio::test]
async fn engine_shutdown_resets_parked_long_polls_within_the_deadline() -> Result<()> {
    let (engine, _listener, in_process, network) = start_engine_with_listener().await?;

    let parked = {
        let network = network.clone();
        tokio::spawn(async move {
            network
                .unary::<_, PollWorkflowTaskQueueResponse>(
                    "PollWorkflowTaskQueue",
                    PollWorkflowTaskQueueRequest {
                        namespace: "default".to_owned(),
                        task_queue: Some(task_queue("idle-queue")),
                        identity: "parked-worker".to_owned(),
                        ..Default::default()
                    },
                    &[],
                )
                .await
        })
    };
    // Synchronise on the poller becoming visible rather than sleeping.
    tokio::time::timeout(STEP, async {
        loop {
            let described: DescribeTaskQueueResponse = in_process
                .unary(
                    "DescribeTaskQueue",
                    DescribeTaskQueueRequest {
                        namespace: "default".to_owned(),
                        task_queue: Some(task_queue("idle-queue")),
                        ..Default::default()
                    },
                    &[],
                )
                .await
                .expect("describe task queue");
            if described
                .pollers
                .iter()
                .any(|poller| poller.identity == "parked-worker")
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("the parked poller never became visible")?;

    // A second poll the client abandons: the reset must release it too.
    let abandoned = {
        let network = network.clone();
        tokio::spawn(async move {
            network
                .unary::<_, PollWorkflowTaskQueueResponse>(
                    "PollWorkflowTaskQueue",
                    PollWorkflowTaskQueueRequest {
                        namespace: "default".to_owned(),
                        task_queue: Some(task_queue("idle-queue")),
                        identity: "abandoned-worker".to_owned(),
                        ..Default::default()
                    },
                    &[],
                )
                .await
        })
    };
    abandoned.abort();

    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(15), engine.shutdown())
        .await
        .context("engine shutdown must not wait for the 60 s poll timeout")??;
    ensure!(
        started.elapsed() < Duration::from_secs(15),
        "shutdown took {:?}",
        started.elapsed()
    );

    let outcome = tokio::time::timeout(STEP, parked)
        .await
        .context("the parked poll must be released by shutdown")??;
    ensure!(
        outcome.is_err(),
        "a reset long poll must not return a task: {outcome:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Property 3: bind failure is a no-op
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum UnbindableAddress {
    OccupiedPort,
    UnroutableInterface,
}

fn unbindable_strategy() -> impl Strategy<Value = UnbindableAddress> {
    prop_oneof![
        Just(UnbindableAddress::OccupiedPort),
        Just(UnbindableAddress::UnroutableInterface),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(6))]

    // Feature: embedded-engine-listener, Property 3: bind failure is a no-op
    #[test]
    fn bind_failure_is_a_no_op(kind in unbindable_strategy()) {
        runtime().block_on(async {
            let occupied = StdTcpListener::bind("127.0.0.1:0").expect("reserve a port");
            let addr: SocketAddr = match kind {
                UnbindableAddress::OccupiedPort => occupied.local_addr().expect("reserved addr"),
                // TEST-NET-1 is never assigned to a local interface.
                UnbindableAddress::UnroutableInterface => "192.0.2.1:0".parse().expect("static"),
            };
            let engine = Engine::start().await.expect("engine starts");
            let report = engine.startup_report().clone();

            let error = engine.listen(addr).await.expect_err("address must not bind");
            let bind_failed_as_requested =
                matches!(error, EngineListenError::Bind { addr: failed, .. } if failed == addr);
            prop_assert!(bind_failed_as_requested, "unexpected error: {}", error);
            prop_assert_eq!(engine.startup_report(), &report);
            prop_assert!(Transport::InProcess(engine.endpoint()).system_info().await.is_ok());

            // The engine is unchanged: a later bind still succeeds and serves.
            let listener = engine.listen("127.0.0.1:0".parse().expect("static")).await.expect("later bind");
            prop_assert!(Transport::network(listener.bound_addr()).await.expect("connect").system_info().await.is_ok());
            listener.shutdown().await.expect("listener stops");
            prop_assert!(engine.shutdown().await.is_ok());
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Property 4: listener lifecycle state machine
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum LifecycleOp {
    Listen,
    Shutdown(usize),
    Drop(usize),
}

fn lifecycle_op_strategy() -> impl Strategy<Value = LifecycleOp> {
    prop_oneof![
        3 => Just(LifecycleOp::Listen),
        1 => (0usize..4).prop_map(LifecycleOp::Shutdown),
        1 => (0usize..4).prop_map(LifecycleOp::Drop),
    ]
}

async fn assert_serving(addr: SocketAddr) -> Result<()> {
    Transport::network(addr)
        .await
        .with_context(|| format!("connect to live listener {addr}"))?
        .system_info()
        .await
        .with_context(|| format!("live listener {addr} must serve"))?;
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]

    // Feature: embedded-engine-listener, Property 4: listener lifecycle state machine
    #[test]
    fn listener_lifecycle_follows_the_reference_model(
        ops in prop::collection::vec(lifecycle_op_strategy(), 1..6),
        graceful_end in any::<bool>(),
    ) {
        runtime().block_on(async {
            let engine = Engine::start().await.expect("engine starts");
            let in_process = Transport::InProcess(engine.endpoint());
            // Reference model: listeners that are live, and addresses that must refuse.
            let mut live: Vec<(SocketAddr, EngineListener)> = Vec::new();
            let mut stopped: Vec<SocketAddr> = Vec::new();
            for op in ops {
                match op {
                    LifecycleOp::Listen => {
                        let listener = engine.listen("127.0.0.1:0".parse().expect("static")).await.expect("bind");
                        live.push((listener.bound_addr(), listener));
                    }
                    LifecycleOp::Shutdown(index) if !live.is_empty() => {
                        let (addr, listener) = live.remove(index % live.len());
                        listener.shutdown().await.expect("listener shutdown");
                        stopped.push(addr);
                    }
                    LifecycleOp::Drop(index) if !live.is_empty() => {
                        let (addr, listener) = live.remove(index % live.len());
                        drop(listener);
                        stopped.push(addr);
                    }
                    LifecycleOp::Shutdown(_) | LifecycleOp::Drop(_) => {}
                }
                for (addr, _) in &live {
                    assert_serving(*addr).await.expect("live listener serves");
                }
                for addr in &stopped {
                    wait_until_refused(*addr).await.expect("stopped listener refuses");
                }
                prop_assert!(in_process.system_info().await.is_ok());
            }
            let remaining: Vec<SocketAddr> = live.iter().map(|(addr, _)| *addr).collect();
            if graceful_end {
                let handles: Vec<EngineListener> = live.into_iter().map(|(_, listener)| listener).collect();
                prop_assert!(engine.shutdown().await.is_ok());
                // Handles outliving engine shutdown are inert.
                for handle in handles {
                    prop_assert!(handle.shutdown().await.is_ok());
                }
            } else {
                drop(live);
                drop(engine);
            }
            for addr in remaining.iter().chain(stopped.iter()) {
                wait_until_refused(*addr).await.expect("every listener refuses after the engine ends");
            }
            prop_assert!(in_process.system_info().await.is_err());
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// Property 1: one engine behind two transports
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    // Feature: embedded-engine-listener, Property 1: one engine behind two transports
    #[test]
    fn one_engine_behind_two_transports(
        assignment in 0u8..4,
        cloud_namespace in any::<bool>(),
    ) {
        runtime().block_on(async {
            let (engine, listener, in_process, network) =
                start_engine_with_listener().await.expect("engine with listener");
            let client = if assignment & 1 == 1 { network.clone() } else { in_process.clone() };
            let worker = if assignment & 2 == 2 { network.clone() } else { in_process.clone() };
            let namespace = if cloud_namespace { "tokeira-cloud" } else { "default" };
            let label = format!("{}-{}-{namespace}", client.label(), worker.label());
            let outcome = run_scenario(&client, &worker, namespace, &label).await;
            listener.shutdown().await.expect("listener stops");
            engine.shutdown().await.expect("engine stops");
            if let Err(error) = outcome {
                return Err(TestCaseError::fail(format!("scenario failed: {error:#}")));
            }
            Ok(())
        })?;
    }
}
