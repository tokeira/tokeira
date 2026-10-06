//! Temporal v1.31.0's limits on a workflow task's commands, through the
//! in-process gRPC endpoint on the in-memory store
//! (`workflow-task-command-limits`, Property 2).
//!
//! A command over a size limit terminates the workflow: v1.31.0 records
//! WorkflowTaskFailed with the command's cause, the buffered events and
//! WorkflowExecutionTerminated, and answers the worker `InvalidArgument`
//! (`service/history/api/respondworkflowtaskcompleted/api.go:470-512, 739-743
//! @ v1.31.0`). On a later attempt nothing is persisted. An oversized query
//! result fails that query.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context as _, Result, ensure};
use http::HeaderMap;
use proptest::prelude::*;
use tokeira_engine::{Engine, InProcessGrpcRequest, TemporalEndpoint};
use tokeira_proto::{
    common::{Memo, Payload, Payloads, SearchAttributes, WorkflowExecution, WorkflowType},
    enums::{CommandType, QueryResultType, WorkflowTaskFailedCause},
    failure::Failure,
    history::{HistoryEvent, history_event::Attributes as EventAttributes},
    public::temporal::api::{
        command::v1::{
            Command, CompleteWorkflowExecutionCommandAttributes,
            FailWorkflowExecutionCommandAttributes, ModifyWorkflowPropertiesCommandAttributes,
            RecordMarkerCommandAttributes, ScheduleActivityTaskCommandAttributes,
            SignalExternalWorkflowExecutionCommandAttributes,
            StartChildWorkflowExecutionCommandAttributes,
            UpsertWorkflowSearchAttributesCommandAttributes, command::Attributes,
        },
        query::v1::{WorkflowQuery, WorkflowQueryResult},
    },
    taskqueue::TaskQueue,
    workflowservice::{
        GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
        PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse, QueryWorkflowRequest,
        QueryWorkflowResponse, RespondQueryTaskCompletedRequest, RespondQueryTaskCompletedResponse,
        RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
        SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    },
};
use tonic::{Code, Status};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
const NAMESPACE: &str = "default";
const IDENTITY: &str = "command-limits-worker";
const BLOB_SIZE_LIMIT: usize = 2 * 1024 * 1024;

async fn try_call<Req, Resp>(
    endpoint: &TemporalEndpoint,
    rpc: &str,
    request: Req,
) -> std::result::Result<Resp, Status>
where
    Req: prost::Message,
    Resp: prost::Message + Default,
{
    let response = endpoint
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: rpc.to_owned(),
            headers: HeaderMap::new(),
            proto: request.encode_to_vec().into(),
        })
        .await?;
    Resp::decode(response.proto.as_slice()).map_err(|error| Status::internal(error.to_string()))
}

async fn call<Req, Resp>(endpoint: &TemporalEndpoint, rpc: &str, request: Req) -> Result<Resp>
where
    Req: prost::Message,
    Resp: prost::Message + Default,
{
    try_call(endpoint, rpc, request)
        .await
        .with_context(|| format!("{rpc} failed"))
}

fn task_queue(name: &str) -> Option<TaskQueue> {
    Some(TaskQueue {
        name: name.to_owned(),
        ..Default::default()
    })
}

fn payload(data_len: usize) -> Payload {
    Payload {
        metadata: BTreeMap::from([("encoding".to_owned(), b"binary/plain".to_vec())]),
        data: vec![b'x'; data_len],
        ..Default::default()
    }
}

/// One payload whose `Payloads` encodes to more than `target` bytes.
fn payloads_over(target: usize) -> Payloads {
    Payloads {
        payloads: vec![payload(target)],
    }
}

fn execution(name: &str) -> Option<WorkflowExecution> {
    Some(WorkflowExecution {
        workflow_id: name.to_owned(),
        run_id: String::new(),
    })
}

async fn start(endpoint: &TemporalEndpoint, name: &str) -> Result<()> {
    let _: StartWorkflowExecutionResponse = call(
        endpoint,
        "StartWorkflowExecution",
        StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: name.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "command-limits".to_owned(),
            }),
            task_queue: task_queue(&format!("{name}-queue")),
            request_id: format!("start-{name}"),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn poll(endpoint: &TemporalEndpoint, name: &str) -> Result<PollWorkflowTaskQueueResponse> {
    let task: PollWorkflowTaskQueueResponse = call(
        endpoint,
        "PollWorkflowTaskQueue",
        PollWorkflowTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(&format!("{name}-queue")),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    ensure!(!task.task_token.is_empty(), "workflow task pollable");
    Ok(task)
}

async fn signal(endpoint: &TemporalEndpoint, name: &str, request_id: &str) -> Result<()> {
    let _: SignalWorkflowExecutionResponse = call(
        endpoint,
        "SignalWorkflowExecution",
        SignalWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_execution: execution(name),
            signal_name: "signal".to_owned(),
            identity: IDENTITY.to_owned(),
            request_id: request_id.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn complete(
    endpoint: &TemporalEndpoint,
    task_token: Vec<u8>,
    commands: Vec<Command>,
    query_results: BTreeMap<String, WorkflowQueryResult>,
) -> std::result::Result<RespondWorkflowTaskCompletedResponse, Status> {
    try_call(
        endpoint,
        "RespondWorkflowTaskCompleted",
        RespondWorkflowTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token,
            commands,
            identity: IDENTITY.to_owned(),
            query_results,
            ..Default::default()
        },
    )
    .await
}

async fn history(endpoint: &TemporalEndpoint, name: &str) -> Result<Vec<HistoryEvent>> {
    let response: GetWorkflowExecutionHistoryResponse = call(
        endpoint,
        "GetWorkflowExecutionHistory",
        GetWorkflowExecutionHistoryRequest {
            namespace: NAMESPACE.to_owned(),
            execution: execution(name),
            ..Default::default()
        },
    )
    .await?;
    Ok(response.history.unwrap_or_default().events)
}

fn command(command_type: CommandType, attributes: Attributes) -> Command {
    Command {
        command_type: command_type as i32,
        user_metadata: None,
        attributes: Some(attributes),
    }
}

/// A command one byte or more over a size limit, with v1.31.0's cause and
/// message for it.
fn oversized(kind: usize, over: usize) -> (Command, WorkflowTaskFailedCause, &'static str) {
    use WorkflowTaskFailedCause as C;
    let target = BLOB_SIZE_LIMIT + over;
    match kind {
        0 => (
            command(
                CommandType::ScheduleActivityTask,
                Attributes::ScheduleActivityTaskCommandAttributes(
                    ScheduleActivityTaskCommandAttributes {
                        activity_id: "activity".to_owned(),
                        activity_type: Some(tokeira_proto::common::ActivityType {
                            name: "work".to_owned(),
                        }),
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: 10,
                            nanos: 0,
                        }),
                        input: Some(payloads_over(target)),
                        ..Default::default()
                    },
                ),
            ),
            C::BadScheduleActivityAttributes,
            "ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
        ),
        1 => (
            command(
                CommandType::CompleteWorkflowExecution,
                Attributes::CompleteWorkflowExecutionCommandAttributes(
                    CompleteWorkflowExecutionCommandAttributes {
                        result: Some(payloads_over(target)),
                    },
                ),
            ),
            // v1.31.0's own choice of cause.
            C::BadScheduleActivityAttributes,
            "CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.",
        ),
        2 => (
            command(
                CommandType::FailWorkflowExecution,
                Attributes::FailWorkflowExecutionCommandAttributes(
                    FailWorkflowExecutionCommandAttributes {
                        failure: Some(Failure {
                            message: "x".repeat(target),
                            ..Default::default()
                        }),
                    },
                ),
            ),
            C::BadFailWorkflowExecutionAttributes,
            "FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.",
        ),
        3 => (
            command(
                CommandType::RecordMarker,
                Attributes::RecordMarkerCommandAttributes(RecordMarkerCommandAttributes {
                    marker_name: "marker".to_owned(),
                    details: BTreeMap::from([("details".to_owned(), payloads_over(target))]),
                    ..Default::default()
                }),
            ),
            C::BadRecordMarkerAttributes,
            "RecordMarkerCommandAttributes.Details exceeds size limit.",
        ),
        4 => (
            command(
                CommandType::SignalExternalWorkflowExecution,
                Attributes::SignalExternalWorkflowExecutionCommandAttributes(
                    SignalExternalWorkflowExecutionCommandAttributes {
                        execution: execution("signal-target"),
                        signal_name: "signal".to_owned(),
                        input: Some(payloads_over(target)),
                        ..Default::default()
                    },
                ),
            ),
            C::BadSignalWorkflowExecutionAttributes,
            "SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.",
        ),
        5 => (
            command(
                CommandType::StartChildWorkflowExecution,
                Attributes::StartChildWorkflowExecutionCommandAttributes(
                    StartChildWorkflowExecutionCommandAttributes {
                        workflow_id: "child".to_owned(),
                        workflow_type: Some(WorkflowType {
                            name: "child".to_owned(),
                        }),
                        input: Some(payloads_over(target)),
                        ..Default::default()
                    },
                ),
            ),
            C::BadStartChildExecutionAttributes,
            "StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.",
        ),
        6 => (
            command(
                CommandType::StartChildWorkflowExecution,
                Attributes::StartChildWorkflowExecutionCommandAttributes(
                    StartChildWorkflowExecutionCommandAttributes {
                        workflow_id: "child".to_owned(),
                        workflow_type: Some(WorkflowType {
                            name: "child".to_owned(),
                        }),
                        memo: Some(Memo {
                            fields: BTreeMap::from([("memo".to_owned(), payload(target))]),
                        }),
                        ..Default::default()
                    },
                ),
            ),
            C::BadStartChildExecutionAttributes,
            "StartChildWorkflowExecutionCommandAttributes.Memo exceeds size limit.",
        ),
        _ => (
            command(
                CommandType::ModifyWorkflowProperties,
                Attributes::ModifyWorkflowPropertiesCommandAttributes(
                    ModifyWorkflowPropertiesCommandAttributes {
                        upserted_memo: Some(Memo {
                            fields: BTreeMap::from([("memo".to_owned(), payload(target))]),
                        }),
                    },
                ),
            ),
            C::BadModifyWorkflowPropertiesAttributes,
            "ModifyWorkflowPropertiesCommandAttributes exceeds size limit.",
        ),
    }
}

const KINDS: usize = 8;
/// CompleteWorkflowExecution and FailWorkflowExecution.
const CLOSE_KINDS: [usize; 2] = [1, 2];

async fn check_termination(
    endpoint: &TemporalEndpoint,
    kind: usize,
    buffered: bool,
    over: usize,
) -> Result<()> {
    let name = format!("command-limits-{kind}-{buffered}-{over}");
    start(endpoint, &name).await?;
    let task = poll(endpoint, &name).await?;
    if buffered {
        signal(endpoint, &name, &format!("{name}-signal")).await?;
    }
    let (command, cause, message) = oversized(kind, over);
    let status = complete(endpoint, task.task_token, vec![command], BTreeMap::new())
        .await
        .expect_err("the completion is refused");
    ensure!(status.code() == Code::InvalidArgument, "{status:?}");
    if buffered && CLOSE_KINDS.contains(&kind) {
        // A close command with buffered events fails with UnhandledCommand
        // before its size is checked (workflow_task_completed_handler.go:
        // 693-695, 748-750 @ v1.31.0).
        ensure!(
            status.message() == "UnhandledCommand",
            "{}",
            status.message()
        );
        return Ok(());
    }
    let expected = format!("{}: {message}", cause.as_str_name_pascal());
    ensure!(
        status.message() == expected,
        "{} != {expected}",
        status.message()
    );

    let events = history(endpoint, &name).await?;
    let failed = events
        .iter()
        .position(|event| {
            matches!(
                event.attributes,
                Some(EventAttributes::WorkflowTaskFailedEventAttributes(_))
            )
        })
        .context("the workflow task failure is recorded")?;
    let tail: Vec<_> = events[failed..]
        .iter()
        .filter_map(|event| event.attributes.as_ref())
        .collect();
    match tail[0] {
        EventAttributes::WorkflowTaskFailedEventAttributes(attributes) => {
            ensure!(attributes.cause == cause as i32);
            ensure!(attributes.identity == IDENTITY);
            let failure = attributes.failure.as_ref().context("a server failure")?;
            ensure!(failure.message == expected);
        }
        other => anyhow::bail!("unexpected {other:?}"),
    }
    let terminated = if buffered {
        ensure!(matches!(
            tail[1],
            EventAttributes::WorkflowExecutionSignaledEventAttributes(_)
        ));
        tail[2]
    } else {
        tail[1]
    };
    match terminated {
        EventAttributes::WorkflowExecutionTerminatedEventAttributes(attributes) => {
            ensure!(attributes.reason == expected);
            ensure!(attributes.identity == "history-service");
            ensure!(attributes.details.is_none());
        }
        other => anyhow::bail!("unexpected {other:?}"),
    }
    ensure!(
        tail.len() == if buffered { 3 } else { 2 },
        "nothing follows the termination: {tail:?}"
    );
    Ok(())
}

trait PascalName {
    fn as_str_name_pascal(&self) -> String;
}

impl PascalName for WorkflowTaskFailedCause {
    /// v1.31.0's `String()` rendering: `WORKFLOW_TASK_FAILED_CAUSE_BAD_X` as
    /// `BadX`.
    fn as_str_name_pascal(&self) -> String {
        self.as_str_name()
            .trim_start_matches("WORKFLOW_TASK_FAILED_CAUSE_")
            .split('_')
            .map(|word| {
                let mut chars = word.chars();
                chars.next().map_or_else(String::new, |first| {
                    first
                        .to_uppercase()
                        .chain(chars.flat_map(char::to_lowercase))
                        .collect()
                })
            })
            .collect()
    }
}

// Feature: workflow-task-command-limits, Property 2: A terminating failure is recorded as v1.31.0 records it
proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    /// Every terminating command, with and without a buffered signal, at a
    /// generated distance over the limit.
    #[test]
    fn property_oversized_commands_terminate_the_workflow(over in 1usize..64) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let engine = Engine::start().await.expect("engine");
            let endpoint = engine.endpoint();
            for kind in 0..KINDS {
                for buffered in [false, true] {
                    check_termination(&endpoint, kind, buffered, over)
                        .await
                        .unwrap_or_else(|error| panic!("kind {kind} buffered={buffered}: {error:#}"));
                }
            }
            engine.shutdown().await.expect("engine shutdown");
        });
    }
}

fn keyword(value: &str) -> Payload {
    Payload {
        metadata: BTreeMap::from([
            ("encoding".to_owned(), b"json/plain".to_vec()),
            ("type".to_owned(), b"Keyword".to_vec()),
        ]),
        data: format!("\"{value}\"").into_bytes(),
        ..Default::default()
    }
}

/// More than 100 search attributes fail the workflow task with the count
/// message, before the registered-key check; on the retried attempt an
/// oversized command persists nothing.
#[tokio::test]
async fn a_later_attempt_persists_nothing() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "command-limits-later-attempt";
    start(&endpoint, name).await?;
    let task = poll(&endpoint, name).await?;
    let unregistered: BTreeMap<String, Payload> = (0..101)
        .map(|index| (format!("Unregistered{index:03}"), keyword("value")))
        .collect();
    let status = complete(
        &endpoint,
        task.task_token,
        vec![command(
            CommandType::UpsertWorkflowSearchAttributes,
            Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                UpsertWorkflowSearchAttributesCommandAttributes {
                    search_attributes: Some(SearchAttributes {
                        indexed_fields: unregistered,
                    }),
                },
            ),
        )],
        BTreeMap::new(),
    )
    .await
    .expect_err("the completion is refused");
    ensure!(status.code() == Code::InvalidArgument);
    ensure!(
        status.message()
            == "BadSearchAttributes: number of search attributes 101 exceeds limit 100",
        "{}",
        status.message()
    );
    let retry = poll(&endpoint, name).await?;
    ensure!(retry.attempt == 2, "attempt {}", retry.attempt);
    let before = history(&endpoint, name).await?;
    let (command, _, message) = oversized(0, 1);
    let status = complete(&endpoint, retry.task_token, vec![command], BTreeMap::new())
        .await
        .expect_err("the completion is refused");
    ensure!(status.code() == Code::InvalidArgument);
    ensure!(status.message() == format!("BadScheduleActivityAttributes: {message}"));
    let after = history(&endpoint, name).await?;
    ensure!(after == before, "nothing is persisted on a later attempt");
    ensure!(
        !after.iter().any(|event| matches!(
            event.attributes,
            Some(EventAttributes::WorkflowExecutionTerminatedEventAttributes(
                _
            ))
        )),
        "the workflow is not terminated"
    );
    engine.shutdown().await?;
    Ok(())
}

async fn query(
    endpoint: TemporalEndpoint,
    name: String,
) -> std::result::Result<QueryWorkflowResponse, Status> {
    try_call(
        &endpoint,
        "QueryWorkflow",
        QueryWorkflowRequest {
            namespace: NAMESPACE.to_owned(),
            execution: execution(&name),
            query: Some(WorkflowQuery {
                query_type: "state".to_owned(),
                query_args: None,
                header: None,
            }),
            ..Default::default()
        },
    )
    .await
}

/// A query result over the blob size limit fails that query: as a query task
/// answered with RespondQueryTaskCompleted, and as a query delivered with a
/// workflow task and answered in its completion.
#[tokio::test]
async fn oversized_query_results_fail_the_query() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "command-limits-query";
    start(&endpoint, name).await?;
    let first = poll(&endpoint, name).await?;
    complete(&endpoint, first.task_token, Vec::new(), BTreeMap::new()).await?;

    // No workflow task is outstanding: the query is a query task.
    let waiting = tokio::spawn(query(endpoint.clone(), name.to_owned()));
    let task = poll(&endpoint, name).await?;
    ensure!(task.query.is_some(), "a query task");
    let _: RespondQueryTaskCompletedResponse = call(
        &endpoint,
        "RespondQueryTaskCompleted",
        RespondQueryTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token: task.task_token,
            completed_type: QueryResultType::Answered as i32,
            query_result: Some(payloads_over(BLOB_SIZE_LIMIT)),
            ..Default::default()
        },
    )
    .await
    .context("the oversized answer is accepted")?;
    let status = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await??
        .expect_err("the query fails");
    ensure!(status.code() == Code::InvalidArgument, "{status:?}");
    ensure!(status.message() == "Blob data size exceeds limit.");

    // A workflow task is outstanding: the query is buffered and rides it
    // (`safeToDispatchDirectly`, queryworkflow/api.go:147-183 @ v1.31.0).
    signal(&endpoint, name, "query-signal").await?;
    let waiting = tokio::spawn(query(endpoint.clone(), name.to_owned()));
    // Let the query reach the runtime before the task is polled.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut task = poll(&endpoint, name).await?;
    if task.queries.is_empty() {
        // The query arrived after the poll and waits for the next turn.
        complete(&endpoint, task.task_token, Vec::new(), BTreeMap::new()).await?;
        task = poll(&endpoint, name).await?;
    }
    if task.queries.is_empty() {
        // A quiescent run answers it as a query task, which fails the same way.
        let _: RespondQueryTaskCompletedResponse = call(
            &endpoint,
            "RespondQueryTaskCompleted",
            RespondQueryTaskCompletedRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: task.task_token,
                completed_type: QueryResultType::Answered as i32,
                query_result: Some(payloads_over(BLOB_SIZE_LIMIT)),
                ..Default::default()
            },
        )
        .await?;
    } else {
        let results = task
            .queries
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    WorkflowQueryResult {
                        result_type: QueryResultType::Answered as i32,
                        answer: Some(payloads_over(BLOB_SIZE_LIMIT)),
                        ..Default::default()
                    },
                )
            })
            .collect();
        complete(&endpoint, task.task_token, Vec::new(), results).await?;
    }
    let status = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await??
        .expect_err("the query fails");
    ensure!(status.code() == Code::InvalidArgument, "{status:?}");
    ensure!(status.message() == "Blob data size exceeds limit.");
    engine.shutdown().await?;
    Ok(())
}
