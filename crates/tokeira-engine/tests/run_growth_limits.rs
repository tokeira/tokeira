//! Temporal v1.31.0's limits on a run's growth, through the in-process gRPC
//! endpoint on the in-memory store, at v1.31.0's values
//! (`run-growth-limits`, Property 2).
//!
//! A write that finds the run's History Size over 50 MiB, or would leave its
//! state over 8 MiB, writes nothing and terminates the run as stored: a
//! started workflow task is force-closed, the buffered events are flushed, and
//! the run is terminated by `history-service`; the caller gets
//! `InvalidArgument` (`service/history/workflow/context.go:1002-1155 @
//! v1.31.0`). A workflow task's completion over the 4 MiB history batch
//! terminates the run the same way, with the error as details
//! (`service/history/api/respondworkflowtaskcompleted/api.go:645-674 @
//! v1.31.0`). A retried activity's failure is stored cut to 4 KiB.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result, ensure};
use http::HeaderMap;
use tokeira_engine::{Engine, InProcessGrpcRequest, TemporalEndpoint};
use tokeira_proto::{
    common::{ActivityType, Payload, Payloads, RetryPolicy, WorkflowExecution, WorkflowType},
    enums::{CommandType, EventType, WorkflowTaskFailedCause},
    failure::{Failure, failure::FailureInfo},
    history::{HistoryEvent, history_event::Attributes as EventAttributes},
    public::temporal::api::command::v1::{
        Command, ScheduleActivityTaskCommandAttributes, command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
        GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse, RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
    },
};
use tonic::{Code, Status};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
const NAMESPACE: &str = "default";
const IDENTITY: &str = "run-growth-worker";
const MIB: usize = 1024 * 1024;
const HISTORY_SIZE_LIMIT: i64 = 50 * 1024 * 1024;
const TRANSACTION_SIZE_LIMIT: usize = 4 * 1024 * 1024;
const STORED_FAILURE_LIMIT: usize = 4 * 1024;

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
        name: format!("{name}-queue"),
        ..Default::default()
    })
}

/// One `binary/plain` payload whose data is `size` bytes.
fn payloads(size: usize) -> Payloads {
    Payloads {
        payloads: vec![Payload {
            metadata: BTreeMap::from([("encoding".to_owned(), b"binary/plain".to_vec())]),
            data: vec![b'x'; size],
            ..Default::default()
        }],
    }
}

/// The largest payloads under v1.31.0's 2 MiB blob limit, with room for the
/// encoding.
fn largest_payloads() -> Payloads {
    payloads(2 * MIB - 64)
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
                name: "run-growth".to_owned(),
            }),
            task_queue: task_queue(name),
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
            task_queue: task_queue(name),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    ensure!(!task.task_token.is_empty(), "workflow task pollable");
    Ok(task)
}

async fn poll_activity(
    endpoint: &TemporalEndpoint,
    name: &str,
) -> Result<PollActivityTaskQueueResponse> {
    let task: PollActivityTaskQueueResponse = call(
        endpoint,
        "PollActivityTaskQueue",
        PollActivityTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(name),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    ensure!(!task.task_token.is_empty(), "activity task pollable");
    Ok(task)
}

async fn signal(
    endpoint: &TemporalEndpoint,
    name: &str,
    request_id: &str,
    input: Payloads,
) -> std::result::Result<SignalWorkflowExecutionResponse, Status> {
    try_call(
        endpoint,
        "SignalWorkflowExecution",
        SignalWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_execution: execution(name),
            signal_name: "signal".to_owned(),
            input: Some(input),
            identity: IDENTITY.to_owned(),
            request_id: request_id.to_owned(),
            ..Default::default()
        },
    )
    .await
}

async fn complete(
    endpoint: &TemporalEndpoint,
    task_token: Vec<u8>,
    commands: Vec<Command>,
) -> std::result::Result<RespondWorkflowTaskCompletedResponse, Status> {
    try_call(
        endpoint,
        "RespondWorkflowTaskCompleted",
        RespondWorkflowTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token,
            commands,
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await
}

async fn describe(
    endpoint: &TemporalEndpoint,
    name: &str,
) -> Result<DescribeWorkflowExecutionResponse> {
    call(
        endpoint,
        "DescribeWorkflowExecution",
        DescribeWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            execution: execution(name),
        },
    )
    .await
}

async fn history_size(endpoint: &TemporalEndpoint, name: &str) -> Result<i64> {
    Ok(describe(endpoint, name)
        .await?
        .workflow_execution_info
        .context("execution info")?
        .history_size_bytes)
}

async fn history(endpoint: &TemporalEndpoint, name: &str) -> Result<Vec<HistoryEvent>> {
    let mut events = Vec::new();
    let mut next_page_token = Vec::new();
    loop {
        let response: GetWorkflowExecutionHistoryResponse = call(
            endpoint,
            "GetWorkflowExecutionHistory",
            GetWorkflowExecutionHistoryRequest {
                namespace: NAMESPACE.to_owned(),
                execution: execution(name),
                next_page_token,
                ..Default::default()
            },
        )
        .await?;
        events.extend(response.history.unwrap_or_default().events);
        if response.next_page_token.is_empty() {
            return Ok(events);
        }
        next_page_token = response.next_page_token;
    }
}

fn schedule_activity(index: usize, input: Payloads, retry: bool) -> Command {
    Command {
        command_type: CommandType::ScheduleActivityTask as i32,
        user_metadata: None,
        attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
            ScheduleActivityTaskCommandAttributes {
                activity_id: format!("activity-{index}"),
                activity_type: Some(ActivityType {
                    name: "work".to_owned(),
                }),
                start_to_close_timeout: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
                input: Some(input),
                retry_policy: retry.then(|| RetryPolicy {
                    initial_interval: Some(prost_types::Duration {
                        seconds: 60,
                        nanos: 0,
                    }),
                    backoff_coefficient: 1.0,
                    maximum_attempts: 3,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

fn expect_invalid_argument<T: std::fmt::Debug>(
    result: std::result::Result<T, Status>,
    message: &str,
) -> Result<()> {
    let status = result.err().context("the call is refused")?;
    ensure!(
        status.code() == Code::InvalidArgument && status.message() == message,
        "expected InvalidArgument {message:?}, got {status:?}"
    );
    Ok(())
}

/// The events after `after_event_id`, as (type, attributes).
fn tail(events: &[HistoryEvent], after_event_id: i64) -> Vec<&HistoryEvent> {
    events
        .iter()
        .filter(|event| event.event_id > after_event_id)
        .collect()
}

fn event_type(event: &HistoryEvent) -> EventType {
    EventType::try_from(event.event_type).unwrap_or(EventType::Unspecified)
}

/// v1.31.0's force-close of the started task by `history-service`.
fn check_force_close(event: &HistoryEvent) -> Result<()> {
    let Some(EventAttributes::WorkflowTaskFailedEventAttributes(failed)) = &event.attributes else {
        anyhow::bail!("expected WorkflowTaskFailed, got {:?}", event_type(event));
    };
    ensure!(
        failed.cause == WorkflowTaskFailedCause::ForceCloseCommand as i32,
        "cause {:?}",
        failed.cause
    );
    ensure!(
        failed.identity == "history-service",
        "identity {:?}",
        failed.identity
    );
    Ok(())
}

/// v1.31.0's termination by `history-service`.
fn check_termination(event: &HistoryEvent, reason: &str) -> Result<Option<Payloads>> {
    let Some(EventAttributes::WorkflowExecutionTerminatedEventAttributes(terminated)) =
        &event.attributes
    else {
        anyhow::bail!(
            "expected WorkflowExecutionTerminated, got {:?}",
            event_type(event)
        );
    };
    ensure!(
        terminated.reason == reason,
        "reason {:?}",
        terminated.reason
    );
    ensure!(
        terminated.identity == "history-service",
        "identity {:?}",
        terminated.identity
    );
    Ok(terminated.details.clone())
}

/// Signals of 2 MiB until the History Size passes 50 MiB: the signal that
/// crosses it succeeds, and the next one terminates the run.
#[tokio::test(flavor = "multi_thread")]
async fn the_history_size_limit_terminates_the_run_at_the_next_write() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "history-size";
    start(&endpoint, name).await?;
    let mut sent = 0;
    while history_size(&endpoint, name).await? <= HISTORY_SIZE_LIMIT {
        ensure!(sent < 40, "the History Size never passed the limit");
        signal(
            &endpoint,
            name,
            &format!("signal-{sent}"),
            largest_payloads(),
        )
        .await
        .with_context(|| format!("signal {sent} under the limit"))?;
        sent += 1;
    }
    let before = history(&endpoint, name).await?;
    let last_event_id = before.last().context("events")?.event_id;

    expect_invalid_argument(
        signal(&endpoint, name, "signal-over", payloads(16)).await,
        "Workflow history size exceeds limit.",
    )?;
    let events = history(&endpoint, name).await?;
    let added = tail(&events, last_event_id);
    // No task was started, so nothing is force-closed.
    ensure!(
        added.len() == 1,
        "added {:?}",
        added.iter().map(|e| event_type(e)).collect::<Vec<_>>()
    );
    let details = check_termination(added[0], "Workflow history size exceeds limit.")?;
    ensure!(details.is_none(), "details {details:?}");
    engine.shutdown().await?;
    Ok(())
}

/// Heartbeat details of 2 MiB on five activities, with a workflow task started
/// and a signal buffered: the heartbeat that would take the state past 8 MiB
/// terminates the run.
#[tokio::test(flavor = "multi_thread")]
async fn the_state_size_limit_force_closes_flushes_and_terminates() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "state-size";
    start(&endpoint, name).await?;
    let first = poll(&endpoint, name).await?;
    let commands = (0..5)
        .map(|index| schedule_activity(index, payloads(8), false))
        .collect();
    complete(&endpoint, first.task_token, commands)
        .await
        .context("schedule the activities")?;
    let mut tokens = Vec::new();
    for _ in 0..5 {
        tokens.push(poll_activity(&endpoint, name).await?.task_token);
    }
    signal(&endpoint, name, "wake", payloads(8))
        .await
        .context("wake the workflow")?;
    let _started = poll(&endpoint, name).await?;
    let started_events = history(&endpoint, name).await?;
    let started_id = started_events.last().context("events")?.event_id;
    signal(&endpoint, name, "buffered", payloads(8))
        .await
        .context("buffer a signal")?;

    let mut refused = false;
    for (index, token) in tokens.into_iter().enumerate() {
        let result: std::result::Result<RecordActivityTaskHeartbeatResponse, Status> = try_call(
            &endpoint,
            "RecordActivityTaskHeartbeat",
            RecordActivityTaskHeartbeatRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: token,
                details: Some(largest_payloads()),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await;
        if result.is_err() {
            expect_invalid_argument(result, "Workflow mutable state size exceeds limit.")?;
            ensure!(index >= 3, "the state passed 8 MiB at heartbeat {index}");
            refused = true;
            break;
        }
    }
    ensure!(refused, "no heartbeat took the state past 8 MiB");

    let events = history(&endpoint, name).await?;
    let added = tail(&events, started_id);
    let types: Vec<EventType> = added.iter().map(|event| event_type(event)).collect();
    ensure!(
        types
            == [
                EventType::WorkflowTaskFailed,
                EventType::WorkflowExecutionSignaled,
                EventType::WorkflowExecutionTerminated
            ],
        "added {types:?}"
    );
    check_force_close(added[0])?;
    let details = check_termination(added[2], "Workflow mutable state size exceeds limit.")?;
    ensure!(details.is_none(), "details {details:?}");
    engine.shutdown().await?;
    Ok(())
}

/// A workflow task's completion whose history batch passes 4 MiB terminates
/// the run with the error as details. gRPC caps a request at 4 MiB, so the
/// batch passes the limit with the signal it flushes; the runtime's tests cover
/// a completion over the limit on its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_completion_over_the_batch_limit_terminates_the_run() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "batch";
    start(&endpoint, name).await?;
    let task = poll(&endpoint, name).await?;
    let started_id = history(&endpoint, name)
        .await?
        .last()
        .context("events")?
        .event_id;
    // Under the 2 MiB buffered event limit, so it is flushed by the completion.
    signal(&endpoint, name, "buffered", payloads(19 * MIB / 10))
        .await
        .context("buffer a signal")?;
    let commands = (0..2)
        .map(|index| schedule_activity(index, payloads(6 * MIB / 5), false))
        .collect();
    let status = complete(&endpoint, task.task_token, commands)
        .await
        .err()
        .context("the completion is refused")?;
    ensure!(status.code() == Code::InvalidArgument, "{status:?}");
    let message = status.message().to_owned();
    let size: usize = message
        .strip_prefix("transaction size of ")
        .and_then(|rest| rest.strip_suffix(" bytes exceeds limit of 4194304 bytes"))
        .context("v1.31.0's message")?
        .parse()?;
    ensure!(size > TRANSACTION_SIZE_LIMIT, "size {size}");

    let events = history(&endpoint, name).await?;
    let added = tail(&events, started_id);
    let types: Vec<EventType> = added.iter().map(|event| event_type(event)).collect();
    ensure!(
        types
            == [
                EventType::WorkflowTaskFailed,
                EventType::WorkflowExecutionSignaled,
                EventType::WorkflowExecutionTerminated
            ],
        "added {types:?}"
    );
    check_force_close(added[0])?;
    let details =
        check_termination(added[2], "Transaction size exceeds limit.")?.context("details")?;
    ensure!(details.payloads.len() == 1, "details {details:?}");
    ensure!(
        details.payloads[0].data == serde_json::to_vec(&message)?,
        "details {:?}",
        String::from_utf8_lossy(&details.payloads[0].data)
    );
    ensure!(
        details.payloads[0]
            .metadata
            .get("encoding")
            .map(Vec::as_slice)
            == Some(b"json/plain".as_slice()),
        "details metadata {:?}",
        details.payloads[0].metadata
    );
    engine.shutdown().await?;
    Ok(())
}

/// A failure over 4 KiB that the activity retries after is stored as a server
/// failure whose cause is the worker's failure cut to 4 KiB.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_activity_s_failure_is_stored_cut_to_the_limit() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "activity-failure";
    start(&endpoint, name).await?;
    let task = poll(&endpoint, name).await?;
    complete(
        &endpoint,
        task.task_token,
        vec![schedule_activity(0, payloads(8), true)],
    )
    .await
    .context("schedule the activity")?;
    let activity = poll_activity(&endpoint, name).await?;
    let failure = Failure {
        message: "m".repeat(10 * 1024),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            tokeira_proto::failure::ApplicationFailureInfo {
                r#type: "Retryable".to_owned(),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let _: RespondActivityTaskFailedResponse = call(
        &endpoint,
        "RespondActivityTaskFailed",
        RespondActivityTaskFailedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token: activity.task_token,
            failure: Some(failure.clone()),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;

    let described = describe(&endpoint, name).await?;
    let pending = described
        .pending_activities
        .first()
        .context("the activity is pending its retry")?;
    let stored = pending.last_failure.clone().context("last failure")?;
    ensure!(
        stored.message == "Failure exceeds size limit.",
        "{:?}",
        stored.message
    );
    ensure!(
        matches!(
            stored.failure_info,
            Some(FailureInfo::ServerFailureInfo(
                tokeira_proto::failure::ServerFailureInfo {
                    non_retryable: false
                }
            ))
        ),
        "{:?}",
        stored.failure_info
    );
    let cause = stored.cause.context("cause")?;
    ensure!(
        cause.message.len() < failure.message.len()
            && cause.message.len() <= STORED_FAILURE_LIMIT
            && failure.message.starts_with(&cause.message),
        "cause of {} bytes",
        cause.message.len()
    );
    engine.shutdown().await?;
    Ok(())
}
