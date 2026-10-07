//! Temporal v1.31.0's limits on a run's signals and updates, through the
//! in-process gRPC endpoint on the in-memory store, at v1.31.0's values
//! (`signal-update-limits`).
//!
//! A run that has recorded 10,000 signals refuses another, from a client, from
//! SignalWithStart or from another workflow, with `InvalidArgument`
//! (`service/history/api/signal_workflow_util.go:53-61`;
//! `service/history/consts/const.go:60-61 @ v1.31.0`), and the workflow that
//! sent the last records the failure with `SIGNAL_COUNT_LIMIT_EXCEEDED`
//! (`service/history/transfer_queue_active_task_executor.go:683-736 @
//! v1.31.0`). A run refuses an update while ten are in flight, or when the
//! requests it holds would reach 20 MiB with the new one's, with
//! `ResourceExhausted`, cause `CONCURRENT_LIMIT` and scope `NAMESPACE`
//! (`service/history/workflow/update/registry.go:398-436 @ v1.31.0`), and an
//! update-with-start carries that refusal as its update's error
//! (`tests/update_workflow_test.go:5806-5858 @ v1.31.0`).

use anyhow::{Context as _, Result, bail, ensure};
use http::HeaderMap;
use prost::Message as _;
use tokeira_engine::{Engine, InProcessGrpcRequest, TemporalEndpoint};
use tokeira_proto::{
    common::{Payload, Payloads, WorkflowExecution, WorkflowType},
    enums::{
        CommandType, ResourceExhaustedCause, ResourceExhaustedScope,
        SignalExternalWorkflowExecutionFailedCause, WorkflowIdConflictPolicy,
    },
    history::history_event::Attributes as EventAttributes,
    public::temporal::api::{
        command::v1::{
            Command, SignalExternalWorkflowExecutionCommandAttributes, command::Attributes,
        },
        errordetails::v1::{MultiOperationExecutionFailure, ResourceExhaustedFailure},
        update::v1::{
            Input as UpdateInput, Meta as UpdateMeta, Request as UpdateRequest, WaitPolicy,
        },
    },
    taskqueue::TaskQueue,
    workflowservice::{
        ExecuteMultiOperationRequest, ExecuteMultiOperationResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse, SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, UpdateWorkflowExecutionRequest,
        UpdateWorkflowExecutionResponse,
        execute_multi_operation_request::{Operation, operation::Operation as MultiOperation},
    },
};
use tokio::{sync::mpsc, task::JoinHandle};
use tonic::{Code, Status};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
const NAMESPACE: &str = "default";
const IDENTITY: &str = "signal-update-limits";
const SIGNAL_LIMIT: usize = 10_000;
const SIGNAL_LIMIT_MESSAGE: &str = "exceeded workflow execution limit for signal events";
const IN_FLIGHT_LIMIT: usize = 10;
const IN_FLIGHT_MESSAGE: &str =
    "limit on number of concurrent in-flight updates has been reached (10)";
const IN_FLIGHT_PAYLOADS_LIMIT: usize = 20 * 1024 * 1024;
const IN_FLIGHT_PAYLOADS_MESSAGE: &str =
    "limit on total payload size of in-flight updates has been reached (20971520 bytes)";
/// Six requests with arguments this size reach the in-flight payload limit and
/// five don't; each fits in gRPC's 4 MiB request.
const LARGE_UPDATE_ARGS: usize = 4_000_000;

/// `google.rpc.Status`, which the status details carry.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

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

fn workflow_type() -> Option<WorkflowType> {
    Some(WorkflowType {
        name: "signal-update-limits".to_owned(),
    })
}

fn execution(name: &str) -> Option<WorkflowExecution> {
    Some(WorkflowExecution {
        workflow_id: name.to_owned(),
        run_id: String::new(),
    })
}

fn payloads(size: usize) -> Payloads {
    Payloads {
        payloads: vec![Payload {
            data: vec![b'x'; size],
            ..Default::default()
        }],
    }
}

async fn start(endpoint: &TemporalEndpoint, name: &str) -> Result<()> {
    let _: StartWorkflowExecutionResponse = call(
        endpoint,
        "StartWorkflowExecution",
        StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: name.to_owned(),
            workflow_type: workflow_type(),
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

async fn signal(
    endpoint: &TemporalEndpoint,
    name: &str,
    request_id: &str,
) -> std::result::Result<SignalWorkflowExecutionResponse, Status> {
    try_call(
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
    .await
}

async fn signal_with_start(
    endpoint: &TemporalEndpoint,
    name: &str,
    request_id: &str,
) -> std::result::Result<SignalWithStartWorkflowExecutionResponse, Status> {
    try_call(
        endpoint,
        "SignalWithStartWorkflowExecution",
        SignalWithStartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: name.to_owned(),
            workflow_type: workflow_type(),
            task_queue: task_queue(name),
            signal_name: "signal".to_owned(),
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
) -> Result<()> {
    let _: RespondWorkflowTaskCompletedResponse = call(
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
    .await?;
    Ok(())
}

/// The command an SDK sends to signal `target`, naming the sender's own
/// namespace as SDKs do.
#[allow(deprecated)]
fn signal_external(target: &str) -> Command {
    Command {
        command_type: CommandType::SignalExternalWorkflowExecution as i32,
        user_metadata: None,
        attributes: Some(
            Attributes::SignalExternalWorkflowExecutionCommandAttributes(
                SignalExternalWorkflowExecutionCommandAttributes {
                    namespace: NAMESPACE.to_owned(),
                    execution: execution(target),
                    signal_name: "signal".to_owned(),
                    ..Default::default()
                },
            ),
        ),
    }
}

fn update_request(name: &str, update_id: &str, args: usize) -> UpdateWorkflowExecutionRequest {
    UpdateWorkflowExecutionRequest {
        namespace: NAMESPACE.to_owned(),
        workflow_execution: execution(name),
        request: Some(UpdateRequest {
            meta: Some(UpdateMeta {
                update_id: update_id.to_owned(),
                identity: IDENTITY.to_owned(),
            }),
            input: Some(UpdateInput {
                header: None,
                name: "update".to_owned(),
                args: Some(payloads(args)),
            }),
        }),
        wait_policy: Some(WaitPolicy { lifecycle_stage: 3 }),
        ..Default::default()
    }
}

/// Sends `count` updates to `name` at once, each with `args` bytes of
/// arguments, and returns the first answer, which must be a refusal, with the
/// calls of the others still waiting on the idle worker.
async fn send_updates(
    endpoint: &TemporalEndpoint,
    name: &str,
    count: usize,
    args: usize,
) -> Result<(Status, Vec<JoinHandle<()>>)> {
    let (answers, mut answered) = mpsc::unbounded_channel();
    let calls = (0..count)
        .map(|index| {
            let endpoint = endpoint.clone();
            let answers = answers.clone();
            let request = update_request(name, &format!("update-{index}"), args);
            tokio::spawn(async move {
                let answer: std::result::Result<UpdateWorkflowExecutionResponse, Status> =
                    try_call(&endpoint, "UpdateWorkflowExecution", request).await;
                // The test stops listening after the first answer.
                drop(answers.send(answer));
            })
        })
        .collect();
    drop(answers);
    match answered.recv().await.context("an update answered")? {
        Ok(response) => bail!("an update was answered before any was refused: {response:?}"),
        Err(status) => Ok((status, calls)),
    }
}

fn check_concurrent_limit_detail(details: &[prost_types::Any]) -> Result<()> {
    let [detail] = details else {
        bail!("expected one detail, got {details:?}");
    };
    ensure!(
        detail.type_url
            == "type.googleapis.com/temporal.api.errordetails.v1.ResourceExhaustedFailure",
        "detail {}",
        detail.type_url
    );
    let failure = ResourceExhaustedFailure::decode(detail.value.as_slice())?;
    ensure!(
        failure.cause == ResourceExhaustedCause::ConcurrentLimit as i32
            && failure.scope == ResourceExhaustedScope::Namespace as i32,
        "detail {failure:?}"
    );
    Ok(())
}

fn expect_concurrent_limit(status: &Status, message: &str) -> Result<()> {
    ensure!(
        status.code() == Code::ResourceExhausted && status.message() == message,
        "expected ResourceExhausted {message:?}, got {status:?}"
    );
    check_concurrent_limit_detail(&RpcStatus::decode(status.details())?.details)
}

fn expect_signal_limit<T: std::fmt::Debug>(result: std::result::Result<T, Status>) -> Result<()> {
    let status = result.err().context("the signal is refused")?;
    ensure!(
        status.code() == Code::InvalidArgument && status.message() == SIGNAL_LIMIT_MESSAGE,
        "expected InvalidArgument {SIGNAL_LIMIT_MESSAGE:?}, got {status:?}"
    );
    Ok(())
}

/// A run that has recorded 10,000 signals refuses the next from a client and
/// from SignalWithStart, answers a repeat of a signal it applied as a
/// duplicate, and fails the signal of another workflow on its sender.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_at_the_signal_limit_refuses_every_new_signal() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let target = "signal-limit";
    start(&endpoint, target).await?;
    for index in 0..SIGNAL_LIMIT {
        signal(&endpoint, target, &format!("signal-{index}"))
            .await
            .with_context(|| format!("signal {index} under the limit"))?;
    }

    expect_signal_limit(signal(&endpoint, target, "signal-over").await)?;
    signal(&endpoint, target, "signal-0")
        .await
        .context("a repeated signal is answered as a duplicate")?;
    expect_signal_limit(signal_with_start(&endpoint, target, "signal-with-start").await)?;
    expect_signal_limit(signal_with_start(&endpoint, target, "signal-0").await)?;

    let sender = "signal-limit-sender";
    start(&endpoint, sender).await?;
    let task = poll(&endpoint, sender).await?;
    complete(&endpoint, task.task_token, vec![signal_external(target)]).await?;
    // The failure schedules the sender's next workflow task.
    let task = poll(&endpoint, sender).await?;
    let events = task.history.context("the task's history")?.events;
    let failed = events
        .iter()
        .find_map(|event| match &event.attributes {
            Some(EventAttributes::SignalExternalWorkflowExecutionFailedEventAttributes(failed)) => {
                Some(failed)
            }
            _ => None,
        })
        .context("the sender records the failure")?;
    ensure!(
        failed.cause == SignalExternalWorkflowExecutionFailedCause::SignalCountLimitExceeded as i32,
        "cause {:?}",
        failed.cause
    );
    engine.shutdown().await?;
    Ok(())
}

/// A run whose idle worker leaves ten updates admitted refuses the eleventh,
/// and the worker's next task carries the ten.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_with_ten_updates_in_flight_refuses_the_eleventh() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "in-flight-limit";
    start(&endpoint, name).await?;
    let (refusal, calls) = send_updates(&endpoint, name, IN_FLIGHT_LIMIT + 1, 16).await?;
    expect_concurrent_limit(&refusal, IN_FLIGHT_MESSAGE)?;
    let task = poll(&endpoint, name).await?;
    ensure!(
        task.messages.len() == IN_FLIGHT_LIMIT,
        "the task carries {} updates",
        task.messages.len()
    );
    calls.iter().for_each(JoinHandle::abort);
    engine.shutdown().await?;
    Ok(())
}

/// A run holding five update requests of nearly 4 MiB refuses a sixth, since
/// the six would reach 20 MiB.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_holding_nearly_20_mib_of_update_requests_refuses_the_next() -> Result<()> {
    let request_size = update_request("in-flight-payloads", "update-0", LARGE_UPDATE_ARGS)
        .request
        .context("the update's request")?
        .encoded_len();
    ensure!(
        5 * request_size < IN_FLIGHT_PAYLOADS_LIMIT && 6 * request_size >= IN_FLIGHT_PAYLOADS_LIMIT,
        "requests of {request_size} bytes"
    );
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "in-flight-payloads";
    start(&endpoint, name).await?;
    let (refusal, calls) = send_updates(&endpoint, name, 6, LARGE_UPDATE_ARGS).await?;
    expect_concurrent_limit(&refusal, IN_FLIGHT_PAYLOADS_MESSAGE)?;
    calls.iter().for_each(JoinHandle::abort);
    engine.shutdown().await?;
    Ok(())
}

/// An update-with-start to a running run with ten updates in flight is refused
/// with the update's error, the start's being aborted.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_with_start_at_the_in_flight_limit_carries_the_updates_refusal() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let name = "update-with-start-limit";
    start(&endpoint, name).await?;
    let (_, calls) = send_updates(&endpoint, name, IN_FLIGHT_LIMIT + 1, 16).await?;

    let status = try_call::<_, ExecuteMultiOperationResponse>(
        &endpoint,
        "ExecuteMultiOperation",
        ExecuteMultiOperationRequest {
            namespace: NAMESPACE.to_owned(),
            operations: vec![
                Operation {
                    operation: Some(MultiOperation::StartWorkflow(
                        StartWorkflowExecutionRequest {
                            namespace: NAMESPACE.to_owned(),
                            workflow_id: name.to_owned(),
                            workflow_type: workflow_type(),
                            task_queue: task_queue(name),
                            request_id: "update-with-start".to_owned(),
                            identity: IDENTITY.to_owned(),
                            workflow_id_conflict_policy: WorkflowIdConflictPolicy::UseExisting
                                as i32,
                            ..Default::default()
                        },
                    )),
                },
                Operation {
                    operation: Some(MultiOperation::UpdateWorkflow(update_request(
                        name,
                        "update-with-start",
                        16,
                    ))),
                },
            ],
            ..Default::default()
        },
    )
    .await
    .err()
    .context("the update-with-start is refused")?;
    ensure!(
        status.code() == Code::ResourceExhausted
            && status.message() == "Update-with-Start could not be executed.",
        "status {status:?}"
    );
    let details = RpcStatus::decode(status.details())?.details;
    let [detail] = details.as_slice() else {
        bail!("expected one detail, got {details:?}");
    };
    ensure!(
        detail.type_url
            == "type.googleapis.com/temporal.api.errordetails.v1.MultiOperationExecutionFailure",
        "detail {}",
        detail.type_url
    );
    let failure = MultiOperationExecutionFailure::decode(detail.value.as_slice())?;
    let [start_status, update_status] = failure.statuses.as_slice() else {
        bail!(
            "expected two operation statuses, got {:?}",
            failure.statuses
        );
    };
    ensure!(
        start_status.code == Code::Aborted as i32
            && start_status.message == "Operation was aborted.",
        "start {start_status:?}"
    );
    ensure!(
        update_status.code == Code::ResourceExhausted as i32
            && update_status.message == IN_FLIGHT_MESSAGE,
        "update {update_status:?}"
    );
    check_concurrent_limit_detail(&update_status.details)?;
    calls.iter().for_each(JoinHandle::abort);
    engine.shutdown().await?;
    Ok(())
}
