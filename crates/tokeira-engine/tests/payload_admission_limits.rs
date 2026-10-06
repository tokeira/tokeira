//! Temporal v1.31.0's payload limits on a worker's responses, through the
//! in-process gRPC endpoint on the in-memory store (`payload-admission-limits`,
//! Property 3).
//!
//! Over the 2 MiB blob size limit, a completion result, cancellation details or
//! heartbeat details fail the activity with a non-retryable server failure
//! while the call succeeds, by task token or by id; a failure is replaced by a
//! server failure whose cause is the original truncated, for an activity or a
//! workflow task; last heartbeat details are dropped
//! (`service/frontend/workflow_handler.go:1231-1245, 1406-1435, 1510-1540,
//! 1603-1640, 1705-1735, 1804-1838, 1927-1962, 2012-2040, 2112-2145 @
//! v1.31.0`). Within the limit, nothing changes.

use std::collections::BTreeMap;

use anyhow::{Context as _, Result};
use http::HeaderMap;
use proptest::prelude::*;
use prost::Message as _;
use tokeira_engine::{Engine, InProcessGrpcRequest, TemporalEndpoint};
use tokeira_proto::{
    common::{ActivityType, Payload, Payloads, WorkflowExecution, WorkflowType},
    enums::{CommandType, WorkflowTaskFailedCause},
    failure::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    history::history_event::Attributes as EventAttributes,
    public::temporal::api::command::v1::{
        Command, ScheduleActivityTaskCommandAttributes, command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        GetWorkflowExecutionHistoryRequest, GetWorkflowExecutionHistoryResponse,
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, RecordActivityTaskHeartbeatByIdRequest,
        RecordActivityTaskHeartbeatByIdResponse, RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse, RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse, RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse, RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse, RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse, RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedRequest,
        RespondWorkflowTaskFailedResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
    },
};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
const NAMESPACE: &str = "default";
const IDENTITY: &str = "payload-limits-worker";
const ACTIVITY_ID: &str = "activity";
const BLOB_SIZE_LIMIT: usize = 2 * 1024 * 1024;

async fn call<Req, Resp>(endpoint: &TemporalEndpoint, rpc: &str, request: Req) -> Result<Resp>
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
        .await
        .with_context(|| format!("{rpc} failed"))?;
    Ok(Resp::decode(response.proto.as_slice())?)
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

fn payloads(data_len: usize) -> Payloads {
    Payloads {
        payloads: vec![payload(data_len)],
    }
}

/// The `Payloads` message nearest `target` bytes on the side of the limit
/// that `over` names: the smallest encoding of at least `target` when over,
/// the largest of at most `target` otherwise. Length prefixes grow at varint
/// boundaries, so not every size has an encoding.
fn payloads_near(target: usize, over: bool) -> Payloads {
    let overhead = payloads(0).encoded_len();
    let mut data_len = target.saturating_sub(overhead);
    while payloads(data_len).encoded_len() > target {
        data_len -= 1;
    }
    if over && payloads(data_len).encoded_len() < target {
        while payloads(data_len).encoded_len() < target {
            data_len += 1;
        }
    }
    payloads(data_len)
}

/// A non-retryable application failure whose message makes the `Failure`
/// encode near `target` bytes, on the `over` side of it.
fn failure_near(target: usize, over: bool) -> Failure {
    let base = |len: usize| Failure {
        message: "x".repeat(len),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "Oversized".to_owned(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let overhead = base(0).encoded_len();
    let mut len = target.saturating_sub(overhead);
    while base(len).encoded_len() > target {
        len -= 1;
    }
    if over {
        while base(len).encoded_len() < target {
            len += 1;
        }
    }
    base(len)
}

fn small_failure() -> Failure {
    Failure {
        message: "ordinary".to_owned(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "Ordinary".to_owned(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn is_server_failure(failure: &Failure, message: &str) -> bool {
    failure.message == message
        && matches!(
            failure.failure_info,
            Some(FailureInfo::ServerFailureInfo(ref info)) if info.non_retryable
        )
}

/// Start a workflow and poll its first workflow task; returns the task token.
async fn start_workflow_task(endpoint: &TemporalEndpoint, name: &str) -> Result<Vec<u8>> {
    let queue = format!("{name}-queue");
    let _: StartWorkflowExecutionResponse = call(
        endpoint,
        "StartWorkflowExecution",
        StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: name.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "payload-limits".to_owned(),
            }),
            task_queue: task_queue(&queue),
            request_id: format!("start-{name}"),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    let workflow_task: PollWorkflowTaskQueueResponse = call(
        endpoint,
        "PollWorkflowTaskQueue",
        PollWorkflowTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(&queue),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    anyhow::ensure!(
        !workflow_task.task_token.is_empty(),
        "workflow task pollable"
    );
    Ok(workflow_task.task_token)
}

/// Start a workflow, schedule one activity, and poll it; returns the task token.
async fn start_activity(endpoint: &TemporalEndpoint, name: &str) -> Result<Vec<u8>> {
    let queue = format!("{name}-queue");
    let workflow_task = start_workflow_task(endpoint, name).await?;
    let _: RespondWorkflowTaskCompletedResponse = call(
        endpoint,
        "RespondWorkflowTaskCompleted",
        RespondWorkflowTaskCompletedRequest {
            task_token: workflow_task,
            identity: IDENTITY.to_owned(),
            commands: vec![Command {
                command_type: CommandType::ScheduleActivityTask as i32,
                user_metadata: None,
                attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                    ScheduleActivityTaskCommandAttributes {
                        activity_id: ACTIVITY_ID.to_owned(),
                        activity_type: Some(ActivityType {
                            name: "work".to_owned(),
                        }),
                        task_queue: task_queue(&queue),
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: 60,
                            nanos: 0,
                        }),
                        ..Default::default()
                    },
                )),
            }],
            ..Default::default()
        },
    )
    .await?;
    let activity_task: PollActivityTaskQueueResponse = call(
        endpoint,
        "PollActivityTaskQueue",
        PollActivityTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(&queue),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    anyhow::ensure!(
        !activity_task.task_token.is_empty(),
        "activity task pollable"
    );
    Ok(activity_task.task_token)
}

/// The first failure `pick` finds in the run's history events.
async fn recorded_failure(
    endpoint: &TemporalEndpoint,
    name: &str,
    pick: impl Fn(EventAttributes) -> Option<Failure>,
) -> Result<Option<Failure>> {
    let response: GetWorkflowExecutionHistoryResponse = call(
        endpoint,
        "GetWorkflowExecutionHistory",
        GetWorkflowExecutionHistoryRequest {
            namespace: NAMESPACE.to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: name.to_owned(),
                run_id: String::new(),
            }),
            ..Default::default()
        },
    )
    .await?;
    Ok(response
        .history
        .unwrap_or_default()
        .events
        .into_iter()
        .find_map(|event| event.attributes.and_then(&pick)))
}

/// The failure of the run's ActivityTaskFailed event, if it has one.
async fn recorded_activity_failure(
    endpoint: &TemporalEndpoint,
    name: &str,
) -> Result<Option<Failure>> {
    recorded_failure(endpoint, name, |attributes| match attributes {
        EventAttributes::ActivityTaskFailedEventAttributes(attributes) => attributes.failure,
        _ => None,
    })
    .await
}

/// The failure of the run's WorkflowTaskFailed event, if it has one.
async fn recorded_workflow_task_failure(
    endpoint: &TemporalEndpoint,
    name: &str,
) -> Result<Option<Failure>> {
    recorded_failure(endpoint, name, |attributes| match attributes {
        EventAttributes::WorkflowTaskFailedEventAttributes(attributes) => attributes.failure,
        _ => None,
    })
    .await
}

/// RespondActivityTaskCompleted, by task token or by the activity's ids.
async fn complete(
    endpoint: &TemporalEndpoint,
    name: &str,
    token: Vec<u8>,
    by_id: bool,
    result: Payloads,
) -> Result<()> {
    if by_id {
        let _: RespondActivityTaskCompletedByIdResponse = call(
            endpoint,
            "RespondActivityTaskCompletedById",
            RespondActivityTaskCompletedByIdRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_id: name.to_owned(),
                activity_id: ACTIVITY_ID.to_owned(),
                result: Some(result),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
    } else {
        let _: RespondActivityTaskCompletedResponse = call(
            endpoint,
            "RespondActivityTaskCompleted",
            RespondActivityTaskCompletedRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: token,
                result: Some(result),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

/// RecordActivityTaskHeartbeat, by task token or by the activity's ids;
/// returns `cancel_requested`.
async fn heartbeat(
    endpoint: &TemporalEndpoint,
    name: &str,
    token: Vec<u8>,
    by_id: bool,
    details: Payloads,
) -> Result<bool> {
    Ok(if by_id {
        let answer: RecordActivityTaskHeartbeatByIdResponse = call(
            endpoint,
            "RecordActivityTaskHeartbeatById",
            RecordActivityTaskHeartbeatByIdRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_id: name.to_owned(),
                activity_id: ACTIVITY_ID.to_owned(),
                details: Some(details),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        answer.cancel_requested
    } else {
        let answer: RecordActivityTaskHeartbeatResponse = call(
            endpoint,
            "RecordActivityTaskHeartbeat",
            RecordActivityTaskHeartbeatRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: token,
                details: Some(details),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        answer.cancel_requested
    })
}

/// RespondActivityTaskFailed, by task token or by the activity's ids; returns
/// the response's `failures`.
async fn fail(
    endpoint: &TemporalEndpoint,
    name: &str,
    token: Vec<u8>,
    by_id: bool,
    failure: Failure,
    last_heartbeat_details: Option<Payloads>,
) -> Result<Vec<Failure>> {
    Ok(if by_id {
        let answer: RespondActivityTaskFailedByIdResponse = call(
            endpoint,
            "RespondActivityTaskFailedById",
            RespondActivityTaskFailedByIdRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_id: name.to_owned(),
                activity_id: ACTIVITY_ID.to_owned(),
                failure: Some(failure),
                last_heartbeat_details,
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        answer.failures
    } else {
        let answer: RespondActivityTaskFailedResponse = call(
            endpoint,
            "RespondActivityTaskFailed",
            RespondActivityTaskFailedRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: token,
                failure: Some(failure),
                last_heartbeat_details,
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        answer.failures
    })
}

/// RespondActivityTaskCanceled, by task token or by the activity's ids.
async fn cancel(
    endpoint: &TemporalEndpoint,
    name: &str,
    token: Vec<u8>,
    by_id: bool,
    details: Payloads,
) -> Result<()> {
    if by_id {
        let _: RespondActivityTaskCanceledByIdResponse = call(
            endpoint,
            "RespondActivityTaskCanceledById",
            RespondActivityTaskCanceledByIdRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_id: name.to_owned(),
                activity_id: ACTIVITY_ID.to_owned(),
                details: Some(details),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
    } else {
        let _: RespondActivityTaskCanceledResponse = call(
            endpoint,
            "RespondActivityTaskCanceled",
            RespondActivityTaskCanceledRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: token,
                details: Some(details),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

/// One activity response just under or just over the blob size limit.
#[derive(Clone, Copy, Debug)]
enum Response {
    Completed,
    Heartbeat,
    Failure,
    LastHeartbeatDetails,
}

async fn check_response(
    engine: &Engine,
    response: Response,
    by_id: bool,
    over: bool,
    offset: usize,
) -> Result<()> {
    let endpoint = engine.endpoint();
    let name = format!("payload-limits-{response:?}-{by_id}-{over}-{offset}").to_lowercase();
    let token = start_activity(&endpoint, &name).await?;
    let target = if over {
        BLOB_SIZE_LIMIT + offset
    } else {
        BLOB_SIZE_LIMIT - offset
    };
    match response {
        Response::Completed => {
            let result = payloads_near(target, over);
            assert_eq!(result.encoded_len() > BLOB_SIZE_LIMIT, over);
            complete(&endpoint, &name, token, by_id, result).await?;
            let failure = recorded_activity_failure(&endpoint, &name).await?;
            assert_eq!(
                failure.is_some_and(|failure| is_server_failure(
                    &failure,
                    "Complete result exceeds size limit."
                )),
                over
            );
        }
        Response::Heartbeat => {
            let details = payloads_near(target, over);
            assert_eq!(details.encoded_len() > BLOB_SIZE_LIMIT, over);
            let cancel_requested = heartbeat(&endpoint, &name, token, by_id, details).await?;
            assert_eq!(cancel_requested, over);
            let failure = recorded_activity_failure(&endpoint, &name).await?;
            assert_eq!(
                failure.is_some_and(|failure| is_server_failure(
                    &failure,
                    "Heartbeat details exceed size limit."
                )),
                over
            );
        }
        Response::Failure => {
            let original = failure_near(target, over);
            assert_eq!(original.encoded_len() > BLOB_SIZE_LIMIT, over);
            let failures = fail(&endpoint, &name, token, by_id, original.clone(), None).await?;
            let recorded = recorded_activity_failure(&endpoint, &name)
                .await?
                .context("a non-retryable failure is recorded")?;
            if over {
                assert!(is_server_failure(&recorded, "Failure exceeds size limit."));
                assert_eq!(failures, vec![recorded.clone()]);
                let cause = recorded.cause.context("the original is the cause")?;
                assert_eq!(cause.message.len(), cause.message.chars().count());
                assert!(cause.encoded_len() <= 512 * 1024);
                assert!(original.message.starts_with(&cause.message));
            } else {
                assert_eq!(recorded.message, original.message);
                assert!(failures.is_empty());
            }
        }
        Response::LastHeartbeatDetails => {
            let details = payloads_near(target, over);
            assert_eq!(details.encoded_len() > BLOB_SIZE_LIMIT, over);
            let failures = fail(
                &endpoint,
                &name,
                token,
                by_id,
                small_failure(),
                Some(details),
            )
            .await?;
            let recorded = recorded_activity_failure(&endpoint, &name)
                .await?
                .context("the failure is recorded")?;
            assert_eq!(recorded.message, "ordinary");
            assert_eq!(
                failures.len() == 1
                    && is_server_failure(&failures[0], "Heartbeat details exceed size limit."),
                over
            );
        }
    }
    Ok(())
}

const RESPONSES: [Response; 4] = [
    Response::Completed,
    Response::Heartbeat,
    Response::Failure,
    Response::LastHeartbeatDetails,
];

// Feature: payload-admission-limits, Property 3: Oversized worker responses become failures
proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]

    /// Every response, by task token and by id, just under and just over the
    /// limit, at a generated distance from it.
    #[test]
    fn property_oversized_worker_responses_become_failures(offset in 1usize..64) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        runtime.block_on(async {
            let engine = Engine::start().await.expect("engine");
            for response in RESPONSES {
                for by_id in [false, true] {
                    for over in [false, true] {
                        check_response(&engine, response, by_id, over, offset)
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{response:?} by_id={by_id} over={over}: {error:#}")
                            });
                    }
                }
            }
            engine.shutdown().await.expect("engine shutdown");
        });
    }
}

/// Cancellation details over the limit fail the activity too, by task token
/// or by id.
#[tokio::test]
async fn oversized_cancellation_details_fail_the_activity() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    for by_id in [false, true] {
        let name = format!("payload-limits-cancel-{by_id}");
        let token = start_activity(&endpoint, &name).await?;
        let details = payloads_near(BLOB_SIZE_LIMIT + 1, true);
        cancel(&endpoint, &name, token, by_id, details).await?;
        let failure = recorded_activity_failure(&endpoint, &name)
            .await?
            .context("the activity is failed")?;
        assert!(is_server_failure(
            &failure,
            "Cancel details exceed size limit."
        ));
    }
    engine.shutdown().await?;
    Ok(())
}

/// A workflow task's failure over the limit is replaced as an activity's is;
/// within the limit it is recorded as sent.
#[tokio::test]
async fn an_oversized_workflow_task_failure_is_replaced() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    for over in [false, true] {
        let name = format!("payload-limits-workflow-task-{over}");
        let token = start_workflow_task(&endpoint, &name).await?;
        let target = if over {
            BLOB_SIZE_LIMIT + 1
        } else {
            BLOB_SIZE_LIMIT
        };
        let original = failure_near(target, over);
        assert_eq!(original.encoded_len() > BLOB_SIZE_LIMIT, over);
        let _: RespondWorkflowTaskFailedResponse = call(
            &endpoint,
            "RespondWorkflowTaskFailed",
            RespondWorkflowTaskFailedRequest {
                namespace: NAMESPACE.to_owned(),
                task_token: token,
                cause: WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure as i32,
                failure: Some(original.clone()),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            },
        )
        .await?;
        let recorded = recorded_workflow_task_failure(&endpoint, &name)
            .await?
            .context("the workflow task failure is recorded")?;
        if over {
            assert!(is_server_failure(&recorded, "Failure exceeds size limit."));
            let cause = recorded.cause.context("the original is the cause")?;
            assert!(cause.encoded_len() <= 512 * 1024);
            assert!(original.message.starts_with(&cause.message));
        } else {
            assert_eq!(recorded.message, original.message);
        }
    }
    engine.shutdown().await?;
    Ok(())
}
