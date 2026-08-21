//! Durable activity retry delivery across a worker replacement, and the
//! embedded transport's execution-context contract that makes it hold.
//!
//! Reproduces the embedded-consumer liveness report: attempt 1 of a retryable
//! activity fails on worker A, worker A shuts down (its private runtime dies)
//! while the engine stays alive, and a replacement worker B polling the same
//! task queue must receive the durably-prepared attempt 2 after its backoff.
//! v1.31.0 delivers that retry through a durable ActivityRetryTimerTask whose
//! executor re-adds the task to matching's durable backlog
//! (`timer_queue_active_task_executor.go:522-620 @ v1.31.0`), so a vanished
//! worker can never strand it. The engine-side equivalent obligation is the
//! backoff-delayed publish, which must therefore live on the engine-host
//! runtime — never on the executor of whichever consumer drove the RPC.

use std::time::Duration;

use anyhow::{Context as _, Result};
use http::HeaderMap;
use tokeira_engine::{Engine, InProcessGrpcRequest, TemporalEndpoint, TokeiradHandle};
use tokeira_proto::{
    common::{ActivityType, RetryPolicy, WorkflowExecution, WorkflowType},
    enums::{CommandType, PendingActivityState},
    failure::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    public::temporal::api::command::v1::{
        Command, ScheduleActivityTaskCommandAttributes, command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse,
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, ShutdownWorkerRequest, ShutdownWorkerResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
        workflow_service_client::WorkflowServiceClient,
    },
};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
const NAMESPACE: &str = "default";
/// Both workers report the same identity: the reproducing consumer runs both
/// SDK workers in one process, where the SDK default identity (`pid@host`) is
/// identical for the original and the replacement.
const WORKER_IDENTITY: &str = "shared-worker-identity";

fn proto_duration(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

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

fn start_request(workflow_id: &str, queue: &str) -> StartWorkflowExecutionRequest {
    StartWorkflowExecutionRequest {
        namespace: NAMESPACE.to_owned(),
        workflow_id: workflow_id.to_owned(),
        workflow_type: Some(WorkflowType {
            name: "retry-replacement-workflow".to_owned(),
        }),
        task_queue: task_queue(queue),
        request_id: format!("start-{workflow_id}"),
        identity: WORKER_IDENTITY.to_owned(),
        ..Default::default()
    }
}

fn workflow_poll_request(queue: &str) -> PollWorkflowTaskQueueRequest {
    PollWorkflowTaskQueueRequest {
        namespace: NAMESPACE.to_owned(),
        task_queue: task_queue(queue),
        identity: WORKER_IDENTITY.to_owned(),
        ..Default::default()
    }
}

fn activity_poll_request(queue: &str) -> PollActivityTaskQueueRequest {
    PollActivityTaskQueueRequest {
        namespace: NAMESPACE.to_owned(),
        task_queue: task_queue(queue),
        identity: WORKER_IDENTITY.to_owned(),
        ..Default::default()
    }
}

/// The reproducing consumer's activity options: retryable with a 1s initial
/// backoff, only a start-to-close timeout, and a short heartbeat.
fn schedule_activity_completion(
    task_token: Vec<u8>,
    queue: &str,
) -> RespondWorkflowTaskCompletedRequest {
    RespondWorkflowTaskCompletedRequest {
        task_token,
        identity: WORKER_IDENTITY.to_owned(),
        commands: vec![Command {
            command_type: CommandType::ScheduleActivityTask as i32,
            user_metadata: None,
            attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                ScheduleActivityTaskCommandAttributes {
                    activity_id: "turn-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: "turn".to_owned(),
                    }),
                    task_queue: task_queue(queue),
                    start_to_close_timeout: Some(proto_duration(Duration::from_secs(15))),
                    heartbeat_timeout: Some(proto_duration(Duration::from_millis(500))),
                    retry_policy: Some(RetryPolicy {
                        initial_interval: Some(proto_duration(Duration::from_secs(1))),
                        backoff_coefficient: 2.0,
                        maximum_interval: Some(proto_duration(Duration::from_secs(10))),
                        maximum_attempts: 5,
                        non_retryable_error_types: Vec::new(),
                    }),
                    ..Default::default()
                },
            )),
        }],
        ..Default::default()
    }
}

fn retryable_failure(task_token: Vec<u8>) -> RespondActivityTaskFailedRequest {
    RespondActivityTaskFailedRequest {
        task_token,
        identity: WORKER_IDENTITY.to_owned(),
        failure: Some(Failure {
            message: "HarnessDied".to_owned(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "HarnessDied".to_owned(),
                    non_retryable: false,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn shutdown_request(queue: &str) -> ShutdownWorkerRequest {
    ShutdownWorkerRequest {
        namespace: NAMESPACE.to_owned(),
        identity: WORKER_IDENTITY.to_owned(),
        sticky_task_queue: format!("{queue}-sticky-worker-a"),
        task_queue: queue.to_owned(),
        ..Default::default()
    }
}

async fn pending_activity(
    engine: &Engine,
    workflow_id: &str,
) -> Result<Option<(i32, PendingActivityState)>> {
    let response: DescribeWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "DescribeWorkflowExecution",
        DescribeWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.to_owned(),
                run_id: String::new(),
            }),
        },
    )
    .await?;
    Ok(response.pending_activities.first().map(|info| {
        (
            info.attempt,
            PendingActivityState::try_from(info.state).unwrap_or(PendingActivityState::Unspecified),
        )
    }))
}

/// Drive: start workflow → first WFT schedules the retryable activity → poll
/// attempt 1. Returns attempt 1's task token.
async fn schedule_and_poll_attempt_one(
    engine: &Engine,
    workflow_id: &str,
    queue: &str,
) -> Result<Vec<u8>> {
    let endpoint = engine.endpoint();
    let _: StartWorkflowExecutionResponse = call(
        &endpoint,
        "StartWorkflowExecution",
        start_request(workflow_id, queue),
    )
    .await?;
    let workflow_task: PollWorkflowTaskQueueResponse = call(
        &endpoint,
        "PollWorkflowTaskQueue",
        workflow_poll_request(queue),
    )
    .await?;
    anyhow::ensure!(
        !workflow_task.task_token.is_empty(),
        "first workflow task must be pollable"
    );
    let _: RespondWorkflowTaskCompletedResponse = call(
        &endpoint,
        "RespondWorkflowTaskCompleted",
        schedule_activity_completion(workflow_task.task_token, queue),
    )
    .await?;
    let attempt_one: PollActivityTaskQueueResponse = call(
        &endpoint,
        "PollActivityTaskQueue",
        activity_poll_request(queue),
    )
    .await?;
    anyhow::ensure!(
        !attempt_one.task_token.is_empty() && attempt_one.attempt == 1,
        "attempt 1 must be pollable by worker A"
    );
    Ok(attempt_one.task_token)
}

/// Worker B: bounded re-poll loop until attempt 2 arrives. The 15s ceiling
/// mirrors the reproducer's failure window and protects the suite, not the
/// ordering — delivery is expected right after the 1s backoff.
async fn poll_until_attempt_two(engine: &Engine, queue: &str) -> Result<()> {
    let mut delivered = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        // A poll parked past the per-round window is dropped (exercising the
        // caller-cancellation path) and counted as an empty round.
        let response: Option<PollActivityTaskQueueResponse> = tokio::time::timeout(
            Duration::from_secs(5),
            call(
                &engine.endpoint(),
                "PollActivityTaskQueue",
                activity_poll_request(queue),
            ),
        )
        .await
        .ok()
        .transpose()?;
        if let Some(response) = response
            && !response.task_token.is_empty()
        {
            delivered = Some(response);
            break;
        }
    }
    let delivered = delivered.context(
        "durable activity attempt 2 remained undelivered: no replacement poll received it",
    )?;
    anyhow::ensure!(
        delivered.attempt == 2,
        "replacement must receive attempt 2, got {}",
        delivered.attempt
    );
    Ok(())
}

/// Baseline: the same consumer context drives the whole sequence. Guards the
/// retry pipeline itself, independent of executor lifetimes.
#[tokio::test]
async fn replacement_worker_receives_durable_activity_retry() -> Result<()> {
    let engine = Engine::start().await?;
    let queue = "retry-replacement-baseline";
    let workflow_id = "retry-replacement-baseline-canary";
    let token = schedule_and_poll_attempt_one(&engine, workflow_id, queue).await?;

    let endpoint = engine.endpoint();
    let _: RespondActivityTaskFailedResponse = call(
        &endpoint,
        "RespondActivityTaskFailed",
        retryable_failure(token),
    )
    .await?;
    let _: ShutdownWorkerResponse =
        call(&endpoint, "ShutdownWorker", shutdown_request(queue)).await?;
    assert_eq!(
        pending_activity(&engine, workflow_id).await?,
        Some((2, PendingActivityState::Scheduled)),
        "durable attempt 2 must be SCHEDULED before worker B"
    );

    poll_until_attempt_two(&engine, queue).await?;
    assert_eq!(
        pending_activity(&engine, workflow_id).await?,
        Some((2, PendingActivityState::Started)),
        "delivered attempt must be durably started"
    );
    engine.shutdown().await
}

/// The reproducer's executor shape: the failing worker drives its RPCs from a
/// private current-thread runtime that dies immediately after the failure
/// report — exactly an embedded SDK worker thread. The engine-host runtime
/// (this test's runtime) stays alive; the durable retry MUST still reach a
/// replacement poller, exactly once.
///
/// Fails without the in-process bridge pinning handler execution to the
/// engine-host runtime: the backoff-delayed publish is spawned from the RPC
/// handler, inherits the dying consumer runtime, and is killed before it
/// fires, stranding the SCHEDULED attempt forever.
#[tokio::test(flavor = "multi_thread")]
async fn retry_committed_from_dying_consumer_runtime_still_delivers() -> Result<()> {
    let engine = Engine::start().await?;
    let queue = "retry-replacement-dying-consumer";
    let workflow_id = "retry-replacement-dying-consumer-canary";
    let token = schedule_and_poll_attempt_one(&engine, workflow_id, queue).await?;

    // Worker A's dying executor: report the failure and the worker shutdown
    // from a separate thread's own current-thread runtime, then drop that
    // runtime. Any engine continuation mistakenly spawned onto it dies here.
    let endpoint = engine.endpoint();
    let queue_name = queue.to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let consumer_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build the consumer runtime")?;
        consumer_runtime.block_on(async move {
            let _: RespondActivityTaskFailedResponse = call(
                &endpoint,
                "RespondActivityTaskFailed",
                retryable_failure(token),
            )
            .await?;
            let _: ShutdownWorkerResponse =
                call(&endpoint, "ShutdownWorker", shutdown_request(&queue_name)).await?;
            Ok::<(), anyhow::Error>(())
        })?;
        drop(consumer_runtime);
        Ok(())
    })
    .await
    .context("join the consumer thread")??;

    // Authoritative state holds the durable attempt 2 before worker B exists.
    assert_eq!(
        pending_activity(&engine, workflow_id).await?,
        Some((2, PendingActivityState::Scheduled)),
        "durable attempt 2 must be SCHEDULED before worker B"
    );

    poll_until_attempt_two(&engine, queue).await?;

    // Exactly once: the attempt is durably started, and no second copy is
    // deliverable — a further poll must come up empty within a bounded window.
    assert_eq!(
        pending_activity(&engine, workflow_id).await?,
        Some((2, PendingActivityState::Started)),
        "delivered attempt must be durably started"
    );
    let second = tokio::time::timeout(
        Duration::from_secs(2),
        call::<_, PollActivityTaskQueueResponse>(
            &engine.endpoint(),
            "PollActivityTaskQueue",
            activity_poll_request(queue),
        ),
    )
    .await;
    match second {
        Err(_elapsed) => {}
        Ok(response) => anyhow::ensure!(
            response?.task_token.is_empty(),
            "attempt 2 must not be delivered twice"
        ),
    }
    engine.shutdown().await
}

/// Cancellation contract: dropping a caller's poll future must abort the
/// spawned handler and release its matching state. The decisive signal:
/// abandoned polls left running as zombies would consume the later retry
/// publish and starve the legitimate replacement poller.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_long_polls_abort_handlers_and_release_matching() -> Result<()> {
    let engine = Engine::start().await?;
    let queue = "retry-replacement-cancelled-polls";
    let workflow_id = "retry-replacement-cancelled-polls-canary";

    // Park a fleet of activity polls on the queue, then cancel them all by
    // dropping their futures — the embedded shape of a worker aborting its
    // outstanding polls at shutdown.
    let mut parked = Vec::new();
    for _ in 0..8 {
        let endpoint = engine.endpoint();
        let queue_name = queue.to_owned();
        parked.push(tokio::spawn(async move {
            let _: Result<PollActivityTaskQueueResponse> = call(
                &endpoint,
                "PollActivityTaskQueue",
                activity_poll_request(&queue_name),
            )
            .await;
        }));
    }
    // Let the polls reach their parked state before cancelling them.
    tokio::task::yield_now().await;
    for handle in &parked {
        handle.abort();
    }
    for handle in parked {
        let _ = handle.await;
    }

    // The full failure/replacement sequence must behave exactly as with no
    // cancelled polls: the retry publish is consumed by the live poller below,
    // not by an aborted handler.
    let token = schedule_and_poll_attempt_one(&engine, workflow_id, queue).await?;
    let endpoint = engine.endpoint();
    let _: RespondActivityTaskFailedResponse = call(
        &endpoint,
        "RespondActivityTaskFailed",
        retryable_failure(token),
    )
    .await?;
    poll_until_attempt_two(&engine, queue).await?;
    assert_eq!(
        pending_activity(&engine, workflow_id).await?,
        Some((2, PendingActivityState::Started)),
        "delivered attempt must be durably started"
    );
    engine.shutdown().await
}

/// Transport parity: the identical dying-consumer sequence over the network
/// listener. The listener-backed server polls handlers on its own runtime by
/// construction; the embedded bridge must match it (the assertion set here is
/// the same as the embedded regression's).
#[tokio::test(flavor = "multi_thread")]
async fn network_listener_delivers_retry_after_consumer_runtime_death() -> Result<()> {
    let engine = TokeiradHandle::start_in_memory("127.0.0.1:0".parse()?).await?;
    let address = format!("http://{}", engine.bound_addr());
    let queue = "retry-replacement-network";
    let workflow_id = "retry-replacement-network-canary";

    let mut client = WorkflowServiceClient::connect(address.clone()).await?;
    let _ = client
        .start_workflow_execution(start_request(workflow_id, queue))
        .await?;
    let workflow_task = client
        .poll_workflow_task_queue(workflow_poll_request(queue))
        .await?
        .into_inner();
    anyhow::ensure!(!workflow_task.task_token.is_empty());
    let _ = client
        .respond_workflow_task_completed(schedule_activity_completion(
            workflow_task.task_token,
            queue,
        ))
        .await?;
    let attempt_one = client
        .poll_activity_task_queue(activity_poll_request(queue))
        .await?
        .into_inner();
    anyhow::ensure!(!attempt_one.task_token.is_empty() && attempt_one.attempt == 1);

    // Worker A dies: its own runtime (and client connection) reports the
    // failure and is then dropped.
    let token = attempt_one.task_token;
    let failure_address = address.clone();
    let queue_name = queue.to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let consumer_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build the consumer runtime")?;
        consumer_runtime.block_on(async move {
            let mut dying_client = WorkflowServiceClient::connect(failure_address).await?;
            let _ = dying_client
                .respond_activity_task_failed(retryable_failure(token))
                .await?;
            let _ = dying_client
                .shutdown_worker(shutdown_request(&queue_name))
                .await?;
            Ok::<(), anyhow::Error>(())
        })?;
        drop(consumer_runtime);
        Ok(())
    })
    .await
    .context("join the consumer thread")??;

    let mut delivered = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.poll_activity_task_queue(activity_poll_request(queue)),
        )
        .await
        .context("network activity poll hung")??
        .into_inner();
        if !response.task_token.is_empty() {
            delivered = Some(response);
            break;
        }
    }
    let delivered = delivered.context("network transport failed to deliver the durable retry")?;
    anyhow::ensure!(delivered.attempt == 2);
    engine.shutdown().await
}

/// Cancellation storms on the embedded transport cannot strand a durable
/// retry. Dropped poll futures (abort-on-drop through the in-process bridge)
/// race each retry's publication and take, so a cancelled handler may consume
/// a broker offer anywhere before its `Started` commit; the live scanner's
/// durable-dispatch reconciliation must redeliver in every case. Each attempt
/// is received exactly once, in order, and the workflow's activity completes
/// with the engine never restarted — no shard takeover, no manual republish.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_poll_storms_cannot_strand_durable_retries() -> Result<()> {
    let engine = Engine::start().await?;
    let queue = "retry-cancel-storm";
    let workflow_id = "retry-cancel-storm-canary";
    let endpoint = engine.endpoint();

    let _: StartWorkflowExecutionResponse = call(
        &endpoint,
        "StartWorkflowExecution",
        start_request(workflow_id, queue),
    )
    .await?;
    let workflow_task: PollWorkflowTaskQueueResponse = call(
        &endpoint,
        "PollWorkflowTaskQueue",
        workflow_poll_request(queue),
    )
    .await?;
    anyhow::ensure!(!workflow_task.task_token.is_empty());
    // Constant 500ms backoff so every round's publication window is quick and
    // identical; enough attempts for four failure rounds plus completion.
    let _: RespondWorkflowTaskCompletedResponse = call(
        &endpoint,
        "RespondWorkflowTaskCompleted",
        RespondWorkflowTaskCompletedRequest {
            task_token: workflow_task.task_token,
            identity: WORKER_IDENTITY.to_owned(),
            commands: vec![Command {
                command_type: CommandType::ScheduleActivityTask as i32,
                user_metadata: None,
                attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                    ScheduleActivityTaskCommandAttributes {
                        activity_id: "turn-1".to_owned(),
                        activity_type: Some(ActivityType {
                            name: "turn".to_owned(),
                        }),
                        task_queue: task_queue(queue),
                        start_to_close_timeout: Some(proto_duration(Duration::from_secs(30))),
                        retry_policy: Some(RetryPolicy {
                            initial_interval: Some(proto_duration(Duration::from_millis(500))),
                            backoff_coefficient: 1.0,
                            maximum_interval: Some(proto_duration(Duration::from_millis(500))),
                            maximum_attempts: 6,
                            non_retryable_error_types: Vec::new(),
                        }),
                        ..Default::default()
                    },
                )),
            }],
            ..Default::default()
        },
    )
    .await?;

    let mut expected_attempt = 1;
    let mut token = loop {
        let response: Option<PollActivityTaskQueueResponse> = tokio::time::timeout(
            Duration::from_secs(5),
            call(
                &endpoint,
                "PollActivityTaskQueue",
                activity_poll_request(queue),
            ),
        )
        .await
        .ok()
        .transpose()?;
        if let Some(response) = response
            && !response.task_token.is_empty()
        {
            anyhow::ensure!(response.attempt == expected_attempt);
            break response.task_token;
        }
    };

    for round in 0..4 {
        let _: RespondActivityTaskFailedResponse = call(
            &endpoint,
            "RespondActivityTaskFailed",
            retryable_failure(token.clone()),
        )
        .await?;

        // The cancellation storm: staggered aborted polls bracketing the
        // 500ms publication window, so drops land while parked, around the
        // wake, and around the broker take.
        for stagger in 0..6u64 {
            let cancelled: std::result::Result<Result<PollActivityTaskQueueResponse>, _> =
                tokio::time::timeout(
                    Duration::from_millis(40 + stagger * 110),
                    call(
                        &endpoint,
                        "PollActivityTaskQueue",
                        activity_poll_request(queue),
                    ),
                )
                .await;
            if let Ok(Ok(response)) = cancelled
                && !response.task_token.is_empty()
            {
                // A storm poll that won the race IS the round's delivery.
                anyhow::ensure!(response.attempt == expected_attempt + 1);
                expected_attempt = response.attempt;
                token = response.task_token;
                break;
            }
        }

        if expected_attempt == round + 2 {
            continue;
        }
        // Otherwise the storm consumed nothing usable: the scanner must
        // redeliver the durable attempt within its cadence.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "durable attempt {} was stranded by the cancellation storm",
                round + 2
            );
            let response: Option<PollActivityTaskQueueResponse> = tokio::time::timeout(
                Duration::from_secs(2),
                call(
                    &endpoint,
                    "PollActivityTaskQueue",
                    activity_poll_request(queue),
                ),
            )
            .await
            .ok()
            .transpose()?;
            if let Some(response) = response
                && !response.task_token.is_empty()
            {
                anyhow::ensure!(
                    response.attempt == round + 2,
                    "attempts must be delivered exactly once, in order: expected {}, got {}",
                    round + 2,
                    response.attempt
                );
                expected_attempt = response.attempt;
                token = response.task_token;
                break;
            }
        }
    }

    // The surviving attempt completes; describe agrees it started exactly at
    // the final attempt and the workflow moves on.
    assert_eq!(
        pending_activity(&engine, workflow_id).await?,
        Some((expected_attempt, PendingActivityState::Started))
    );
    let _: tokeira_proto::workflowservice::RespondActivityTaskCompletedResponse = call(
        &endpoint,
        "RespondActivityTaskCompleted",
        tokeira_proto::workflowservice::RespondActivityTaskCompletedRequest {
            task_token: token,
            identity: WORKER_IDENTITY.to_owned(),
            ..Default::default()
        },
    )
    .await?;
    let next_wft: PollWorkflowTaskQueueResponse = call(
        &endpoint,
        "PollWorkflowTaskQueue",
        workflow_poll_request(queue),
    )
    .await?;
    anyhow::ensure!(
        !next_wft.task_token.is_empty(),
        "activity completion must schedule the next workflow task"
    );
    engine.shutdown().await
}
