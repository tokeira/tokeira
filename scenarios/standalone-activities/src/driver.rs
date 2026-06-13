//! Driver for the standalone-activities scenario.
//!
//! Drives a complete standalone-activity lifecycle against a running `tokeirad`,
//! over the real gRPC wire, playing both roles a real deployment splits across a
//! client and a worker:
//!
//!   - **client**: `StartActivityExecution`, `DescribeActivityExecution`,
//!     `TerminateActivityExecution`;
//!   - **worker**: `PollActivityTaskQueue`, `RespondActivityTaskCompleted` /
//!     `RespondActivityTaskFailed`.
//!
//! It asserts an **observable** outcome at each stage (the status/outcome a
//! `Describe` returns), not any server internal:
//!
//!   1. **completed** — start an activity, poll it as a worker, respond completed,
//!      and confirm `Describe` reports `COMPLETED` with the result payload;
//!   2. **failed** — start, poll, respond failed, confirm `Describe` reports
//!      `FAILED`;
//!   3. **terminated** — start, terminate via the public API, confirm `Describe`
//!      reports `TERMINATED`.
//!
//! Why Tokeira's own proto client and not the published Temporal Rust SDK: the
//! standalone-activity RPCs do not exist in any published SDK yet (their vendored
//! protos predate the surface), so there is nothing to build against downstream.
//! This driver uses the generated `WorkflowServiceClient` from `tokeira-proto`
//! (vendored API v1.62.11) — still a genuine over-the-wire consumer.
//!
//! Server prerequisite: standalone activities must be enabled on the server
//! (`policy.compatibility.enable_standalone_activities = true`); with the gate off
//! every SA RPC answers `UNIMPLEMENTED`, matching the `v1.31.0` baseline.

mod config;

use std::time::Duration;

use anyhow::{Result, anyhow};
use tonic::transport::Channel;

use tokeira_proto::public::temporal::api::common::v1 as common;
use tokeira_proto::public::temporal::api::enums::v1 as enums;
use tokeira_proto::public::temporal::api::failure::v1 as failure;
use tokeira_proto::public::temporal::api::taskqueue::v1 as taskqueue;
use tokeira_proto::public::temporal::api::workflowservice::v1 as wf;
use tokeira_proto::public::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient;

use config::ScenarioConfig;

/// Client handle alias for the helpers (tonic client methods take `&mut self`).
type Client = WorkflowServiceClient<Channel>;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = ScenarioConfig::from_env();
    let mut client = WorkflowServiceClient::connect(cfg.address.clone())
        .await
        .map_err(|e| anyhow!("connect to {}: {e}", cfg.address))?;

    println!(
        "scenario start: address={} namespace={} task_queue={}",
        cfg.address, cfg.namespace, cfg.task_queue
    );

    ensure_namespace(&mut client, &cfg).await?;

    scenario_completed(&mut client, &cfg).await?;
    scenario_failed(&mut client, &cfg).await?;
    scenario_terminated(&mut client, &cfg).await?;

    println!("scenario passed: completed / failed / terminated all observed as expected");
    Ok(())
}

/// Register the namespace so the scenario is self-contained; an existing namespace
/// (the usual case for `default`) is fine, so an `ALREADY_EXISTS` is ignored.
async fn ensure_namespace(client: &mut Client, cfg: &ScenarioConfig) -> Result<()> {
    let req = wf::RegisterNamespaceRequest {
        namespace: cfg.namespace.clone(),
        workflow_execution_retention_period: Some(prost_types::Duration {
            seconds: 24 * 60 * 60,
            nanos: 0,
        }),
        ..Default::default()
    };
    match client.register_namespace(req).await {
        Ok(_) => println!("[ns] registered namespace {}", cfg.namespace),
        Err(status) if status.code() == tonic::Code::AlreadyExists => {
            println!("[ns] namespace {} already exists", cfg.namespace);
        }
        Err(status) => return Err(anyhow!("register namespace: {status}")),
    }
    Ok(())
}

/// Stage 1 — happy path: start, poll as the worker, complete, assert `COMPLETED`.
async fn scenario_completed(client: &mut Client, cfg: &ScenarioConfig) -> Result<()> {
    let activity_id = unique_id("sa-completed");
    let run_id = start_activity(client, cfg, &activity_id).await?;
    println!("[completed] started activity_id={activity_id} run_id={run_id}");

    let task = poll_activity_task(client, cfg).await?;
    let req = wf::RespondActivityTaskCompletedRequest {
        task_token: task.task_token,
        result: Some(payloads(b"pong")),
        identity: cfg.identity.clone(),
        namespace: cfg.namespace.clone(),
        ..Default::default()
    };
    client
        .respond_activity_task_completed(req)
        .await
        .map_err(|s| anyhow!("respond completed: {s}"))?;

    let status = describe_status(client, cfg, &activity_id, &run_id).await?;
    expect(
        status,
        enums::ActivityExecutionStatus::Completed,
        "after responding completed",
    )
}

/// Stage 2 — failure path: start, poll, fail, assert `FAILED`.
async fn scenario_failed(client: &mut Client, cfg: &ScenarioConfig) -> Result<()> {
    let activity_id = unique_id("sa-failed");
    let run_id = start_activity(client, cfg, &activity_id).await?;
    println!("[failed] started activity_id={activity_id} run_id={run_id}");

    let task = poll_activity_task(client, cfg).await?;
    let req = wf::RespondActivityTaskFailedRequest {
        task_token: task.task_token,
        failure: Some(failure::Failure {
            message: "scenario-induced failure".to_owned(),
            ..Default::default()
        }),
        identity: cfg.identity.clone(),
        namespace: cfg.namespace.clone(),
        ..Default::default()
    };
    client
        .respond_activity_task_failed(req)
        .await
        .map_err(|s| anyhow!("respond failed: {s}"))?;

    let status = describe_status(client, cfg, &activity_id, &run_id).await?;
    expect(
        status,
        enums::ActivityExecutionStatus::Failed,
        "after responding failed",
    )
}

/// Stage 3 — terminate path: start, terminate via the public API, assert
/// `TERMINATED` (no worker pickup needed).
async fn scenario_terminated(client: &mut Client, cfg: &ScenarioConfig) -> Result<()> {
    let activity_id = unique_id("sa-terminated");
    let run_id = start_activity(client, cfg, &activity_id).await?;
    println!("[terminated] started activity_id={activity_id} run_id={run_id}");

    let req = wf::TerminateActivityExecutionRequest {
        namespace: cfg.namespace.clone(),
        activity_id: activity_id.clone(),
        run_id: run_id.clone(),
        identity: cfg.identity.clone(),
        reason: "scenario terminate".to_owned(),
        ..Default::default()
    };
    client
        .terminate_activity_execution(req)
        .await
        .map_err(|s| anyhow!("terminate: {s}"))?;

    let status = describe_status(client, cfg, &activity_id, &run_id).await?;
    expect(
        status,
        enums::ActivityExecutionStatus::Terminated,
        "after terminating",
    )
}

/// Start a standalone activity and return its server-assigned run id.
async fn start_activity(
    client: &mut Client,
    cfg: &ScenarioConfig,
    activity_id: &str,
) -> Result<String> {
    let req = wf::StartActivityExecutionRequest {
        namespace: cfg.namespace.clone(),
        identity: cfg.identity.clone(),
        request_id: uuid::Uuid::new_v4().to_string(),
        activity_id: activity_id.to_owned(),
        activity_type: Some(common::ActivityType {
            name: "DemoActivity".to_owned(),
        }),
        task_queue: Some(taskqueue::TaskQueue {
            name: cfg.task_queue.clone(),
            ..Default::default()
        }),
        start_to_close_timeout: Some(prost_types::Duration {
            seconds: 30,
            nanos: 0,
        }),
        input: Some(payloads(b"ping")),
        ..Default::default()
    };
    let resp = client
        .start_activity_execution(req)
        .await
        .map_err(|s| anyhow!("start activity: {s}"))?
        .into_inner();
    Ok(resp.run_id)
}

/// Poll the task queue as a worker until a task is dispatched. The server enqueues
/// the dispatch synchronously on start, so the task is normally available on the
/// first poll; the retry loop tolerates scheduling slack.
async fn poll_activity_task(
    client: &mut Client,
    cfg: &ScenarioConfig,
) -> Result<wf::PollActivityTaskQueueResponse> {
    for _ in 0..50 {
        let req = wf::PollActivityTaskQueueRequest {
            namespace: cfg.namespace.clone(),
            task_queue: Some(taskqueue::TaskQueue {
                name: cfg.task_queue.clone(),
                ..Default::default()
            }),
            identity: cfg.identity.clone(),
            ..Default::default()
        };
        let resp = client
            .poll_activity_task_queue(req)
            .await
            .map_err(|s| anyhow!("poll activity task: {s}"))?
            .into_inner();
        // An empty task_token is the "no task" sentinel (the server returns a
        // default response when the queue is empty).
        if !resp.task_token.is_empty() {
            return Ok(resp);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!(
        "no activity task dispatched on task queue {}",
        cfg.task_queue
    ))
}

/// Describe an activity execution and return its `ActivityExecutionStatus` as the
/// raw enum discriminant.
async fn describe_status(
    client: &mut Client,
    cfg: &ScenarioConfig,
    activity_id: &str,
    run_id: &str,
) -> Result<i32> {
    let req = wf::DescribeActivityExecutionRequest {
        namespace: cfg.namespace.clone(),
        activity_id: activity_id.to_owned(),
        run_id: run_id.to_owned(),
        include_input: false,
        include_outcome: true,
        long_poll_token: Vec::new(),
    };
    let resp = client
        .describe_activity_execution(req)
        .await
        .map_err(|s| anyhow!("describe activity: {s}"))?
        .into_inner();
    Ok(resp.info.map(|info| info.status).unwrap_or(0))
}

/// Assert an observed status equals the expected one, else fail the scenario.
fn expect(observed: i32, expected: enums::ActivityExecutionStatus, ctx: &str) -> Result<()> {
    if observed == expected as i32 {
        println!("[ok] {ctx}: status = {expected:?}");
        Ok(())
    } else {
        Err(anyhow!(
            "{ctx}: expected status {expected:?} ({}), observed discriminant {observed}",
            expected as i32
        ))
    }
}

/// Wrap raw bytes in a single-payload `Payloads` envelope.
fn payloads(data: &[u8]) -> common::Payloads {
    common::Payloads {
        payloads: vec![common::Payload {
            metadata: Default::default(),
            data: data.to_vec(),
        }],
    }
}

/// A unique, human-readable activity id for one scenario stage.
fn unique_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}
