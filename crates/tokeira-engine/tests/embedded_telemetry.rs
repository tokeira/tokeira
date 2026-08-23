//! Host-owned telemetry integration for one embedded engine lifecycle.
//!
//! This integration target contains exactly one test. It installs host tracing and
//! metrics globals before engine construction so Tokio-spawned handler/runtime tasks use
//! the same host instrumentation; nextest also isolates the target in its own process.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context as _, Result};
use http::HeaderMap;
use metrics_util::debugging::DebuggingRecorder;
use opentelemetry::trace::{SpanId, TraceId, TracerProvider as _};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter},
};
use tokeira_engine::{Engine, InProcessGrpcRequest, TemporalEndpoint};
use tokeira_proto::{
    common::{ActivityType, Payload, Payloads, WorkflowType},
    enums::CommandType,
    public::temporal::api::command::v1::{
        Command, ScheduleActivityTaskCommandAttributes, command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
    },
};
use tracing_subscriber::layer::SubscriberExt as _;

const CANARY: &str = "sensitive-prompt-tool-payload-canary";
const WORKFLOW_ID: &str = "telemetry-workflow";
const TASK_QUEUE: &str = "telemetry-queue";
const ACTIVITY_ID: &str = "telemetry-activity";
const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";

#[derive(Clone, Debug, Default)]
struct TestSpanExporter(Arc<Mutex<Vec<SpanData>>>);

impl TestSpanExporter {
    fn spans(&self) -> Vec<SpanData> {
        self.0.lock().expect("span capture lock").clone()
    }
}

impl SpanExporter for TestSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().expect("span capture lock").extend(batch);
        Ok(())
    }
}

async fn call<Req, Resp>(
    endpoint: &TemporalEndpoint,
    rpc: &str,
    headers: HeaderMap,
    request: Req,
) -> Result<Resp>
where
    Req: prost::Message,
    Resp: prost::Message + Default,
{
    let response = endpoint
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: rpc.to_owned(),
            headers,
            proto: request.encode_to_vec().into(),
        })
        .await?;
    Ok(Resp::decode(response.proto.as_slice())?)
}

fn task_queue() -> Option<TaskQueue> {
    Some(TaskQueue {
        name: TASK_QUEUE.to_owned(),
        ..Default::default()
    })
}

fn canary_payload() -> Payloads {
    Payloads {
        payloads: vec![Payload {
            metadata: Default::default(),
            data: CANARY.as_bytes().to_vec(),
            external_payloads: Vec::new(),
        }],
    }
}

#[tokio::test(flavor = "current_thread")]
async fn host_owned_telemetry_covers_rpc_workflow_activity_and_shutdown() -> Result<()> {
    let exporter = TestSpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("embedded-host-integration");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|error| anyhow::anyhow!("host trace install failed: {error}"))?;
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder
        .install()
        .map_err(|error| anyhow::anyhow!("host metrics install failed: {error}"))?;

    let engine = Engine::start().await?;
    let mut headers = HeaderMap::new();
    headers.insert(
        "traceparent",
        "00-33333333333333333333333333333333-4444444444444444-01".parse()?,
    );
    headers.insert("tracestate", "host=integration".parse()?);
    let started: StartWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "StartWorkflowExecution",
        headers,
        StartWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            workflow_id: WORKFLOW_ID.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "telemetry-workflow-type".to_owned(),
            }),
            task_queue: task_queue(),
            input: Some(canary_payload()),
            request_id: "telemetry-start-request".to_owned(),
            ..Default::default()
        },
    )
    .await?;
    anyhow::ensure!(!started.run_id.is_empty());

    let workflow_task: PollWorkflowTaskQueueResponse = tokio::time::timeout(
        Duration::from_secs(5),
        call(
            &engine.endpoint(),
            "PollWorkflowTaskQueue",
            HeaderMap::new(),
            PollWorkflowTaskQueueRequest {
                namespace: "default".to_owned(),
                task_queue: task_queue(),
                identity: "telemetry-worker".to_owned(),
                ..Default::default()
            },
        ),
    )
    .await
    .context("workflow task telemetry poll timed out")??;
    anyhow::ensure!(!workflow_task.task_token.is_empty());

    let _: RespondWorkflowTaskCompletedResponse = call(
        &engine.endpoint(),
        "RespondWorkflowTaskCompleted",
        HeaderMap::new(),
        RespondWorkflowTaskCompletedRequest {
            task_token: workflow_task.task_token,
            identity: "telemetry-worker".to_owned(),
            commands: vec![Command {
                command_type: CommandType::ScheduleActivityTask as i32,
                user_metadata: None,
                attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                    ScheduleActivityTaskCommandAttributes {
                        activity_id: ACTIVITY_ID.to_owned(),
                        activity_type: Some(ActivityType {
                            name: "telemetry-activity-type".to_owned(),
                        }),
                        task_queue: task_queue(),
                        input: Some(canary_payload()),
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: 30,
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

    let activity: PollActivityTaskQueueResponse = tokio::time::timeout(
        Duration::from_secs(5),
        call(
            &engine.endpoint(),
            "PollActivityTaskQueue",
            HeaderMap::new(),
            PollActivityTaskQueueRequest {
                namespace: "default".to_owned(),
                task_queue: task_queue(),
                identity: "telemetry-worker".to_owned(),
                ..Default::default()
            },
        ),
    )
    .await
    .context("activity telemetry poll timed out")??;
    anyhow::ensure!(!activity.task_token.is_empty());
    anyhow::ensure!(activity.activity_id == ACTIVITY_ID);
    engine.shutdown().await?;

    // Tokeira shutdown is the boundary after which the still-host-owned
    // provider may be flushed. Embedded shutdown must not consume it.
    provider
        .force_flush()
        .map_err(|error| anyhow::anyhow!("host trace flush failed: {error}"))?;
    let spans = exporter.spans();
    let start_span = spans
        .iter()
        .find(|span| span.name.as_ref() == "grpc.start_workflow_execution")
        .context("host exporter did not receive the start RPC span")?;
    anyhow::ensure!(start_span.span_context.trace_id() == TraceId::from_bytes([0x33; 16]));
    anyhow::ensure!(start_span.parent_span_id == SpanId::from_bytes([0x44; 8]));
    anyhow::ensure!(start_span.parent_span_is_remote);

    let workflow_span = spans
        .iter()
        .find(|span| span.name.as_ref() == "workflow_task.process")
        .context("workflow task processing span was not exported")?;
    let workflow_fields = format!("{:?}", workflow_span.attributes);
    anyhow::ensure!(workflow_fields.contains(WORKFLOW_ID));
    anyhow::ensure!(workflow_fields.contains(&started.run_id));
    let activity_span = spans
        .iter()
        .find(|span| span.name.as_ref() == "activity_task.process")
        .context("activity task processing span was not exported")?;
    let activity_fields = format!("{:?}", activity_span.attributes);
    anyhow::ensure!(activity_fields.contains(WORKFLOW_ID));
    anyhow::ensure!(activity_fields.contains(&started.run_id));
    anyhow::ensure!(activity_fields.contains(ACTIVITY_ID));
    anyhow::ensure!(
        spans
            .iter()
            .any(|span| span.name.as_ref() == "lane.process")
    );
    anyhow::ensure!(!format!("{spans:?}").contains(CANARY));

    let lifecycle_metrics = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == tokeira_observability::EMBEDDED_LIFECYCLE_OPERATIONS_TOTAL
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(!lifecycle_metrics.is_empty());
    let allowed_labels = [
        "storage_mode",
        "cluster_status",
        "schema_outcome",
        "ownership_outcome",
        "database_class",
        "operation_kind",
        "error_class",
    ];
    for (key, _, _, _) in &lifecycle_metrics {
        for label in key.key().labels() {
            anyhow::ensure!(allowed_labels.contains(&label.key()));
            anyhow::ensure!(!label.value().contains(CANARY));
        }
    }
    Ok(())
}
