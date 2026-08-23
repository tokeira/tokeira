//! Focused lifecycle and SDK-connection coverage for the zero-listener engine.

use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use http::HeaderMap;
use prost::Message as _;
use temporalio_client::{Connection, ConnectionOptions};
use tokeira_engine::{
    Engine, InProcessGrpcRequest, SnapshotPolicyConfig, TemporalEndpoint, TokeiraConfig,
};
use tokeira_proto::{
    common::{WorkflowExecution, WorkflowType},
    taskqueue::TaskQueue,
    workflowservice::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse, GetSystemInfoRequest,
        GetSystemInfoResponse, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    },
};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
static TEST_SNAPSHOT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestSnapshotPath {
    directory: PathBuf,
    file: PathBuf,
}

impl TestSnapshotPath {
    fn new(label: &str) -> Result<Self> {
        let sequence = TEST_SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "tokeira-engine-{label}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory)?;
        let file = directory.join("engine.snapshot");
        Ok(Self { directory, file })
    }
}

impl Drop for TestSnapshotPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn snapshot_config(path: &Path, interval_ms: u64) -> TokeiraConfig {
    let mut config = TokeiraConfig::default();
    config.policy.snapshot = Some(SnapshotPolicyConfig {
        location: path.to_path_buf(),
        interval_ms,
    });
    config
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
        .await?;
    Ok(Resp::decode(response.proto.as_slice())?)
}

async fn start_workflow(engine: &Engine, workflow_id: &str, task_queue: &str) -> Result<()> {
    let _: StartWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "StartWorkflowExecution",
        StartWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            workflow_id: workflow_id.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "snapshot-workflow".to_owned(),
            }),
            task_queue: Some(TaskQueue {
                name: task_queue.to_owned(),
                ..Default::default()
            }),
            request_id: format!("start-{workflow_id}"),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn assert_workflow_exists(engine: &Engine, workflow_id: &str) -> Result<()> {
    let response: DescribeWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "DescribeWorkflowExecution",
        DescribeWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.to_owned(),
                run_id: String::new(),
            }),
        },
    )
    .await?;
    let execution = response
        .workflow_execution_info
        .and_then(|info| info.execution)
        .context("restored workflow description omitted execution identity")?;
    assert_eq!(execution.workflow_id, workflow_id);
    Ok(())
}

async fn assert_recovered_workflow_task(engine: &Engine, task_queue: &str) -> Result<()> {
    let response: PollWorkflowTaskQueueResponse = tokio::time::timeout(
        Duration::from_secs(5),
        call(
            &engine.endpoint(),
            "PollWorkflowTaskQueue",
            PollWorkflowTaskQueueRequest {
                namespace: "default".to_owned(),
                task_queue: Some(TaskQueue {
                    name: task_queue.to_owned(),
                    ..Default::default()
                }),
                identity: "snapshot-test-worker".to_owned(),
                ..Default::default()
            },
        ),
    )
    .await
    .context("timed out waiting for the recovery sweep to republish workflow work")??;
    assert!(
        !response.task_token.is_empty(),
        "recovery must republish a durable workflow task"
    );
    Ok(())
}

async fn wait_for_snapshot(path: &Path) -> Result<()> {
    for _ in 0..10_000 {
        if tokio::fs::metadata(path).await.is_ok() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    bail!("periodic snapshot was not written to {}", path.display())
}

async fn wait_for_snapshot_change(path: &Path, previous: &[u8]) -> Result<()> {
    for _ in 0..10_000 {
        if let Ok(bytes) = tokio::fs::read(path).await
            && bytes != previous
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
    bail!("periodic snapshot was not refreshed at {}", path.display())
}

#[tokio::test]
async fn raw_endpoint_dispatches_get_system_info() -> Result<()> {
    let engine = Engine::start().await?;
    let response = engine
        .endpoint()
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "GetSystemInfo".to_owned(),
            headers: HeaderMap::new(),
            proto: GetSystemInfoRequest::default().encode_to_vec().into(),
        })
        .await?;

    let decoded = GetSystemInfoResponse::decode(response.proto.as_slice())?;
    assert!(
        decoded.capabilities.is_some(),
        "the embedded endpoint must expose Tokeira's real system capabilities"
    );
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn temporal_client_connects_through_service_override() -> Result<()> {
    let engine = Engine::start().await?;
    let options = ConnectionOptions::new(url::Url::parse("http://tokeira-engine.invalid:7233")?)
        .service_override(engine.service_override())
        .dns_load_balancing(None)
        .build();

    let connection = Connection::connect(options).await?;
    assert!(
        connection.capabilities().is_some(),
        "Connection::connect must complete GetSystemInfo through the override"
    );
    drop(connection);
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn embedded_start_does_not_bind_configured_listeners() -> Result<()> {
    let occupied_grpc = TcpListener::bind("127.0.0.1:0")?;
    let occupied_nexus = TcpListener::bind("127.0.0.1:0")?;
    let mut config = TokeiraConfig::default();
    config.infrastructure.network.grpc_addr = occupied_grpc.local_addr()?.to_string();
    config.policy.nexus_completion.http_addr = occupied_nexus.local_addr()?.to_string();

    // Construction would fail with AddressInUse if either the Temporal or Nexus
    // callback transport were still an implicit part of engine startup.
    let engine = Engine::start_with_config(config).await?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_closes_existing_endpoint_clones() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    engine.shutdown().await?;

    let status = endpoint
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "GetSystemInfo".to_owned(),
            headers: HeaderMap::new(),
            proto: GetSystemInfoRequest::default().encode_to_vec().into(),
        })
        .await
        .expect_err("an endpoint clone must reject calls after engine shutdown");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    Ok(())
}

#[tokio::test]
async fn graceful_shutdown_snapshot_restores_state_and_recovery() -> Result<()> {
    let snapshot = TestSnapshotPath::new("graceful-snapshot")?;
    let config = snapshot_config(&snapshot.file, 3_600_000);
    let engine = Engine::start_with_config(config.clone()).await?;
    start_workflow(&engine, "graceful-workflow", "graceful-queue").await?;
    assert!(
        !snapshot.file.exists(),
        "the interval policy must not take an immediate startup snapshot"
    );

    engine.shutdown().await?;
    assert!(
        snapshot.file.is_file(),
        "graceful shutdown must persist the final snapshot"
    );

    let restored = Engine::start_with_config(config.clone()).await?;
    assert_workflow_exists(&restored, "graceful-workflow").await?;
    assert_recovered_workflow_task(&restored, "graceful-queue").await?;
    restored.shutdown().await?;

    // Graceful shutdown retires the recovery lease before the final cut. A
    // snapshot written after one restored lifecycle must remain bootable again.
    let restored_again = Engine::start_with_config(config).await?;
    assert_workflow_exists(&restored_again, "graceful-workflow").await?;
    restored_again.shutdown().await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn interval_snapshot_is_written_before_shutdown_and_restores() -> Result<()> {
    let snapshot = TestSnapshotPath::new("interval-snapshot")?;
    let config = snapshot_config(&snapshot.file, 100);
    let engine = Engine::start_with_config(config.clone()).await?;
    start_workflow(&engine, "interval-workflow", "interval-queue").await?;

    tokio::time::advance(Duration::from_millis(100)).await;
    wait_for_snapshot(&snapshot.file).await?;
    assert!(
        snapshot.file.is_file(),
        "the configured interval must persist without waiting for shutdown"
    );

    engine.shutdown().await?;
    let before_recovery_interval = tokio::fs::read(&snapshot.file).await?;
    let restored = Engine::start_with_config(config).await?;
    assert_workflow_exists(&restored, "interval-workflow").await?;

    // The restored runtime owns a real recovery lease. Capture that live state,
    // then drop without a final snapshot to model the last interval file after a
    // process crash. The next boot must retire the captured owner, advance the
    // fence, and recover normally.
    tokio::time::advance(Duration::from_millis(100)).await;
    wait_for_snapshot_change(&snapshot.file, &before_recovery_interval).await?;
    drop(restored);
    tokio::task::yield_now().await;

    let after_drop = Engine::start_with_config(snapshot_config(&snapshot.file, 100)).await?;
    assert_workflow_exists(&after_drop, "interval-workflow").await?;
    after_drop.shutdown().await?;
    tokio::time::resume();
    Ok(())
}

#[tokio::test]
async fn corrupt_snapshot_refuses_boot_instead_of_starting_empty() -> Result<()> {
    let snapshot = TestSnapshotPath::new("corrupt-snapshot")?;
    std::fs::write(&snapshot.file, b"not a tokeira snapshot")?;

    let error = Engine::start_with_config(snapshot_config(&snapshot.file, 1_000))
        .await
        .expect_err("corrupt snapshot must fail startup");
    let message = format!("{error:#}");
    assert!(
        message.contains("failed to restore engine snapshot"),
        "unexpected startup error: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn unknown_rpc_preserves_unimplemented_status() -> Result<()> {
    let engine = Engine::start().await?;
    let status = engine
        .endpoint()
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "NoSuchRpc".to_owned(),
            headers: HeaderMap::new(),
            proto: Vec::new().into(),
        })
        .await
        .expect_err("the tonic router must reject an unknown method");

    assert_eq!(status.code(), tonic::Code::Unimplemented);
    engine.shutdown().await?;
    Ok(())
}

/// The listener-backed in-memory server applies the SAME snapshot policy as
/// the embedded facade: graceful shutdown persists the final cut and the next
/// listener boot restores from it, including republishing the recoverable
/// workflow task.
#[tokio::test]
async fn served_in_memory_snapshot_round_trips_across_listener_restarts() -> Result<()> {
    use tokeira_proto::workflowservice::workflow_service_client::WorkflowServiceClient;

    let snapshot = TestSnapshotPath::new("served-snapshot")?;
    let config = snapshot_config(&snapshot.file, 3_600_000);

    let first = tokeira_engine::TokeiradHandle::start_in_memory_with_config(
        "127.0.0.1:0".parse()?,
        config.clone(),
    )
    .await?;
    let mut client =
        WorkflowServiceClient::connect(format!("http://{}", first.bound_addr())).await?;
    let _ = client
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            workflow_id: "served-snapshot-workflow".to_owned(),
            workflow_type: Some(WorkflowType {
                name: "snapshot-workflow".to_owned(),
            }),
            task_queue: Some(TaskQueue {
                name: "served-snapshot-queue".to_owned(),
                ..Default::default()
            }),
            request_id: "start-served-snapshot".to_owned(),
            ..Default::default()
        })
        .await?;
    first.shutdown().await?;
    assert!(
        snapshot.file.is_file(),
        "listener shutdown must persist the final snapshot"
    );

    let second =
        tokeira_engine::TokeiradHandle::start_in_memory_with_config("127.0.0.1:0".parse()?, config)
            .await?;
    let mut client =
        WorkflowServiceClient::connect(format!("http://{}", second.bound_addr())).await?;
    let described = client
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: "served-snapshot-workflow".to_owned(),
                run_id: String::new(),
            }),
        })
        .await?
        .into_inner();
    assert!(described.workflow_execution_info.is_some());
    let task = tokio::time::timeout(
        Duration::from_secs(5),
        client.poll_workflow_task_queue(PollWorkflowTaskQueueRequest {
            namespace: "default".to_owned(),
            task_queue: Some(TaskQueue {
                name: "served-snapshot-queue".to_owned(),
                ..Default::default()
            }),
            identity: "served-snapshot-worker".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .context("recovery must republish the workflow task on the listener boot")??
    .into_inner();
    assert!(!task.task_token.is_empty());
    second.shutdown().await?;
    Ok(())
}
