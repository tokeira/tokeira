#![cfg(feature = "dsql-integration")]

//! Explicit, cost-bearing live-AWS verification for managed embedded Aurora DSQL.
//!
//! The test is ignored by default because it creates and then explicitly destroys a
//! real cluster. Re-running it with the same descriptor path recovers an interrupted
//! attempt by canonical cluster ID and the already-durable creation client token.

use std::{
    collections::BTreeMap,
    future::Future,
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context as _, Result, bail, ensure};
use async_trait::async_trait;
use http::{HeaderMap, HeaderValue};
use prost::Message as _;
use temporalio_client::{Connection, ConnectionOptions};
use tokeira_config::{
    EmbeddedDsqlLimits, EmbeddedEngineConfig, EmbeddedStorageConfig, ManagedClusterIntent,
    ManagedEmbeddedDsqlConfig,
};
use tokeira_engine::{
    EmbeddedEngineStartError, EmbeddedStartupPhase, Engine, InProcessGrpcRequest, TemporalEndpoint,
    TokeiraConfig,
};
use tokeira_managed_dsql::{
    AdminDeadline, AwsDsqlControlPlane, ClusterDescriptorState, ClusterDescriptorStore,
    ClusterDescriptorV1, CreateOrRecoverRequest, DescriptorError, DestroyOutcome,
    LocalClusterDescriptorStore, ManagedDsqlAdmin, ManagedDsqlError, ManagedDsqlLifecycle,
    StartupDeadline, SystemLifecycleEnvironment, VersionedClusterDescriptor,
};
use tokeira_proto::{
    common::{WorkflowExecution, WorkflowType},
    enums::{CommandType, WorkflowExecutionStatus},
    public::temporal::api::command::v1::{
        Command, CompleteWorkflowExecutionCommandAttributes,
        command::Attributes as CommandAttributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        DescribeWorkflowExecutionRequest, DescribeWorkflowExecutionResponse, GetSystemInfoRequest,
        PollWorkflowTaskQueueRequest, RespondWorkflowTaskCompletedRequest,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_storage::dsql::ControlLeaseAcquireOutcome;

const LIVE_ACKNOWLEDGEMENT: &str = "CREATE_AND_DELETE";
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";

/// Descriptor seam that models a process failure after AWS creation but before the
/// canonical ready identity can replace the already-durable pending descriptor.
#[derive(Clone, Debug)]
struct CrashAfterCreateStore {
    inner: LocalClusterDescriptorStore,
    fail_ready_once: Arc<AtomicBool>,
}

impl CrashAfterCreateStore {
    fn new(inner: LocalClusterDescriptorStore) -> Self {
        Self {
            inner,
            fail_ready_once: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl ClusterDescriptorStore for CrashAfterCreateStore {
    async fn load(&self) -> Result<Option<VersionedClusterDescriptor>, DescriptorError> {
        self.inner.load().await
    }

    async fn compare_and_swap(
        &self,
        expected_revision: Option<u64>,
        next: &ClusterDescriptorV1,
    ) -> Result<u64, DescriptorError> {
        if matches!(next.state, ClusterDescriptorState::Ready { .. })
            && self.fail_ready_once.swap(false, Ordering::SeqCst)
        {
            return Err(DescriptorError::Io(
                "injected post-create descriptor failure".to_owned(),
            ));
        }
        self.inner.compare_and_swap(expected_revision, next).await
    }
}

fn required_environment(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} must be set for the live-AWS test"))
}

fn live_configuration(
    region: &str,
    descriptor_path: PathBuf,
    tags: BTreeMap<String, String>,
    grpc_listener: &TcpListener,
    metrics_listener: &TcpListener,
    nexus_listener: &TcpListener,
) -> EmbeddedEngineConfig {
    let mut server = TokeiraConfig::default();
    server.infrastructure.network.grpc_addr = grpc_listener
        .local_addr()
        .expect("occupied gRPC listener has an address")
        .to_string();
    server.infrastructure.network.metrics_addr = metrics_listener
        .local_addr()
        .expect("occupied metrics listener has an address")
        .to_string();
    server.policy.nexus_completion.http_addr = nexus_listener
        .local_addr()
        .expect("occupied Nexus listener has an address")
        .to_string();
    EmbeddedEngineConfig {
        server,
        storage: EmbeddedStorageConfig::ManagedDsql(ManagedEmbeddedDsqlConfig {
            intent: ManagedClusterIntent::CreateOrRecover,
            descriptor_path,
            region: region.to_owned(),
            migration_policy: None,
            limits: EmbeddedDsqlLimits::default(),
            tags,
        }),
        startup_timeout_ms: LIFECYCLE_TIMEOUT.as_millis() as u64,
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

fn trace_headers(trace_id: &str, span_id: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "traceparent",
        HeaderValue::from_str(&format!("00-{trace_id}-{span_id}-01"))?,
    );
    headers.insert("tracestate", HeaderValue::from_static("tokeira=live"));
    Ok(headers)
}

fn combine_generation_result<T, E>(
    exercise: Result<T>,
    shutdown: std::result::Result<(), E>,
    generation: &str,
) -> Result<T> {
    match (exercise, shutdown) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => {
            Err(error).with_context(|| format!("{generation} engine exercise failed"))
        }
        (Ok(_), Err(_)) => bail!("{generation} engine shutdown failed"),
        (Err(_), Err(_)) => bail!("{generation} engine exercise and shutdown failed"),
    }
}

async fn exercise_first_generation(
    engine: &Engine,
    config: &EmbeddedEngineConfig,
    cluster_id: &str,
    cluster_arn: &str,
) -> Result<(String, String, i64, String)> {
    let report = engine.startup_report();
    let cluster = report
        .cluster
        .as_ref()
        .context("managed startup report must contain canonical cluster identity")?;
    ensure!(cluster.cluster_id == cluster_id);
    ensure!(cluster.cluster_arn == cluster_arn);
    ensure!(
        report.schema.is_some(),
        "managed startup must apply the schema contract"
    );
    ensure!(
        report.ownership.is_some(),
        "managed startup must acquire exclusive embedded ownership"
    );
    let first_fence = report
        .ownership
        .context("managed startup must report the owner fence")?
        .fence_token;

    let options = ConnectionOptions::new(url::Url::parse(
        "http://managed-embedded-dsql.invalid:7233",
    )?)
    .service_override(engine.service_override())
    .dns_load_balancing(None)
    .build();
    let connection = Connection::connect(options).await?;
    ensure!(
        connection.capabilities().is_some(),
        "the DSQL-backed embedded edge must serve Temporal SDK calls through service_override"
    );
    drop(connection);

    let cluster_prefix = cluster_id.chars().take(8).collect::<String>();
    let workflow_id = format!("managed-live-{cluster_prefix}");
    let started: StartWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "StartWorkflowExecution",
        trace_headers("11111111111111111111111111111111", "1111111111111111")?,
        StartWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            workflow_id: workflow_id.clone(),
            workflow_type: Some(WorkflowType {
                name: "managed-live-workflow".to_owned(),
            }),
            task_queue: Some(TaskQueue {
                name: "managed-live-queue".to_owned(),
                ..Default::default()
            }),
            request_id: format!("start-{workflow_id}"),
            ..Default::default()
        },
    )
    .await?;
    ensure!(!started.run_id.is_empty());

    // Listener leg (embedded-engine-listener Property 7): a network worker runs
    // one execution against the DSQL-backed engine, and the competing start
    // below is checked with the listener still attached so ownership stays
    // exclusive regardless of transport.
    let listener = engine.listen("127.0.0.1:0".parse()?).await?;
    let network_workflow_id = format!("managed-live-listener-{cluster_prefix}");
    let network_result = exercise_network_worker(listener.bound_addr(), &network_workflow_id).await;

    let mut competing_config = config.clone();
    competing_config.startup_timeout_ms = Duration::from_secs(120).as_millis() as u64;
    let competing_result = match Engine::start_with_embedded_config(competing_config).await {
        Ok(competing) => {
            let shutdown = competing.shutdown().await;
            ensure!(
                shutdown.is_ok(),
                "an unexpectedly admitted competing engine also failed explicit shutdown"
            );
            Err(anyhow::anyhow!(
                "a live embedded owner unexpectedly admitted a second engine"
            ))
        }
        Err(EmbeddedEngineStartError::Phase {
            phase: EmbeddedStartupPhase::Ownership,
        }) => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "the competing engine must fail at ownership without mutating shared state: {error}"
        )),
    };
    let listener_shutdown = listener.shutdown().await;
    network_result?;
    competing_result?;
    listener_shutdown.context("listener-only shutdown must leave the engine serving")?;
    let _: DescribeWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "DescribeWorkflowExecution",
        HeaderMap::new(),
        DescribeWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.clone(),
                run_id: String::new(),
            }),
        },
    )
    .await
    .context("the in-process endpoint must keep serving after listener shutdown")?;

    Ok((
        workflow_id,
        started.run_id,
        first_fence,
        network_workflow_id,
    ))
}

/// Run one execution end to end over the listener: start, poll the workflow
/// task, complete it, and observe the closed execution.
async fn exercise_network_worker(addr: std::net::SocketAddr, workflow_id: &str) -> Result<()> {
    let mut client = WorkflowServiceClient::connect(format!("http://{addr}")).await?;
    let _ = client
        .get_system_info(GetSystemInfoRequest::default())
        .await?;
    let queue = "managed-live-listener-queue";
    let started = client
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            workflow_id: workflow_id.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "managed-live-listener-workflow".to_owned(),
            }),
            task_queue: Some(TaskQueue {
                name: queue.to_owned(),
                ..Default::default()
            }),
            request_id: format!("start-{workflow_id}"),
            ..Default::default()
        })
        .await?
        .into_inner();
    ensure!(!started.run_id.is_empty());
    let task = tokio::time::timeout(
        Duration::from_secs(30),
        client.poll_workflow_task_queue(PollWorkflowTaskQueueRequest {
            namespace: "default".to_owned(),
            task_queue: Some(TaskQueue {
                name: queue.to_owned(),
                ..Default::default()
            }),
            identity: "managed-live-network-worker".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .context("the network worker must receive the workflow task")??
    .into_inner();
    ensure!(
        !task.task_token.is_empty(),
        "the network poll must deliver the task"
    );
    client
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            task_token: task.task_token,
            identity: "managed-live-network-worker".to_owned(),
            namespace: "default".to_owned(),
            commands: vec![Command {
                command_type: CommandType::CompleteWorkflowExecution as i32,
                user_metadata: None,
                attributes: Some(
                    CommandAttributes::CompleteWorkflowExecutionCommandAttributes(
                        CompleteWorkflowExecutionCommandAttributes { result: None },
                    ),
                ),
            }],
            ..Default::default()
        })
        .await?;
    let described = client
        .describe_workflow_execution(DescribeWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.to_owned(),
                run_id: started.run_id,
            }),
        })
        .await?
        .into_inner();
    let status = described
        .workflow_execution_info
        .map(|info| info.status)
        .context("describe over the listener must carry execution info")?;
    ensure!(
        status == WorkflowExecutionStatus::Completed as i32,
        "the network-completed execution must be closed, got {status}"
    );
    Ok(())
}

async fn exercise_restart_generation(
    engine: &Engine,
    cluster_id: &str,
    cluster_arn: &str,
    workflow_id: &str,
    run_id: &str,
    first_fence: i64,
    network_workflow_id: &str,
) -> Result<()> {
    let restart_report = engine.startup_report();
    let restart_cluster = restart_report
        .cluster
        .as_ref()
        .context("restart must report its recovered cluster")?;
    ensure!(restart_cluster.cluster_id == cluster_id);
    ensure!(restart_cluster.cluster_arn == cluster_arn);
    let restart_ownership = restart_report
        .ownership
        .context("restart must report clean ownership")?;
    ensure!(restart_ownership.outcome == ControlLeaseAcquireOutcome::Clean);
    ensure!(restart_ownership.fence_token > first_fence);

    let described: DescribeWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "DescribeWorkflowExecution",
        trace_headers("22222222222222222222222222222222", "2222222222222222")?,
        DescribeWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.to_owned(),
                run_id: String::new(),
            }),
        },
    )
    .await?;
    let execution = described
        .workflow_execution_info
        .and_then(|info| info.execution)
        .context("restarted engine must preserve the workflow execution")?;
    ensure!(execution.workflow_id == workflow_id);
    ensure!(execution.run_id == run_id);

    // The execution a network worker completed in the first generation is part
    // of the same durable history the restarted owner recovers.
    let network_described: DescribeWorkflowExecutionResponse = call(
        &engine.endpoint(),
        "DescribeWorkflowExecution",
        HeaderMap::new(),
        DescribeWorkflowExecutionRequest {
            namespace: "default".to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: network_workflow_id.to_owned(),
                run_id: String::new(),
            }),
        },
    )
    .await?;
    let network_status = network_described
        .workflow_execution_info
        .map(|info| info.status)
        .context("restarted engine must preserve the network-completed execution")?;
    ensure!(network_status == WorkflowExecutionStatus::Completed as i32);
    Ok(())
}

async fn exercise_ready_cluster(
    region: &str,
    descriptor_path: PathBuf,
    tags: BTreeMap<String, String>,
    cluster_id: &str,
    cluster_arn: &str,
) -> Result<()> {
    // Occupied configured listeners prove embedded transport remains callback-only.
    let occupied_grpc = TcpListener::bind("127.0.0.1:0")?;
    let occupied_metrics = TcpListener::bind("127.0.0.1:0")?;
    let occupied_nexus = TcpListener::bind("127.0.0.1:0")?;
    let config = live_configuration(
        region,
        descriptor_path,
        tags,
        &occupied_grpc,
        &occupied_metrics,
        &occupied_nexus,
    );

    let engine = Engine::start_with_embedded_config(config.clone()).await?;
    let old_endpoint = engine.endpoint();
    let first_result = exercise_first_generation(&engine, &config, cluster_id, cluster_arn).await;
    let first_shutdown = engine.shutdown().await;
    let (workflow_id, run_id, first_fence, network_workflow_id) =
        combine_generation_result(first_result, first_shutdown, "first")?;

    let old_status = old_endpoint
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "GetSystemInfo".to_owned(),
            headers: HeaderMap::new(),
            proto: GetSystemInfoRequest::default().encode_to_vec().into(),
        })
        .await
        .expect_err("the old embedded endpoint must remain closed after ownership release");
    ensure!(old_status.code() == tonic::Code::Unavailable);

    let restarted = Engine::start_with_embedded_config(config).await?;
    let restart_result = exercise_restart_generation(
        &restarted,
        cluster_id,
        cluster_arn,
        &workflow_id,
        &run_id,
        first_fence,
        &network_workflow_id,
    )
    .await;
    let restart_shutdown = restarted.shutdown().await;
    combine_generation_result(restart_result, restart_shutdown, "restarted")
}

async fn destroy_ready_cluster(
    control: AwsDsqlControlPlane,
    store: LocalClusterDescriptorStore,
    expected_cluster_id: String,
    expected_cluster_arn: String,
) -> Result<()> {
    let admin = ManagedDsqlAdmin::new(control, store.clone());
    let plan = admin
        .plan_destroy(AdminDeadline::after(LIFECYCLE_TIMEOUT))
        .await?;
    ensure!(plan.cluster_id == expected_cluster_id);
    ensure!(plan.cluster_arn == expected_cluster_arn);
    ensure!(
        plan.deletion_protection_enabled,
        "the explicit destroy plan must observe deletion protection before disabling it"
    );
    let confirmation = plan.confirm();
    let destroyed = admin
        .apply_destroy(&plan, confirmation, AdminDeadline::after(LIFECYCLE_TIMEOUT))
        .await?;
    ensure!(destroyed.outcome == DestroyOutcome::Destroyed);
    ensure!(
        matches!(
            store
                .load()
                .await?
                .context("destruction must leave a durable tombstone")?
                .into_v1()
                .state,
            ClusterDescriptorState::Destroyed { .. }
        ),
        "explicit destruction must persist a destroyed tombstone"
    );
    Ok(())
}

fn combine_ready_cluster_results(exercise: Result<()>, teardown: Result<()>) -> Result<()> {
    match (exercise, teardown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(_), Ok(())) => {
            bail!("managed embedded DSQL exercise failed; administrative teardown completed")
        }
        (Ok(()), Err(_)) => bail!(
            "managed embedded DSQL administrative teardown failed; the cluster may remain live; use descriptor-bound recovery"
        ),
        (Err(_), Err(_)) => bail!(
            "managed embedded DSQL exercise and administrative teardown failed; the cluster may remain live; use descriptor-bound recovery"
        ),
    }
}

async fn complete_ready_cluster<Exercise, Teardown, TeardownFuture>(
    exercise: Exercise,
    teardown: Teardown,
) -> Result<()>
where
    Exercise: Future<Output = Result<()>>,
    Teardown: FnOnce() -> TeardownFuture,
    TeardownFuture: Future<Output = Result<()>>,
{
    let exercise_result = exercise.await;
    let teardown_result = teardown().await;
    combine_ready_cluster_results(exercise_result, teardown_result)
}

#[tokio::test]
#[ignore = "creates and destroys a billable Aurora DSQL cluster; follow docs/testing/managed-embedded-dsql-live-aws.md"]
async fn managed_embedded_dsql_live_lifecycle() -> Result<()> {
    ensure!(
        required_environment("TOKEIRA_LIVE_MANAGED_DSQL_ACK")? == LIVE_ACKNOWLEDGEMENT,
        "TOKEIRA_LIVE_MANAGED_DSQL_ACK must equal {LIVE_ACKNOWLEDGEMENT}"
    );
    let region = required_environment("TOKEIRA_LIVE_DSQL_REGION")?;
    let descriptor_path = PathBuf::from(required_environment("TOKEIRA_LIVE_DSQL_DESCRIPTOR_PATH")?);
    ensure!(
        descriptor_path.is_absolute(),
        "TOKEIRA_LIVE_DSQL_DESCRIPTOR_PATH must be absolute"
    );
    let parent = descriptor_path
        .parent()
        .context("the descriptor path must have a parent directory")?;
    ensure!(
        parent.is_dir(),
        "descriptor parent {} must already exist",
        parent.display()
    );

    let mut tags = BTreeMap::new();
    tags.insert("tokeira-live-test".to_owned(), "true".to_owned());
    let request = CreateOrRecoverRequest {
        region: region.clone(),
        tags: tags.clone(),
    };
    let store = LocalClusterDescriptorStore::new(&descriptor_path);
    let control = AwsDsqlControlPlane::from_region(region.clone()).await;
    let environment = SystemLifecycleEnvironment;

    if store.load().await?.is_none() {
        let crash_store = CrashAfterCreateStore::new(store.clone());
        let lifecycle = ManagedDsqlLifecycle::new(control.clone(), crash_store, environment);
        let failure = lifecycle
            .create_or_recover(
                request.clone(),
                StartupDeadline::after(&environment, LIFECYCLE_TIMEOUT),
            )
            .await
            .expect_err("the fresh live run must inject a post-create failure");
        ensure!(
            matches!(
                failure,
                ManagedDsqlError::Descriptor(DescriptorError::Io(ref message))
                    if message == "injected post-create descriptor failure"
            ),
            "unexpected injected failure: {failure}"
        );
        let pending = store
            .load()
            .await?
            .context("the creation token must remain durable after the injected failure")?
            .into_v1();
        ensure!(
            matches!(pending.state, ClusterDescriptorState::PendingCreate),
            "the injected failure must leave a pending descriptor"
        );
    }

    let before_recovery = store
        .load()
        .await?
        .context("the live lifecycle requires a pending or ready descriptor")?
        .into_v1();
    if matches!(
        before_recovery.state,
        ClusterDescriptorState::Destroyed { .. }
    ) {
        bail!(
            "the descriptor is a destroyed tombstone; choose a new descriptor path for a new cluster"
        );
    }
    let creation_token = before_recovery.creation_client_token.expose().to_owned();

    let lifecycle = ManagedDsqlLifecycle::new(control.clone(), store.clone(), environment);
    let recovered = lifecycle
        .create_or_recover(
            request,
            StartupDeadline::after(&environment, LIFECYCLE_TIMEOUT),
        )
        .await?;
    let ready = store
        .load()
        .await?
        .context("recovery must commit the canonical identity")?
        .into_v1();
    let (cluster_id, cluster_arn, endpoint) = match &ready.state {
        ClusterDescriptorState::Ready {
            cluster_id,
            cluster_arn,
            endpoint,
        } => (cluster_id, cluster_arn, endpoint),
        ClusterDescriptorState::PendingCreate => {
            bail!("recovery returned without committing a ready descriptor")
        }
        ClusterDescriptorState::Destroyed { .. } => {
            bail!("recovery unexpectedly observed a destroyed tombstone")
        }
    };
    // Once the descriptor is Ready, assertion failures belong to the captured
    // exercise result so none can bypass descriptor-bound teardown.
    let ready_validation = (|| -> Result<()> {
        ensure!(
            ready.creation_client_token.expose() == creation_token,
            "recovery must reuse the creation token persisted before CreateCluster"
        );
        ensure!(recovered.identity.cluster_id == *cluster_id);
        ensure!(recovered.identity.cluster_arn == *cluster_arn);
        ensure!(recovered.endpoint == *endpoint);
        ensure!(
            recovered.deletion_protection_enabled,
            "managed creation must enable deletion protection"
        );
        Ok(())
    })();

    let teardown_cluster_id = cluster_id.to_owned();
    let teardown_cluster_arn = cluster_arn.to_owned();
    complete_ready_cluster(
        async {
            ready_validation?;
            exercise_ready_cluster(&region, descriptor_path, tags, cluster_id, cluster_arn).await
        },
        move || destroy_ready_cluster(control, store, teardown_cluster_id, teardown_cluster_arn),
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::anyhow;

    use super::*;

    const SECRET_CANARIES: [&str; 6] = [
        "credential-canary=AKIA0000000000000000",
        "database-token-canary=db-token-secret",
        "creation-token-canary=create-token-secret",
        "postgresql://secret@cluster.example.invalid/tokeira",
        "endpoint-canary=cluster.dsql.example.invalid",
        "/private/descriptor-canary.json",
    ];

    #[tokio::test]
    async fn ready_cluster_teardown_runs_once_and_results_are_secret_safe() {
        for (exercise_fails, teardown_fails, expected_message) in [
            (false, false, None),
            (
                true,
                false,
                Some("managed embedded DSQL exercise failed; administrative teardown completed"),
            ),
            (
                false,
                true,
                Some(
                    "managed embedded DSQL administrative teardown failed; the cluster may remain live; use descriptor-bound recovery",
                ),
            ),
            (
                true,
                true,
                Some(
                    "managed embedded DSQL exercise and administrative teardown failed; the cluster may remain live; use descriptor-bound recovery",
                ),
            ),
        ] {
            let teardown_calls = Arc::new(AtomicUsize::new(0));
            let canary_error = SECRET_CANARIES.join("|");
            let exercise_error = canary_error.clone();
            let teardown_error = canary_error.clone();
            let calls = Arc::clone(&teardown_calls);

            let result = complete_ready_cluster(
                async move {
                    if exercise_fails {
                        Err(anyhow!(exercise_error))
                    } else {
                        Ok(())
                    }
                },
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if teardown_fails {
                        Err(anyhow!(teardown_error))
                    } else {
                        Ok(())
                    }
                },
            )
            .await;

            assert_eq!(teardown_calls.load(Ordering::SeqCst), 1);
            match expected_message {
                Some(expected) => {
                    let message = result
                        .expect_err("selected failure must be classified")
                        .to_string();
                    assert_eq!(message, expected);
                    for canary in SECRET_CANARIES {
                        assert!(!message.contains(canary));
                    }
                }
                None => result.expect("successful exercise and teardown must succeed"),
            }
        }
    }
}
