//! Regression probes for Temporal Rust SDK 0.7.0 shutdown with an Activity retry pending.
//!
//! The ordinary spike remains the clean completed-workflow path. These separate
//! probes order shutdown after the provider announces an imminent retryable
//! failure but before the Activity returns it to SDK core. The Activity then
//! drains its event-to-heartbeat pump and records a final heartbeat before
//! returning the error, matching Odori's race. The probes cover both storage
//! modes and both same-runtime and Odori-shaped dedicated-thread worker
//! placement. On unfixed code, the ignored managed-DSQL dedicated-thread test
//! reproduces SDK Core's slot-permit shutdown panic; the in-memory controls
//! exit cleanly.

use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, ensure};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowDescribeOptions,
    WorkflowStartOptions,
    callback_based::{CallbackBasedGrpcService, GrpcRequest},
};
use temporalio_common::{
    RetryPolicy, error::ApplicationFailure, protos::temporal::api::enums::v1::PendingActivityState,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tokeira_config::{
    EmbeddedDsqlLimits, EmbeddedEngineConfig, EmbeddedStorageConfig, ManagedClusterIntent,
    ManagedEmbeddedDsqlConfig,
};
use tokeira_engine::{Engine, TokeiraConfig};
use tokio::sync::Notify;

const EMBEDDED_URL: &str = "http://tokeira-engine.invalid:7233";
const LIVE_DSQL_ACKNOWLEDGEMENT: &str = "USE_EXISTING_CLUSTER";
const RETRY_BACKOFF: Duration = Duration::from_secs(60);
const IN_MEMORY_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_DSQL_PROBE_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const MANAGED_DSQL_STARTUP_TIMEOUT_MS: u64 = 15 * 60 * 1_000;
// Odori does not override SDK 0.7.0's default, so its workers retain this cache size.
const ODORI_MAX_CACHED_WORKFLOWS: usize = 1_000;

#[derive(Debug)]
struct RpcObservation {
    rpc: String,
    started_after: Duration,
    elapsed: Duration,
    outcome: &'static str,
}

#[derive(Debug)]
struct RpcObservations {
    probe_started: Instant,
    calls: Mutex<Vec<RpcObservation>>,
}

impl RpcObservations {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            probe_started: Instant::now(),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn summary(&self) -> String {
        let calls = self.calls.lock().expect("RPC observation lock poisoned");
        calls
            .iter()
            .filter(|call| {
                matches!(
                    call.rpc.as_str(),
                    "RecordActivityTaskHeartbeat"
                        | "RespondActivityTaskFailed"
                        | "RespondWorkflowTaskCompleted"
                        | "RespondWorkflowTaskFailed"
                )
            })
            .map(|call| {
                format!(
                    "{} start={:.3}s elapsed={:.3}s {}",
                    call.rpc,
                    call.started_after.as_secs_f64(),
                    call.elapsed.as_secs_f64(),
                    call.outcome,
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

struct RpcObservationGuard {
    observations: Arc<RpcObservations>,
    rpc: String,
    started: Instant,
    outcome: &'static str,
}

impl RpcObservationGuard {
    fn new(observations: Arc<RpcObservations>, rpc: String) -> Self {
        Self {
            observations,
            rpc,
            started: Instant::now(),
            outcome: "cancelled",
        }
    }

    fn complete(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for RpcObservationGuard {
    fn drop(&mut self) {
        let observation = RpcObservation {
            rpc: self.rpc.clone(),
            started_after: self.started.duration_since(self.observations.probe_started),
            elapsed: self.started.elapsed(),
            outcome: self.outcome,
        };
        self.observations
            .calls
            .lock()
            .expect("RPC observation lock poisoned")
            .push(observation);
    }
}

enum ProbeStorage {
    InMemory,
    ManagedDsql {
        region: String,
        descriptor_path: PathBuf,
    },
}

#[derive(Clone, Copy)]
enum WorkerPlacement {
    HostRuntime,
    DedicatedCurrentThread,
}

#[workflow]
#[derive(Default)]
struct RetryPendingWorkflow;

#[workflow_methods]
impl RetryPendingWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.execute_activity(
            RetryFailureActivities::fail_retryably,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
                .retry_policy(
                    RetryPolicy::builder()
                        .initial_interval(RETRY_BACKOFF)
                        .maximum_attempts(2)
                        .build(),
                )
                .build(),
        )
        .await?;
        Ok(())
    }
}

struct RetryFailureActivities {
    failure_announced: Arc<Notify>,
    release_provider_failure: Arc<Notify>,
}

#[activities]
impl RetryFailureActivities {
    #[activity]
    async fn fail_retryably(self: Arc<Self>, ctx: ActivityContext) -> Result<(), ActivityError> {
        let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel::<String>(64);
        let heartbeat_ctx = ctx.clone();
        let heartbeat_pump = tokio::spawn(async move {
            let mut final_state = String::new();
            while let Some(event) = event_receiver.recv().await {
                final_state = event;
                let _ = heartbeat_ctx.record_heartbeat(final_state.clone()).await;
            }
            final_state
        });
        event_sender
            .send("provider-session-established".to_owned())
            .await
            .map_err(ActivityError::from)?;

        // Odori announces FailureReturned inside the provider. The provider
        // has not returned to the Activity wrapper at this point, so the host
        // can begin worker shutdown while the Activity still owns its slot.
        self.failure_announced.notify_one();
        self.release_provider_failure.notified().await;
        let provider_result = Err(ActivityError::application(ApplicationFailure::new(
            anyhow!("intentional retryable Activity failure"),
        )));

        // Dropping the provider's event sink closes the channel. Odori joins
        // this pump and writes one final heartbeat before exposing the provider
        // failure to Temporal SDK core.
        drop(event_sender);
        let final_heartbeat = heartbeat_pump.await.unwrap_or_default();
        let _ = ctx.record_heartbeat(final_heartbeat).await;
        provider_result
    }
}

#[tokio::test]
async fn in_memory_worker_shutdown_at_retryable_failure_boundary_exits_cleanly() -> Result<()> {
    tokio::time::timeout(
        IN_MEMORY_PROBE_TIMEOUT,
        run_probe(ProbeStorage::InMemory, WorkerPlacement::HostRuntime),
    )
    .await
    .context("in-memory retry-pending shutdown probe exceeded 30 seconds")??;
    Ok(())
}

#[tokio::test]
async fn in_memory_dedicated_worker_thread_shutdown_exits_cleanly() -> Result<()> {
    tokio::time::timeout(
        IN_MEMORY_PROBE_TIMEOUT,
        run_probe(
            ProbeStorage::InMemory,
            WorkerPlacement::DedicatedCurrentThread,
        ),
    )
    .await
    .context("dedicated-thread in-memory shutdown probe exceeded 30 seconds")??;
    Ok(())
}

#[tokio::test]
#[ignore = "uses an existing live Aurora DSQL cluster; set the documented acknowledgement and descriptor environment"]
async fn managed_dsql_worker_shutdown_at_retryable_failure_boundary_exits_cleanly() -> Result<()> {
    tokio::time::timeout(
        LIVE_DSQL_PROBE_TIMEOUT,
        run_probe(live_dsql_storage()?, WorkerPlacement::HostRuntime),
    )
    .await
    .context("managed-DSQL retry-pending shutdown probe exceeded 20 minutes")??;
    Ok(())
}

#[tokio::test]
#[ignore = "uses an existing live Aurora DSQL cluster; set the documented acknowledgement and descriptor environment"]
async fn managed_dsql_dedicated_worker_thread_shutdown_exits_cleanly() -> Result<()> {
    tokio::time::timeout(
        LIVE_DSQL_PROBE_TIMEOUT,
        run_probe(
            live_dsql_storage()?,
            WorkerPlacement::DedicatedCurrentThread,
        ),
    )
    .await
    .context("dedicated-thread managed-DSQL shutdown probe exceeded 20 minutes")??;
    Ok(())
}

fn live_dsql_storage() -> Result<ProbeStorage> {
    ensure!(
        required_environment("TOK_REPRO_DSQL_ACK")? == LIVE_DSQL_ACKNOWLEDGEMENT,
        "TOK_REPRO_DSQL_ACK must equal {LIVE_DSQL_ACKNOWLEDGEMENT}"
    );
    let region = required_environment("TOK_REPRO_DSQL_REGION")?;
    let descriptor_path = PathBuf::from(required_environment("TOK_REPRO_DSQL_DESCRIPTOR_PATH")?);
    ensure!(
        descriptor_path.is_absolute(),
        "TOK_REPRO_DSQL_DESCRIPTOR_PATH must be absolute"
    );
    ensure!(
        descriptor_path.is_file(),
        "TOK_REPRO_DSQL_DESCRIPTOR_PATH must name an existing managed descriptor"
    );
    Ok(ProbeStorage::ManagedDsql {
        region,
        descriptor_path,
    })
}

fn required_environment(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} must be set for the live-DSQL probe"))
}

fn unique_execution_names() -> Result<(String, String)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock must be after the Unix epoch")?
        .as_nanos();
    let suffix = format!("{}-{timestamp}", std::process::id());
    Ok((
        format!("sdk-v0-7-retry-shutdown-{suffix}"),
        format!("sdk-v0-7-retry-shutdown-{suffix}"),
    ))
}

async fn run_probe(storage: ProbeStorage, worker_placement: WorkerPlacement) -> Result<()> {
    // Sentinels retain the clean spike's proof that service_override does not
    // fall back to an accidental Temporal or Nexus TCP listener.
    let grpc_guard = TcpListener::bind("127.0.0.1:0").context("reserve gRPC sentinel port")?;
    let metrics_guard =
        TcpListener::bind("127.0.0.1:0").context("reserve metrics sentinel port")?;
    let nexus_guard = TcpListener::bind("127.0.0.1:0").context("reserve Nexus sentinel port")?;
    let mut config = TokeiraConfig::default();
    config.infrastructure.network.grpc_addr = grpc_guard.local_addr()?.to_string();
    config.infrastructure.network.metrics_addr = metrics_guard.local_addr()?.to_string();
    config.policy.nexus_completion.http_addr = nexus_guard.local_addr()?.to_string();

    let engine = match storage {
        ProbeStorage::InMemory => Engine::start_with_config(config)
            .await
            .context("start zero-listener in-memory Tokeira engine")?,
        ProbeStorage::ManagedDsql {
            region,
            descriptor_path,
        } => Engine::start_with_embedded_config(EmbeddedEngineConfig {
            server: config,
            storage: EmbeddedStorageConfig::ManagedDsql(ManagedEmbeddedDsqlConfig {
                intent: ManagedClusterIntent::CreateOrRecover,
                descriptor_path,
                region,
                migration_policy: None,
                limits: EmbeddedDsqlLimits::default(),
                tags: BTreeMap::new(),
            }),
            startup_timeout_ms: MANAGED_DSQL_STARTUP_TIMEOUT_MS,
        })
        .await
        .context("start zero-listener managed-DSQL Tokeira engine")?,
    };
    let rpc_observations = RpcObservations::new();
    let service_override =
        observed_service_override(engine.service_override(), Arc::clone(&rpc_observations));
    let connection = Connection::connect(
        ConnectionOptions::new(Url::parse(EMBEDDED_URL)?)
            .service_override(service_override)
            .dns_load_balancing(None)
            .build(),
    )
    .await
    .context("connect Temporal Rust SDK 0.7.0 through service_override")?;
    let client = Client::new(connection, ClientOptions::new("default".to_owned()).build())?;

    let failure_announced = Arc::new(Notify::new());
    let release_provider_failure = Arc::new(Notify::new());
    let (task_queue, workflow_id) = unique_execution_names()?;
    let scenario_result = match worker_placement {
        WorkerPlacement::HostRuntime => {
            run_on_host_runtime(
                client,
                task_queue,
                workflow_id,
                failure_announced,
                release_provider_failure,
            )
            .await
        }
        WorkerPlacement::DedicatedCurrentThread => {
            run_on_dedicated_current_thread(
                client,
                task_queue,
                workflow_id,
                failure_announced,
                release_provider_failure,
            )
            .await
        }
    };
    let shutdown_result = engine
        .shutdown()
        .await
        .context("shut down embedded Tokeira engine");
    scenario_result
        .with_context(|| format!("observed embedded RPCs: {}", rpc_observations.summary()))?;
    shutdown_result?;
    Ok(())
}

fn observed_service_override(
    inner: CallbackBasedGrpcService,
    observations: Arc<RpcObservations>,
) -> CallbackBasedGrpcService {
    CallbackBasedGrpcService {
        callback: Arc::new(move |request: GrpcRequest| {
            let callback = Arc::clone(&inner.callback);
            let observations = Arc::clone(&observations);
            Box::pin(async move {
                let mut guard = RpcObservationGuard::new(observations, request.rpc.clone());
                let result = callback(request).await;
                guard.complete(if result.is_ok() { "ok" } else { "error" });
                result
            })
        }),
    }
}

fn worker_options(
    task_queue: String,
    failure_announced: Arc<Notify>,
    release_provider_failure: Arc<Notify>,
    max_cached_workflows: usize,
) -> Result<WorkerOptions> {
    let worker_options = WorkerOptions::new(task_queue)
        .max_cached_workflows(max_cached_workflows)
        .register_workflow::<RetryPendingWorkflow>()?
        .register_activities(RetryFailureActivities {
            failure_announced,
            release_provider_failure,
        })
        .build();
    Ok(worker_options)
}

async fn run_on_host_runtime(
    client: Client,
    task_queue: String,
    workflow_id: String,
    failure_announced: Arc<Notify>,
    release_provider_failure: Arc<Notify>,
) -> Result<()> {
    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let worker_options = worker_options(
        task_queue.clone(),
        Arc::clone(&failure_announced),
        Arc::clone(&release_provider_failure),
        0,
    )?;
    let mut worker = Worker::new(&runtime, client.clone(), worker_options)?;
    let shutdown_worker = worker.shutdown_handle();

    let handle = client
        .start_workflow(
            RetryPendingWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start retry-pending workflow")?;
    let shutdown_at_failure_boundary = async {
        failure_announced.notified().await;
        shutdown_worker();
        release_provider_failure.notify_one();
        Result::<_, anyhow::Error>::Ok(())
    };
    let worker_run = worker.run();
    tokio::pin!(shutdown_at_failure_boundary);
    tokio::pin!(worker_run);

    tokio::select! {
        outcome = &mut shutdown_at_failure_boundary => outcome?,
        outcome = &mut worker_run => {
            return match outcome {
                Ok(()) => Err(anyhow!("worker stopped before the Activity returned its failure")),
                Err(error) => Err(error).context("SDK worker stopped before the failure boundary"),
            };
        }
    }

    worker_run
        .await
        .context("SDK worker should shut down cleanly with a retry pending")?;

    wait_for_retry_to_be_scheduled(&handle).await
}

async fn run_on_dedicated_current_thread(
    client: Client,
    task_queue: String,
    workflow_id: String,
    failure_announced: Arc<Notify>,
    release_provider_failure: Arc<Notify>,
) -> Result<()> {
    let worker_client = client.clone();
    let worker_queue = task_queue.clone();
    let worker_failure_announced = Arc::clone(&failure_announced);
    let worker_release_provider_failure = Arc::clone(&release_provider_failure);
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let worker_thread = std::thread::Builder::new()
        .name("sdk-v0-7-retry-shutdown-worker".to_owned())
        .spawn(move || -> Result<()> {
            let local = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("build the dedicated worker thread's Tokio runtime")?;
            local.block_on(async move {
                let runtime = Runtime::new_assume_tokio(Default::default())
                    .context("assemble SDK runtime on the dedicated worker thread")?;
                let options = worker_options(
                    worker_queue,
                    worker_failure_announced,
                    worker_release_provider_failure,
                    ODORI_MAX_CACHED_WORKFLOWS,
                )?;
                let mut worker = Worker::new(&runtime, worker_client, options)
                    .context("construct SDK worker on its dedicated thread")?;
                let shutdown: Box<dyn Fn() + Send + Sync> = Box::new(worker.shutdown_handle());
                if ready_sender.send(shutdown).is_err() {
                    // The host abandoned startup, so no workflow can be submitted.
                    return Ok(());
                }
                worker.run().await.context(
                    "SDK worker should shut down cleanly on its dedicated current-thread runtime",
                )
            })
        })
        .context("spawn the dedicated SDK worker thread")?;
    let shutdown_worker = ready_receiver
        .await
        .context("dedicated SDK worker ended before signalling readiness")?;

    let handle = match client
        .start_workflow(
            RetryPendingWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start retry-pending workflow")
    {
        Ok(handle) => handle,
        Err(error) => {
            shutdown_worker();
            join_worker_thread(worker_thread).await?;
            return Err(error);
        }
    };
    failure_announced.notified().await;
    shutdown_worker();
    release_provider_failure.notify_one();
    join_worker_thread(worker_thread).await?;

    wait_for_retry_to_be_scheduled(&handle).await
}

async fn join_worker_thread(worker_thread: std::thread::JoinHandle<Result<()>>) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        worker_thread
            .join()
            .map_err(|_| anyhow!("dedicated SDK worker thread panicked"))?
    })
    .await
    .context("join the dedicated SDK worker thread")??;
    Ok(())
}

async fn wait_for_retry_to_be_scheduled(
    handle: &temporalio_client::WorkflowHandle<Client, retry_pending_workflow::Run>,
) -> Result<()> {
    // A clean worker exit alone could hide a dropped completion. The pending
    // state proves the retryable failure crossed the SDK boundary and Tokeira
    // durably scheduled the replacement attempt during shutdown.
    loop {
        let description = handle
            .describe(WorkflowDescribeOptions::default())
            .await
            .context("describe retry-pending workflow")?;
        let retry_is_pending = description.raw().pending_activities.iter().any(|activity| {
            activity.last_failure.is_some()
                && activity.state == PendingActivityState::Scheduled as i32
        });
        if retry_is_pending {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}
