//! Temporal Rust SDK 0.8.0 workflow and activity against embedded Tokeira.

use std::{net::TcpListener, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tokeira_engine::{Engine, TokeiraConfig};

const EMBEDDED_URL: &str = "http://tokeira-engine.invalid:7233";
const TASK_QUEUE: &str = "sdk-v0-7-embedded-spike";
const WORKFLOW_ID: &str = "sdk-v0-7-embedded-greeting";
const EXPECTED_RESULT: &str = "Hello, embedded Tokeira!";
const SPIKE_TIMEOUT: Duration = Duration::from_secs(30);

#[workflow]
#[derive(Default)]
/// Typed workflow registered with the SDK worker for the embedded round trip.
pub struct EmbeddedGreetingWorkflow;

#[workflow_methods]
impl EmbeddedGreetingWorkflow {
    #[run]
    /// Executes one activity and returns its greeting unchanged.
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx
            .execute_activity(
                GreetingActivities::greet,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;
        Ok(greeting)
    }
}

/// Activity collection registered alongside [`EmbeddedGreetingWorkflow`].
pub struct GreetingActivities;

#[activities]
impl GreetingActivities {
    #[activity]
    /// Produces the value asserted after the workflow result crosses the SDK boundary.
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(SPIKE_TIMEOUT, run())
        .await
        .context("embedded SDK spike exceeded 30 seconds")??;
    Ok(())
}

async fn run() -> Result<()> {
    // Occupying both configured listener addresses makes an accidental TCP
    // fallback fail deterministically with AddressInUse during engine startup.
    let grpc_guard = TcpListener::bind("127.0.0.1:0").context("reserve gRPC sentinel port")?;
    let nexus_guard = TcpListener::bind("127.0.0.1:0").context("reserve Nexus sentinel port")?;
    let mut config = TokeiraConfig::default();
    config.infrastructure.network.grpc_addr = grpc_guard.local_addr()?.to_string();
    config.policy.nexus_completion.http_addr = nexus_guard.local_addr()?.to_string();

    let engine = Engine::start_with_config(config)
        .await
        .context("start zero-listener Tokeira engine")?;
    let connection_options = ConnectionOptions::new(Url::parse(EMBEDDED_URL)?)
        .service_override(engine.service_override())
        .dns_load_balancing(None)
        .build();
    let connection = Connection::connect(connection_options)
        .await
        .context("connect Temporal Rust SDK 0.8.0 through service_override")?;
    if connection.capabilities().is_none() {
        bail!("embedded GetSystemInfo response omitted server capabilities");
    }
    let client = Client::new(connection, ClientOptions::new("default".to_owned()).build())?;

    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let worker_options = WorkerOptions::new(TASK_QUEUE)
        .max_cached_workflows(0)
        .register_workflow::<EmbeddedGreetingWorkflow>()?
        .register_activities(GreetingActivities)
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), worker_options)?;
    let shutdown_worker = worker.shutdown_handle();

    let workflow_call = async {
        let handle = client
            .start_workflow(
                EmbeddedGreetingWorkflow::run,
                "embedded Tokeira".to_owned(),
                WorkflowStartOptions::new(TASK_QUEUE, WORKFLOW_ID).build(),
            )
            .await
            .context("start greeting workflow")?;
        let run_id = handle
            .run_id()
            .context("workflow handle omitted its run id")?
            .to_owned();
        let result = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .context("await greeting workflow result")?;
        Result::<_, anyhow::Error>::Ok((run_id, result))
    };
    let worker_run = worker.run();
    tokio::pin!(workflow_call);
    tokio::pin!(worker_run);

    let workflow_outcome = tokio::select! {
        outcome = &mut workflow_call => outcome,
        outcome = &mut worker_run => {
            return match outcome {
                Ok(()) => Err(anyhow!("worker stopped before the workflow completed")),
                Err(error) => Err(error).context("SDK worker stopped before workflow completion"),
            };
        }
    };
    shutdown_worker();
    worker_run.await.context("shut down SDK worker")?;

    let (run_id, result) = workflow_outcome?;
    if result != EXPECTED_RESULT {
        bail!("unexpected workflow result: {result:?}");
    }
    println!("Temporal Rust SDK: 0.8.0");
    println!("Transport: temporalio-client::service_override (no TCP listener)");
    println!("Workflow run_id: {run_id}");
    println!("Workflow result: {result}");

    engine.shutdown().await?;
    Ok(())
}
