//! Bench worker: polls `tokeira-bench` and executes `EchoWorkflow`.
//!
//! Run this in one terminal while `bench-starter` runs in another. Both
//! binaries pick up the service address from the standard SDK config chain
//! (`TEMPORAL_SERVICE_ADDRESS` env var, `~/.config/temporalio/temporal.toml`,
//! or defaults to `http://localhost:7233`).

use clap::Parser;
use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};
use tokeira_bench::{BENCH_TASK_QUEUE, EchoWorkflow};

#[derive(Parser)]
#[command(
    name = "bench-worker",
    about = "Polls a local tokeirad for bench workflows"
)]
struct Args {
    /// Override the task queue name. Defaults to the shared `BENCH_TASK_QUEUE`.
    #[arg(long, default_value = BENCH_TASK_QUEUE)]
    task_queue: String,
}

// `Box<dyn std::error::Error>` matches the signature every SDK example uses.
// Several SDK error types are not `Send + Sync`, so they cannot flow into
// `anyhow::Error` via `?`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // CoreRuntime owns the tokio runtime used by the SDK's I/O threads.
    // `new_assume_tokio` reuses the ambient tokio runtime that `#[tokio::main]`
    // set up, which is what every SDK example in sdk-core does for simple
    // workers.
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()?,
    )?;

    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new(&args.task_queue)
        .register_workflow::<EchoWorkflow>()
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    tracing::info!(task_queue = %args.task_queue, "bench worker started");
    worker.run().await?;

    Ok(())
}
