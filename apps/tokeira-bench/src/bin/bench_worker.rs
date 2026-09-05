//! Bench worker: polls `tokeira-bench` and executes `EchoWorkflow`.
//!
//! Run this in one terminal while `bench-starter` runs in another. Both
//! binaries pick up the service address from the standard SDK config chain
//! (`TEMPORAL_SERVICE_ADDRESS` env var, `~/.config/temporalio/temporal.toml`,
//! or defaults to `http://localhost:7233`).

// Bench harness: printed results are the product.
#![allow(clippy::print_stdout, clippy::print_stderr)]
use clap::Parser;
use temporalio_client::{
    Client, ClientOptions, Connection, envconfig::LoadClientConfigProfileOptions,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions, runtime::PollerBehavior};
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

    // The SDK runtime owns the I/O threads behind the worker. Building it from
    // the ambient Tokio runtime reuses what `#[tokio::main]` set up, which is
    // what every SDK example does for a simple worker.
    let runtime = Runtime::from_current_tokio(Default::default())?;

    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

    let worker_options = WorkerOptions::new(&args.task_queue)
        .max_cached_workflows(2000)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(50))
        .nonsticky_to_sticky_poll_ratio(0.1)
        .register_workflow::<EchoWorkflow>()?
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    tracing::info!(task_queue = %args.task_queue, "bench worker started");
    worker.run().await?;

    Ok(())
}
