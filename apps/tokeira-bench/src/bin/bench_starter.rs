//! Bench starter: fires N EchoWorkflow executions against a local tokeirad and
//! reports latency + throughput.
//!
//! Each workflow's latency is measured from the moment the start request
//! leaves the client to the moment its result arrives. The starter caps the
//! number of in-flight workflows at `--concurrency` so we can dial load up or
//! down without overwhelming a single-node tokeirad.

// Bench harness: printed results are the product.
#![allow(clippy::print_stdout, clippy::print_stderr)]
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use tokeira_bench::{BENCH_TASK_QUEUE, EchoWorkflow};
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(
    name = "bench-starter",
    about = "Fires N EchoWorkflow executions at a local tokeirad and reports timings"
)]
struct Args {
    /// Total number of workflow executions to fire.
    #[arg(long, default_value_t = 100)]
    count: u32,

    /// Maximum number of workflows in flight at any one time.
    #[arg(long, default_value_t = 10)]
    concurrency: u32,

    /// Per-workflow deadline. If a workflow does not complete within this
    /// window, its latency is still recorded but the starter continues.
    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    per_workflow_timeout: Duration,

    /// Prefix for generated workflow IDs. The starter appends an index or
    /// a hex entropy token (depending on `--id-scheme`) so each execution
    /// has a unique ID.
    #[arg(long, default_value = "bench")]
    workflow_id_prefix: String,

    /// How workflow IDs are composed. `sequential` appends the bench index
    /// (`bench-0`, `bench-1`, …) — deterministic but can concentrate on a
    /// small number of hash buckets. `random` appends 16 hex chars derived
    /// from the system nanosecond clock plus the bench index, spreading
    /// IDs across the hash space to rule out bench-side concentration as a
    /// cause of throughput ceilings.
    #[arg(long, value_enum, default_value_t = IdScheme::Sequential)]
    id_scheme: IdScheme,

    /// Override the task queue name.
    #[arg(long, default_value = BENCH_TASK_QUEUE)]
    task_queue: String,

    /// Emit a machine-readable JSON summary on stdout in addition to the
    /// human-readable table.
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum IdScheme {
    /// `{prefix}-{index}` — deterministic, can concentrate on shards.
    Sequential,
    /// `{prefix}-{hex16}` — spread across the hash space.
    Random,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    // A tiny helper so `--per-workflow-timeout 30s` / `500ms` / `2m` work
    // without pulling in humantime.
    let s = s.trim();
    let (value, unit) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|idx| s.split_at(idx))
        .ok_or_else(|| format!("duration missing unit suffix: `{s}`"))?;
    let value: u64 = value
        .parse()
        .map_err(|e| format!("invalid duration number `{value}`: {e}"))?;
    let millis = match unit {
        "ms" => value,
        "s" => value.checked_mul(1_000).ok_or("duration overflow")?,
        "m" => value.checked_mul(60_000).ok_or("duration overflow")?,
        other => return Err(format!("unknown duration unit `{other}` (use ms, s, or m)")),
    };
    Ok(Duration::from_millis(millis))
}

#[derive(Debug, Default, serde::Serialize)]
struct Summary {
    target_count: u32,
    succeeded: u64,
    failed: u64,
    wall_clock_seconds: f64,
    throughput_per_second: f64,
    latency_ms_p50: u64,
    latency_ms_p95: u64,
    latency_ms_p99: u64,
    latency_ms_max: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if args.count == 0 {
        return Err("--count must be greater than zero".into());
    }
    if args.concurrency == 0 {
        return Err("--concurrency must be greater than zero".into());
    }

    // `ConfigError` and several other SDK errors are not `Send + Sync`, so
    // they cannot flow into `anyhow::Context`. The surrounding `?` still
    // surfaces them; we render a hint alongside so operators know what to
    // check when the load fails.
    let (conn_opts, client_opts) = ClientOptions::load_from_config(
        LoadClientConfigProfileOptions::default(),
    )
    .inspect_err(|_| {
        eprintln!(
            "hint: failed to load Temporal client config; set TEMPORAL_SERVICE_ADDRESS or create a temporal.toml"
        );
    })?;
    let connection = Connection::connect(conn_opts)
        .await
        .inspect_err(|_| {
            eprintln!(
                "hint: failed to connect to the Temporal server (is tokeirad running on localhost:7233?)"
            );
        })?;
    let client = Arc::new(Client::new(connection, client_opts)?);

    // HDR histogram in microseconds: range covers 1 μs up to 10 minutes per
    // workflow; 3 significant digits gives us reliable p99 readings without
    // blowing up memory.
    let mut histogram: Histogram<u64> = Histogram::new_with_bounds(1, 600_000_000, 3)
        .map_err(|e| format!("failed to build histogram: {e}"))?;

    let semaphore = Arc::new(Semaphore::new(args.concurrency as usize));
    let mut in_flight = FuturesUnordered::new();
    let mut succeeded: u64 = 0;
    let mut failed: u64 = 0;

    tracing::info!(
        count = args.count,
        concurrency = args.concurrency,
        task_queue = %args.task_queue,
        "bench starting"
    );
    let wall_start = Instant::now();

    for index in 0..args.count {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore open");
        let client = Arc::clone(&client);
        let workflow_id = match args.id_scheme {
            IdScheme::Sequential => format!("{}-{index}", args.workflow_id_prefix),
            IdScheme::Random => {
                // Blend the nanosecond clock with the iteration index so the
                // resulting hex token is unique across concurrent calls even
                // when the clock has coarse resolution. Two 64-bit halves
                // render as 16 hex chars — plenty of entropy for hash
                // spread without pulling in a UUID crate.
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                let salt = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17)
                    ^ (index as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                format!("{}-{salt:016x}", args.workflow_id_prefix)
            }
        };
        let task_queue = args.task_queue.clone();
        let timeout = args.per_workflow_timeout;

        in_flight.push(tokio::spawn(async move {
            // The permit stays alive until this future drops, which is when
            // the workflow completes — bounding concurrency naturally.
            let _permit = permit;
            let started = Instant::now();
            let outcome = run_one(&client, &workflow_id, &task_queue, timeout).await;
            (started.elapsed(), outcome)
        }));
    }

    while let Some(joined) = in_flight.next().await {
        match joined {
            Ok((elapsed, Ok(()))) => {
                let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
                // Saturating record ensures a single stall past the histogram
                // upper bound does not poison the rest of the run.
                histogram
                    .record(micros.min(histogram.high()))
                    .expect("histogram range already enforced");
                succeeded = succeeded.saturating_add(1);
            }
            Ok((_, Err(error))) => {
                failed = failed.saturating_add(1);
                tracing::warn!(?error, "workflow execution failed");
            }
            Err(join_error) => {
                failed = failed.saturating_add(1);
                tracing::warn!(?join_error, "starter task panicked");
            }
        }
    }

    let wall = wall_start.elapsed();
    let wall_seconds = wall.as_secs_f64();
    let throughput = if wall_seconds > 0.0 {
        succeeded as f64 / wall_seconds
    } else {
        0.0
    };

    let summary = Summary {
        target_count: args.count,
        succeeded,
        failed,
        wall_clock_seconds: wall_seconds,
        throughput_per_second: throughput,
        latency_ms_p50: histogram.value_at_quantile(0.5) / 1_000,
        latency_ms_p95: histogram.value_at_quantile(0.95) / 1_000,
        latency_ms_p99: histogram.value_at_quantile(0.99) / 1_000,
        latency_ms_max: histogram.max() / 1_000,
    };

    println!();
    println!("── bench results ─────────────────────────────");
    println!("target count         : {}", summary.target_count);
    println!("succeeded            : {}", summary.succeeded);
    println!("failed               : {}", summary.failed);
    println!("wall clock           : {:.3}s", summary.wall_clock_seconds);
    println!(
        "throughput           : {:.2} workflows/s",
        summary.throughput_per_second
    );
    println!("latency p50          : {} ms", summary.latency_ms_p50);
    println!("latency p95          : {} ms", summary.latency_ms_p95);
    println!("latency p99          : {} ms", summary.latency_ms_p99);
    println!("latency max          : {} ms", summary.latency_ms_max);
    println!("──────────────────────────────────────────────");

    if args.json {
        // Emit on a single line so consumers can grep `{` to find the JSON
        // even when logs are interleaved.
        let json = serde_json::to_string(&summary)?;
        println!("{json}");
    }

    if failed > 0 {
        return Err(format!("{failed} workflow execution(s) failed").into());
    }
    Ok(())
}

async fn run_one(
    client: &Client,
    workflow_id: &str,
    task_queue: &str,
    timeout: Duration,
) -> Result<()> {
    let handle = client
        .start_workflow(
            EchoWorkflow::run,
            // The input doubles as a correlation tag. Echoing it back lets us
            // sanity-check that a given workflow run completed with the right
            // payload.
            workflow_id.to_string(),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .with_context(|| format!("start_workflow({workflow_id}) failed"))?;

    let result_future = handle.get_result(WorkflowGetResultOptions::default());
    let result = tokio::time::timeout(timeout, result_future)
        .await
        .with_context(|| format!("workflow {workflow_id} did not complete within {timeout:?}"))?
        .with_context(|| format!("get_result({workflow_id}) failed"))?;

    // Integrity check: the EchoWorkflow returns its input verbatim.
    if result != workflow_id {
        return Err(anyhow!(
            "unexpected echo payload for {workflow_id}: got {result:?}"
        ));
    }
    Ok(())
}
