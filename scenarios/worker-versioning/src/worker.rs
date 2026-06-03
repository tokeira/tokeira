//! Versioned worker for the worker-versioning scenario.
//!
//! Run one instance per deployment version. Each worker advertises a
//! `(deployment_name, build_id)` and a default versioning behaviour, then polls the
//! shared task queue. Tokeira routes runs to a version based on the deployment's
//! current/ramping config and each run's versioning behaviour; this worker only
//! executes whatever it is handed and reports its own build id back through the
//! activity, which is how the starter observes the routing decision.
//!
//! Usage:
//!   worker --build-id <id> [--behavior pinned|auto-upgrade] [--task-queue <q>]
//!          [--deployment <name>]
//! Environment overrides: TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE.

mod config;
mod workflows;

use std::str::FromStr;

use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::{
    protos::temporal::api::enums::v1::VersioningBehavior,
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion},
};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use config::ScenarioConfig;
use workflows::{VersionActivities, VersionProbeWorkflow};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = ScenarioConfig::from_env();
    let args = WorkerArgs::parse()?;

    let runtime = CoreRuntime::new_assume_tokio(RuntimeOptions::builder().build()?)?;
    let url = Url::from_str(&cfg.address)?;
    let connection = Connection::connect(ConnectionOptions::new(url).build()).await?;
    let client = Client::new(connection, ClientOptions::new(&cfg.namespace).build())?;

    // Opt the worker into Worker Deployment Versioning under (deployment, build_id),
    // with the per-version default behaviour. The high-level SDK applies this default
    // to every workflow task this worker completes.
    let deployment_options = WorkerDeploymentOptions {
        version: WorkerDeploymentVersion {
            deployment_name: cfg.deployment.clone(),
            build_id: args.build_id.clone(),
        },
        use_worker_versioning: true,
        default_versioning_behavior: Some(args.behavior),
    };

    let worker_options = WorkerOptions::new(&cfg.task_queue)
        .deployment_options(deployment_options)
        .register_workflow::<VersionProbeWorkflow>()
        .register_activities(VersionActivities {
            build_id: args.build_id.clone(),
        })
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    println!(
        "worker up: deployment={} build_id={} behavior={:?} task_queue={}",
        cfg.deployment, args.build_id, args.behavior, cfg.task_queue
    );
    worker.run().await?;
    Ok(())
}

/// Parsed worker CLI arguments.
struct WorkerArgs {
    build_id: String,
    behavior: VersioningBehavior,
}

impl WorkerArgs {
    fn parse() -> anyhow::Result<Self> {
        let mut build_id: Option<String> = None;
        let mut behavior = VersioningBehavior::AutoUpgrade;
        let mut args = std::env::args().skip(1);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--build-id" => {
                    build_id = Some(
                        args.next()
                            .ok_or_else(|| anyhow::anyhow!("--build-id requires a value"))?,
                    );
                }
                "--behavior" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--behavior requires a value"))?;
                    behavior = parse_behavior(&value)?;
                }
                // --task-queue / --deployment are read from the environment via
                // ScenarioConfig; accept and skip them here so a single arg vector can
                // drive both the worker and the starter.
                "--task-queue" | "--deployment" => {
                    let _ = args.next();
                }
                other => return Err(anyhow::anyhow!("unknown worker argument: {other}")),
            }
        }
        Ok(Self {
            build_id: build_id.ok_or_else(|| anyhow::anyhow!("--build-id is required"))?,
            behavior,
        })
    }
}

/// Map the CLI behaviour string to the SDK enum.
fn parse_behavior(value: &str) -> anyhow::Result<VersioningBehavior> {
    match value {
        "pinned" => Ok(VersioningBehavior::Pinned),
        "auto-upgrade" | "auto_upgrade" => Ok(VersioningBehavior::AutoUpgrade),
        other => Err(anyhow::anyhow!(
            "unknown behavior '{other}' (expected 'pinned' or 'auto-upgrade')"
        )),
    }
}
