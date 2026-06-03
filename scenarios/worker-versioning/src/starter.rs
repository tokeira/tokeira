//! Driver for the worker-versioning scenario.
//!
//! Assumes the two versioned workers are already running (start them first — a worker
//! registers its `(deployment, build_id)` with the server by polling, which is how the
//! deployment and its versions come into existence; there is no explicit
//! create-version RPC in this SDK). The starter then drives the routing flow and
//! asserts the observable outcome at each stage:
//!
//!   1. wait for both versions (`v1`, `v2`) to register (Describe shows them)
//!   2. set `v1` current; new auto-upgrade traffic runs on `v1`
//!   3. ramp `v2` to 50%; new auto-upgrade traffic splits across `v1`/`v2`
//!   4. promote `v2` to current; new auto-upgrade traffic runs on `v2`
//!
//! Each workflow returns the build id of the worker that executed it (via the probe
//! activity), so the split is directly observable. The starter exits non-zero if an
//! observed outcome does not match the expectation.

mod config;
mod workflows;

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use anyhow::{Result, anyhow};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, NamespacedClient,
    WorkflowGetResultOptions, WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::protos::temporal::api::workflowservice::v1::{
    DescribeWorkerDeploymentRequest, SetWorkerDeploymentCurrentVersionRequest,
    SetWorkerDeploymentRampingVersionRequest,
};
use temporalio_sdk_core::Url;
use tonic::IntoRequest;

use config::{BUILD_V1, BUILD_V2, ScenarioConfig};
use workflows::VersionProbeWorkflow;

/// How many workflows to start per routing stage (enough to observe a ~50% split).
const SAMPLES_PER_STAGE: usize = 20;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = ScenarioConfig::from_env();
    let url = Url::from_str(&cfg.address)?;
    let connection = Connection::connect(ConnectionOptions::new(url).build()).await?;
    let client = Client::new(connection, ClientOptions::new(&cfg.namespace).build())?;

    println!(
        "scenario start: deployment={} task_queue={} versions=[{BUILD_V1}, {BUILD_V2}]",
        cfg.deployment, cfg.task_queue
    );

    // Step 1 — both versions must have registered by polling before we can route.
    let conflict_token = wait_for_versions(&client, &cfg).await?;

    // Step 2 — make v1 current; auto-upgrade traffic should all land on v1.
    let token = set_current(&client, &cfg, BUILD_V1, conflict_token).await?;
    let dist = run_samples(&client, &cfg, "current=v1").await?;
    assert_all_on(&dist, BUILD_V1, "after setting v1 current")?;

    // Step 3 — ramp v2 to 50%; auto-upgrade traffic should split across v1 and v2.
    let token = set_ramping(&client, &cfg, BUILD_V2, 50.0, token).await?;
    let dist = run_samples(&client, &cfg, "ramp v2@50%").await?;
    assert_split(&dist, BUILD_V1, BUILD_V2, "after ramping v2 to 50%")?;

    // Step 4 — promote v2 to current (auto-unsets ramping); traffic should land on v2.
    let _token = set_current(&client, &cfg, BUILD_V2, token).await?;
    let dist = run_samples(&client, &cfg, "current=v2").await?;
    assert_all_on(&dist, BUILD_V2, "after promoting v2 to current")?;

    println!("scenario passed: routing matched expectations at every stage");
    Ok(())
}

/// Poll `DescribeWorkerDeployment` until both versions have registered, returning the
/// deployment's current conflict token. Versions register when their workers poll, so
/// this also gates on the workers being up.
async fn wait_for_versions(client: &Client, cfg: &ScenarioConfig) -> Result<Vec<u8>> {
    let deadline = Duration::from_secs(30);
    let started = std::time::Instant::now();
    loop {
        let resp = client
            .connection()
            .clone()
            .describe_worker_deployment(
                DescribeWorkerDeploymentRequest {
                    namespace: client.namespace(),
                    deployment_name: cfg.deployment.clone(),
                }
                .into_request(),
            )
            .await;

        if let Ok(resp) = resp {
            let resp = resp.into_inner();
            let known: Vec<String> = resp
                .worker_deployment_info
                .as_ref()
                .map(|info| {
                    info.version_summaries
                        .iter()
                        .filter_map(|s| s.deployment_version.as_ref().map(|v| v.build_id.clone()))
                        .collect()
                })
                .unwrap_or_default();
            if known.iter().any(|b| b == BUILD_V1) && known.iter().any(|b| b == BUILD_V2) {
                println!("[1] both versions registered: {known:?}");
                return Ok(resp.conflict_token);
            }
        }

        if started.elapsed() > deadline {
            return Err(anyhow!(
                "timed out waiting for versions {BUILD_V1} and {BUILD_V2} to register; \
                 are both workers running?"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Set the deployment's current version; returns the new conflict token.
async fn set_current(
    client: &Client,
    cfg: &ScenarioConfig,
    build_id: &str,
    conflict_token: Vec<u8>,
) -> Result<Vec<u8>> {
    #[allow(deprecated)]
    let req = SetWorkerDeploymentCurrentVersionRequest {
        namespace: client.namespace(),
        deployment_name: cfg.deployment.clone(),
        build_id: build_id.to_owned(),
        conflict_token,
        identity: "worker-versioning-scenario".to_owned(),
        ..Default::default()
    };
    let resp = client
        .connection()
        .clone()
        .set_worker_deployment_current_version(req.into_request())
        .await?
        .into_inner();
    println!("[set] current version = {build_id}");
    Ok(resp.conflict_token)
}

/// Set the deployment's ramping version + percentage; returns the new conflict token.
async fn set_ramping(
    client: &Client,
    cfg: &ScenarioConfig,
    build_id: &str,
    percentage: f32,
    conflict_token: Vec<u8>,
) -> Result<Vec<u8>> {
    #[allow(deprecated)]
    let req = SetWorkerDeploymentRampingVersionRequest {
        namespace: client.namespace(),
        deployment_name: cfg.deployment.clone(),
        build_id: build_id.to_owned(),
        percentage,
        conflict_token,
        identity: "worker-versioning-scenario".to_owned(),
        ..Default::default()
    };
    let resp = client
        .connection()
        .clone()
        .set_worker_deployment_ramping_version(req.into_request())
        .await?
        .into_inner();
    println!("[set] ramping version = {build_id} @ {percentage}%");
    Ok(resp.conflict_token)
}

/// Start `SAMPLES_PER_STAGE` auto-upgrade workflows and tally which build executed each.
async fn run_samples(
    client: &Client,
    cfg: &ScenarioConfig,
    stage: &str,
) -> Result<BTreeMap<String, usize>> {
    let mut dist: BTreeMap<String, usize> = BTreeMap::new();
    for i in 0..SAMPLES_PER_STAGE {
        let workflow_id = format!("wv-{}-{}-{i}", cfg.deployment, stage_slug(stage));
        let handle = client
            .start_workflow(
                VersionProbeWorkflow::run,
                (),
                WorkflowStartOptions::new(cfg.task_queue.clone(), workflow_id).build(),
            )
            .await?;
        let build_id = handle
            .get_result(WorkflowGetResultOptions::default())
            .await?;
        *dist.entry(build_id).or_default() += 1;
    }
    println!("[run] {stage}: {dist:?}");
    Ok(dist)
}

/// Assert every sampled run executed on `build_id`.
fn assert_all_on(dist: &BTreeMap<String, usize>, build_id: &str, ctx: &str) -> Result<()> {
    let on_target = dist.get(build_id).copied().unwrap_or(0);
    let total: usize = dist.values().sum();
    if on_target == total && total > 0 {
        Ok(())
    } else {
        Err(anyhow!(
            "{ctx}: expected all {total} runs on {build_id}, got {dist:?}"
        ))
    }
}

/// Assert the sampled runs split across both builds (neither got everything). The ramp
/// split is probabilistic, so this only checks both versions received some traffic.
fn assert_split(
    dist: &BTreeMap<String, usize>,
    build_a: &str,
    build_b: &str,
    ctx: &str,
) -> Result<()> {
    let a = dist.get(build_a).copied().unwrap_or(0);
    let b = dist.get(build_b).copied().unwrap_or(0);
    if a > 0 && b > 0 {
        Ok(())
    } else {
        Err(anyhow!(
            "{ctx}: expected traffic on both {build_a} and {build_b}, got {dist:?}"
        ))
    }
}

/// Compress a stage label into a workflow-id-safe slug.
fn stage_slug(stage: &str) -> String {
    stage
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}
