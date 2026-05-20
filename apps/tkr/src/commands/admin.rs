//! `tkr admin <subcommand>` — on-demand admin task execution.
//!
//! The `tokeira-admin` ECS service normally runs at 0 replicas. This
//! command scales it to 1, waits for the task to reach RUNNING, executes
//! the supplied subcommand via `ecs:ExecuteCommand`, streams output, and
//! scales back to 0.
//!
//! A drop guard ensures the scale-back-to-0 happens even on Ctrl-C. The
//! one exception is a startup timeout: if the task never reaches RUNNING,
//! we surface the `stoppedReason` but do NOT scale back — the operator
//! must investigate or re-invoke.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

/// Default timeout waiting for the admin task to reach RUNNING.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);

/// Polling interval when waiting for the admin service to stabilize.
const POLL_INTERVAL: Duration = Duration::from_secs(3);

/// The ECS service name for the admin service.
const ADMIN_SERVICE: &str = "tokeira-admin";

/// The short name used in the Ops layer.
const ADMIN_SHORT_NAME: &str = "admin";

pub async fn run(command: Vec<String>, ctx: DeploymentContext) -> Result<()> {
    let config = match &ctx.platform_config {
        PlatformDeploymentConfig::Ecs(config) => config.as_ref().clone(),
        _ => bail!("tkr admin is only supported on ECS deployments"),
    };

    if command.is_empty() {
        bail!("no admin subcommand provided. examples: schema setup, schema migrate 5, diagnostics runtime");
    }

    let full_command = command.join(" ");
    println!("admin: executing '{full_command}' on {ADMIN_SERVICE}");

    // Build AWS clients.
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.region.clone()))
        .load()
        .await;
    let ecs_client = aws_sdk_ecs::Client::new(&aws_config);

    let cluster = &config.cluster.name;

    // Check current state — if already running at 1, skip scale-up.
    let current_desired = describe_desired_count(&ecs_client, cluster, ADMIN_SERVICE).await?;

    let needs_scale_up = current_desired == 0;
    if needs_scale_up {
        println!("admin: scaling {ADMIN_SERVICE} from 0 to 1");
        update_desired_count(&ecs_client, cluster, ADMIN_SERVICE, 1).await?;
    } else {
        println!("admin: {ADMIN_SERVICE} already at desired_count={current_desired}");
    }

    // Use a guard to ensure we scale back to 0 on drop (Ctrl-C, early
    // return, panic). The guard is disarmed on the timeout failure path
    // where we intentionally leave the service up.
    let guard = ScaleDownGuard {
        client: ecs_client.clone(),
        cluster: cluster.clone(),
        service: ADMIN_SERVICE.to_owned(),
        armed: needs_scale_up,
    };

    // Wait for the task to reach RUNNING.
    let task_arn = match wait_for_running(&ecs_client, cluster, ADMIN_SERVICE, STARTUP_TIMEOUT)
        .await
    {
        Ok(arn) => arn,
        Err(err) => {
            // On timeout, fetch stoppedReason and do NOT scale back.
            guard.disarm();
            let reason = fetch_stopped_reason(&ecs_client, cluster, ADMIN_SERVICE).await;
            let msg = match reason {
                Some(r) => format!(
                    "admin task failed to reach RUNNING within {}s: {err}. stoppedReason: {r}",
                    STARTUP_TIMEOUT.as_secs()
                ),
                None => format!(
                    "admin task failed to reach RUNNING within {}s: {err}",
                    STARTUP_TIMEOUT.as_secs()
                ),
            };
            bail!("{msg}\n\nnote: service was NOT scaled back to 0. Re-run `tkr admin` to retry or investigate via `tkr exec admin`.");
        }
    };

    println!("admin: task {task_arn} is RUNNING, executing command");

    // Execute the command via ECS Exec, handing off to session-manager-plugin.
    let exec_result =
        execute_command(&ecs_client, cluster, &task_arn, &full_command, ADMIN_SHORT_NAME).await;

    // The guard will scale back to 0 on drop. We explicitly drop it here
    // so the scale-down happens before we propagate any exec error.
    drop(guard);

    exec_result
}

/// Describes the current desired count for an ECS service.
async fn describe_desired_count(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    service: &str,
) -> Result<u32> {
    let output = client
        .describe_services()
        .cluster(cluster)
        .services(service)
        .send()
        .await
        .context("ecs:DescribeServices failed")?;

    let svc = output
        .services()
        .first()
        .ok_or_else(|| anyhow::anyhow!("ECS service '{service}' not found in cluster '{cluster}'"))?;

    Ok(svc.desired_count().max(0) as u32)
}

/// Updates the desired count for an ECS service.
async fn update_desired_count(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    service: &str,
    desired: u32,
) -> Result<()> {
    client
        .update_service()
        .cluster(cluster)
        .service(service)
        .desired_count(desired as i32)
        .send()
        .await
        .context("ecs:UpdateService failed")?;
    Ok(())
}

/// Polls DescribeServices until runningCount == 1 and desiredCount == 1,
/// then returns the task ARN of the running task.
async fn wait_for_running(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    service: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out waiting for {service} to reach RUNNING");
        }

        let output = client
            .describe_services()
            .cluster(cluster)
            .services(service)
            .send()
            .await
            .context("ecs:DescribeServices poll failed")?;

        if let Some(svc) = output.services().first() {
            let desired = svc.desired_count();
            let running = svc.running_count();

            if desired == 1 && running == 1 {
                // Find the running task ARN.
                let task_arn = find_running_task(client, cluster, service).await?;
                return Ok(task_arn);
            }
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Lists tasks for the service and returns the first RUNNING task ARN.
async fn find_running_task(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    service: &str,
) -> Result<String> {
    let output = client
        .list_tasks()
        .cluster(cluster)
        .service_name(service)
        .desired_status(aws_sdk_ecs::types::DesiredStatus::Running)
        .send()
        .await
        .context("ecs:ListTasks failed")?;

    output
        .task_arns()
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no running tasks found for service '{service}'"))
}

/// Fetches the stoppedReason from the most recent task for the service.
async fn fetch_stopped_reason(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    service: &str,
) -> Option<String> {
    // List stopped tasks for the service.
    let list_output = client
        .list_tasks()
        .cluster(cluster)
        .service_name(service)
        .desired_status(aws_sdk_ecs::types::DesiredStatus::Stopped)
        .send()
        .await
        .ok()?;

    let task_arns = list_output.task_arns();
    if task_arns.is_empty() {
        return None;
    }

    // Describe the tasks to get stoppedReason.
    let describe_output = client
        .describe_tasks()
        .cluster(cluster)
        .set_tasks(Some(task_arns.to_vec()))
        .send()
        .await
        .ok()?;

    // Return the stoppedReason from the most recent task.
    describe_output
        .tasks()
        .iter()
        .filter_map(|t| t.stopped_reason().map(str::to_owned))
        .next()
}

/// Executes a command on the admin task via ECS Exec, handing off to
/// session-manager-plugin for interactive output.
async fn execute_command(
    client: &aws_sdk_ecs::Client,
    cluster: &str,
    task_arn: &str,
    command: &str,
    container: &str,
) -> Result<()> {
    let output = client
        .execute_command()
        .cluster(cluster)
        .task(task_arn)
        .container(container)
        .interactive(true)
        .command(command)
        .send()
        .await
        .context("ecs:ExecuteCommand failed")?;

    let session = output
        .session()
        .ok_or_else(|| anyhow::anyhow!("ecs:ExecuteCommand returned no session"))?;

    // session-manager-plugin expects the session payload as JSON on stdin
    // and the endpoint URL as an argument.
    let session_json = serde_json::json!({
        "SessionId": session.session_id().unwrap_or_default(),
        "StreamUrl": session.stream_url().unwrap_or_default(),
        "TokenValue": session.token_value().unwrap_or_default(),
    });

    let region = client.config().region().map(|r| r.as_ref()).unwrap_or("us-east-1");
    let endpoint = format!("https://ecs.{region}.amazonaws.com");

    // Check that session-manager-plugin is installed.
    if which::which("session-manager-plugin").is_err() {
        bail!(
            "session-manager-plugin is not installed.\n\
             Install it from: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html"
        );
    }

    let status = tokio::process::Command::new("session-manager-plugin")
        .arg(serde_json::to_string(&session_json)?)
        .arg(region)
        .arg("StartSession")
        .arg("")
        .arg(&endpoint)
        .arg(serde_json::to_string(&serde_json::json!({
            "Target": task_arn,
        }))?)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .await
        .context("failed to spawn session-manager-plugin")?;

    if !status.success() {
        bail!("session-manager-plugin exited with {status}");
    }

    Ok(())
}

/// Drop guard that scales the admin service back to 0 on drop.
///
/// This ensures cleanup happens even on Ctrl-C or early returns. The guard
/// can be disarmed (e.g. on timeout failure where we intentionally leave
/// the service up for investigation).
struct ScaleDownGuard {
    client: aws_sdk_ecs::Client,
    cluster: String,
    service: String,
    armed: bool,
}

impl ScaleDownGuard {
    /// Disarm the guard so it does NOT scale down on drop.
    fn disarm(mut self) {
        self.armed = false;
        // Prevent Drop from running the scale-down.
        std::mem::forget(self);
    }
}

impl Drop for ScaleDownGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        println!("admin: scaling {ADMIN_SERVICE} back to 0");
        // We're in a Drop impl so we can't use async directly. Spawn a
        // blocking task on the tokio runtime to perform the scale-down.
        let client = self.client.clone();
        let cluster = self.cluster.clone();
        let service = self.service.clone();
        // Use spawn_blocking + block_on to ensure the scale-down completes
        // even during shutdown (Ctrl-C). tokio::spawn would be cancelled.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            if let Ok(rt) = rt {
                let _ = rt.block_on(async {
                    let _ = client
                        .update_service()
                        .cluster(&cluster)
                        .service(&service)
                        .desired_count(0)
                        .send()
                        .await;
                });
            }
        })
        .join()
        .ok();
    }
}
