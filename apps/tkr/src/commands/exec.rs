//! `tkr exec <service> [--container <name>] -- <cmd>...` — interactive
//! container access via ECS Exec.
//!
//! Finds a running task for the given service, calls `ecs:ExecuteCommand`
//! with `interactive = true`, and hands the returned session payload off
//! to `session-manager-plugin` for the interactive terminal session.

use anyhow::{Context, Result, bail};
use tokeira_ecs_deployment::EcsConfig;

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

/// Default container name for each ECS service.
///
/// When `--container` is omitted, we resolve to the primary application
/// container. We never default to the Alloy sidecar.
fn default_container_name(service: &str) -> &'static str {
    match service {
        "edge-api" => "tokeira-edge-api",
        "edge-poll" => "tokeira-edge-poll",
        "runtime" => "tokeira-runtime",
        "projection" => "tokeira-projection",
        "controller" => "tokeira-controller",
        "autoscaler" => "tokeira-autoscaler",
        "admin" => "tokeira-admin",
        "mimir" => "tokeira-mimir",
        "loki" => "tokeira-loki",
        "grafana" => "tokeira-grafana",
        _ => "unknown",
    }
}

/// ECS service name (prefixed with `tokeira-`) for a given short service name.
fn ecs_service_name(service: &str) -> String {
    match service {
        "edge-api" => "tokeira-edge-api".into(),
        "edge-poll" => "tokeira-edge-poll".into(),
        "runtime" => "tokeira-runtime".into(),
        "projection" => "tokeira-projection".into(),
        "controller" => "tokeira-controller".into(),
        "autoscaler" => "tokeira-autoscaler".into(),
        "admin" => "tokeira-admin".into(),
        "mimir" => "tokeira-mimir".into(),
        "loki" => "tokeira-loki".into(),
        "grafana" => "tokeira-grafana".into(),
        other => format!("tokeira-{other}"),
    }
}

const VALID_SERVICES: &[&str] = &[
    "edge-api",
    "edge-poll",
    "runtime",
    "projection",
    "controller",
    "autoscaler",
    "admin",
    "mimir",
    "loki",
    "grafana",
];

pub async fn run(
    service: &str,
    container: Option<&str>,
    cmd: &[String],
    ctx: DeploymentContext,
) -> Result<()> {
    let config = match &ctx.platform_config {
        PlatformDeploymentConfig::Ecs(config) => config.as_ref().clone(),
        _ => bail!("tkr exec is only supported on ECS deployments"),
    };

    run_ecs(service, container, cmd, &config).await
}

pub async fn run_ecs(
    service: &str,
    container: Option<&str>,
    cmd: &[String],
    config: &EcsConfig,
) -> Result<()> {
    // Validate service name.
    if !VALID_SERVICES.contains(&service) {
        bail!(
            "unknown service '{service}'. valid services: {}",
            VALID_SERVICES.join(", ")
        );
    }

    // Require session-manager-plugin on PATH.
    if which::which("session-manager-plugin").is_err() {
        bail!(
            "session-manager-plugin is not installed or not on PATH.\n\n\
             Install it from: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html\n\n\
             On macOS: brew install --cask session-manager-plugin\n\
             On Linux: see the AWS documentation for your distribution."
        );
    }

    // Resolve container name.
    let container_name = container.unwrap_or_else(|| default_container_name(service));

    // Build the command string for ECS Exec.
    if cmd.is_empty() {
        bail!("no command specified. usage: tkr exec <service> -- <cmd>...");
    }
    let command_str = cmd.join(" ");

    // Initialize AWS clients.
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.region.clone()))
        .load()
        .await;
    let ecs_client = aws_sdk_ecs::Client::new(&aws_config);

    // Find a running task for the service.
    let cluster = &config.cluster.name;
    let ecs_service = ecs_service_name(service);

    println!("finding running task for {ecs_service}...");

    let list_output = ecs_client
        .list_tasks()
        .cluster(cluster)
        .service_name(&ecs_service)
        .desired_status(aws_sdk_ecs::types::DesiredStatus::Running)
        .send()
        .await
        .context("ecs:ListTasks failed")?;

    let task_arn = list_output
        .task_arns()
        .first()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no running tasks found for service '{ecs_service}' in cluster '{cluster}'.\n\
                 Is the service scaled to at least 1 replica? Try: tkr scale up {service} 1"
            )
        })?
        .clone();

    println!("executing command in container '{container_name}' on task {task_arn}");

    // Call ecs:ExecuteCommand.
    let exec_output = ecs_client
        .execute_command()
        .cluster(cluster)
        .task(&task_arn)
        .container(container_name)
        .interactive(true)
        .command(&command_str)
        .send()
        .await;

    let exec_output = match exec_output {
        Ok(output) => output,
        Err(err) => {
            let err_str = format!("{err}");
            if err_str.contains("ssmmessages") || err_str.contains("TargetNotConnectedException")
            {
                bail!(
                    "ECS Exec failed — the task role is likely missing ssmmessages permissions.\n\n\
                     Remediation:\n\
                     1. Ensure the task role has the following policy attached:\n\
                        - Action: ssmmessages:CreateControlChannel, ssmmessages:CreateDataChannel,\n\
                          ssmmessages:OpenControlChannel, ssmmessages:OpenDataChannel\n\
                        - Resource: *\n\
                     2. Ensure the ECS service has `enable_execute_command = true`\n\
                     3. Ensure the VPC has an ssmmessages endpoint or NAT gateway\n\
                     4. After fixing, force a new deployment: tkr deploy apply --yes --force\n\n\
                     Underlying error: {err}"
                );
            }
            return Err(err).context("ecs:ExecuteCommand failed");
        }
    };

    // Extract the session payload and hand off to session-manager-plugin.
    let session = exec_output
        .session()
        .ok_or_else(|| anyhow::anyhow!("ecs:ExecuteCommand returned no session payload"))?;

    let session_id = session.session_id().unwrap_or("unknown");
    let stream_url = session.stream_url().unwrap_or("");
    let token_value = session.token_value().unwrap_or("");

    // session-manager-plugin expects the session payload as a JSON argument.
    let session_json = serde_json::json!({
        "SessionId": session_id,
        "StreamUrl": stream_url,
        "TokenValue": token_value,
    });

    // Derive the SSM endpoint for the region.
    let endpoint = format!("https://ssm.{}.amazonaws.com", config.region);

    // Spawn session-manager-plugin with the session payload.
    let status = tokio::process::Command::new("session-manager-plugin")
        .arg(serde_json::to_string(&session_json)?)
        .arg(&config.region)
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
