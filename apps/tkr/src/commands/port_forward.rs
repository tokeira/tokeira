//! `tkr port-forward <service>` — tunnel to a private ECS service via SSM,
//! or report host port mappings for compose/local platforms.
//!
//! For ECS deployments this command opens an SSM Session Manager tunnel from
//! the operator's workstation to the target service. Two SSM document types
//! are used depending on the service topology:
//!
//! - **Single-host services** (grafana, mimir, loki): each runs as a single
//!   task per dedicated host. We target the EC2 instance directly with
//!   `AWS-StartPortForwardingSession`.
//!
//! - **Replica services** (edge-api, edge-poll, controller): these run on
//!   shared capacity provider pools. We use
//!   `AWS-StartPortForwardingSessionToRemoteHost` with the Service Connect
//!   DNS name as the remote host.
//!
//! For compose/local platforms the command prints the host port mappings
//! (informational only — no active tunnel is needed).

use std::process::Stdio;

use anyhow::{Result, bail};
use tokio::process::Command as TokioCommand;
use tokio::signal;

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

use super::PlatformOps;

/// Service-to-default-port mapping for ECS port-forward.
fn default_port(service: &str) -> Option<u16> {
    match service {
        "grafana" => Some(3000),
        "edge-api" => Some(7233),
        "edge-poll" => Some(7234),
        "controller" => Some(7240),
        "mimir" => Some(9009),
        "loki" => Some(3100),
        _ => None,
    }
}

/// Services that run as a single task on a dedicated host and can be reached
/// by targeting the EC2 instance directly.
fn is_single_host_service(service: &str) -> bool {
    matches!(service, "grafana" | "mimir" | "loki")
}

/// Map a service name to its capacity provider name in the ECS cluster.
fn capacity_provider_name(service: &str, project_name: &str) -> String {
    let suffix = match service {
        "grafana" => "grafana",
        "mimir" => "mimir",
        "loki" => "loki",
        "edge-api" => "edge-api",
        "edge-poll" => "edge-poll",
        "controller" => "control",
        _ => service,
    };
    format!("{project_name}-cp-{suffix}")
}

/// Map a service name to its Service Connect DNS endpoint.
fn service_connect_host(service: &str, namespace: &str) -> String {
    let name = match service {
        "edge-api" => "edge-api",
        "edge-poll" => "edge-poll",
        "controller" => "controller",
        _ => service,
    };
    format!("{name}.{namespace}")
}

const VALID_PORT_FORWARD_SERVICES: &[&str] =
    &["grafana", "edge-api", "edge-poll", "controller", "mimir", "loki"];

pub async fn run(service: &str, local_port: Option<u16>, ctx: DeploymentContext) -> Result<()> {
    match &ctx.platform_config {
        PlatformDeploymentConfig::Ecs(config) => {
            run_ecs(service, local_port, config).await
        }
        _ => {
            // Compose/local: show port mappings (original behaviour).
            run_compose_local(service, &ctx).await
        }
    }
}

/// Original compose/local port-forward: prints host port mappings.
async fn run_compose_local(service: &str, ctx: &DeploymentContext) -> Result<()> {
    let ops = PlatformOps::from_context(ctx)?;
    let mappings = ops.port_mappings(service).await?;
    if mappings.is_empty() {
        println!("no port mappings for service {service}");
    } else {
        for mapping in mappings {
            println!(
                "{}:{} -> {}:{}/{}",
                mapping.host_addr,
                mapping.host_port,
                service,
                mapping.container_port,
                mapping.protocol
            );
        }
    }
    Ok(())
}

/// ECS port-forward: tunnel via SSM Session Manager.
pub async fn run_ecs(
    service: &str,
    local_port: Option<u16>,
    config: &tokeira_ecs_deployment::EcsConfig,
) -> Result<()> {
    // 1. Validate service name.
    if !VALID_PORT_FORWARD_SERVICES.contains(&service) {
        bail!(
            "unknown port-forward service '{service}'. valid services: {}",
            VALID_PORT_FORWARD_SERVICES.join(", ")
        );
    }

    // 2. Resolve the target port.
    let remote_port = default_port(service)
        .expect("validated service always has a default port");
    let local = local_port.unwrap_or(remote_port);

    // 3. Check session-manager-plugin is available.
    check_session_manager_plugin()?;

    // 4. Discover a container instance for the service's capacity provider.
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.region.clone()))
        .load()
        .await;
    let ecs_client = aws_sdk_ecs::Client::new(&aws_config);

    let cp_name = capacity_provider_name(service, &config.project_name);
    let instance_id = discover_instance(&ecs_client, &config.cluster.name, &cp_name).await?;

    // 5. Build and run the SSM session command.
    if is_single_host_service(service) {
        // Direct port-forward to the instance.
        run_ssm_port_forward(
            &config.region,
            &instance_id,
            local,
            remote_port,
            service,
        )
        .await
    } else {
        // Remote-host port-forward via Service Connect endpoint.
        let remote_host =
            service_connect_host(service, &config.cluster.service_connect_namespace);
        run_ssm_port_forward_remote_host(
            &config.region,
            &instance_id,
            &remote_host,
            local,
            remote_port,
            service,
        )
        .await
    }
}

/// Verify that `session-manager-plugin` is on PATH.
fn check_session_manager_plugin() -> Result<()> {
    if which::which("session-manager-plugin").is_err() {
        bail!(
            "session-manager-plugin is not installed or not on PATH.\n\
             Install it from: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html"
        );
    }
    Ok(())
}

/// Discover an EC2 instance ID from the ECS cluster filtered by capacity provider.
async fn discover_instance(
    ecs_client: &aws_sdk_ecs::Client,
    cluster: &str,
    capacity_provider: &str,
) -> Result<String> {
    // List container instances filtered by capacity provider status.
    let list_output = ecs_client
        .list_container_instances()
        .cluster(cluster)
        .status(aws_sdk_ecs::types::ContainerInstanceStatus::Active)
        .send()
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "ecs:ListContainerInstances failed: {}",
                err.into_service_error()
            )
        })?;

    let arns = list_output.container_instance_arns();
    if arns.is_empty() {
        bail!("no active container instances found in cluster '{cluster}'");
    }

    // Describe them to find one matching the capacity provider.
    let describe_output = ecs_client
        .describe_container_instances()
        .cluster(cluster)
        .set_container_instances(Some(arns.to_vec()))
        .send()
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "ecs:DescribeContainerInstances failed: {}",
                err.into_service_error()
            )
        })?;

    for ci in describe_output.container_instances() {
        if ci.capacity_provider_name().unwrap_or_default() == capacity_provider {
            if let Some(ec2_id) = ci.ec2_instance_id() {
                return Ok(ec2_id.to_owned());
            }
        }
    }

    bail!(
        "no container instance found for capacity provider '{capacity_provider}' in cluster '{cluster}'"
    );
}

/// Spawn `aws ssm start-session` with `AWS-StartPortForwardingSession`.
async fn run_ssm_port_forward(
    region: &str,
    instance_id: &str,
    local_port: u16,
    remote_port: u16,
    service: &str,
) -> Result<()> {
    println!(
        "forwarding localhost:{local_port} -> {service}:{remote_port} via instance {instance_id}"
    );

    let parameters = serde_json::json!({
        "portNumber": [remote_port.to_string()],
        "localPortNumber": [local_port.to_string()]
    });

    let mut child = TokioCommand::new("aws")
        .args([
            "ssm",
            "start-session",
            "--target",
            instance_id,
            "--document-name",
            "AWS-StartPortForwardingSession",
            "--parameters",
            &parameters.to_string(),
            "--region",
            region,
        ])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| anyhow::anyhow!("failed to spawn aws ssm start-session: {err}"))?;

    // Wait for the process or Ctrl-C.
    tokio::select! {
        status = child.wait() => {
            let status = status?;
            if !status.success() {
                bail!("ssm session exited with status {status}");
            }
            Ok(())
        }
        _ = signal::ctrl_c() => {
            // Send SIGTERM to the child so it cleans up the session.
            child.kill().await.ok();
            println!("\nsession terminated");
            Ok(())
        }
    }
}

/// Spawn `aws ssm start-session` with `AWS-StartPortForwardingSessionToRemoteHost`.
async fn run_ssm_port_forward_remote_host(
    region: &str,
    instance_id: &str,
    remote_host: &str,
    local_port: u16,
    remote_port: u16,
    service: &str,
) -> Result<()> {
    println!(
        "forwarding localhost:{local_port} -> {remote_host}:{remote_port} via instance {instance_id} (service: {service})"
    );

    let parameters = serde_json::json!({
        "host": [remote_host],
        "portNumber": [remote_port.to_string()],
        "localPortNumber": [local_port.to_string()]
    });

    let mut child = TokioCommand::new("aws")
        .args([
            "ssm",
            "start-session",
            "--target",
            instance_id,
            "--document-name",
            "AWS-StartPortForwardingSessionToRemoteHost",
            "--parameters",
            &parameters.to_string(),
            "--region",
            region,
        ])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| anyhow::anyhow!("failed to spawn aws ssm start-session: {err}"))?;

    // Wait for the process or Ctrl-C.
    tokio::select! {
        status = child.wait() => {
            let status = status?;
            if !status.success() {
                bail!("ssm session exited with status {status}");
            }
            Ok(())
        }
        _ = signal::ctrl_c() => {
            child.kill().await.ok();
            println!("\nsession terminated");
            Ok(())
        }
    }
}
