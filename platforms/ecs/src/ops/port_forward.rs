//! Session Manager tunnels for private ECS services.
//!
//! The operation derives routing coordinates from the admitted definition,
//! selects a running task and its actual container instance from ECS, then
//! gives the interactive session lifecycle to the operator's AWS CLI. AWS
//! credentials and Session Manager tokens never cross the platform API.
//! The child inherits the terminal and is always reaped on exit or Ctrl-C.

use std::{
    env,
    ffi::OsStr,
    future::Future,
    io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
};

use anyhow::{Context as _, Result, bail};
use tokeira_aws::AwsClients;
use tokeira_platform::declaration::{DeploymentRef, PortForwardOutcome};

use super::{
    EcsOperationCoordinates, ForwardTarget, SERVICES, Service, lookup_service, provider_error,
    running_tasks, task_private_ip,
};

// The direct SSM document connects to the managed EC2 node's loopback
// interface. ECS definitions uniformly use `awsvpc`, where the application
// port belongs to the task ENI instead, so both topology modes must use the
// remote-host document. Dedicated workloads target that ENI directly;
// replicated workloads retain their stable Service Connect endpoint.
const SSM_REMOTE_HOST_DOCUMENT: &str = "AWS-StartPortForwardingSessionToRemoteHost";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskTunnelTarget {
    container_instance_arn: String,
    private_ip: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionCommand {
    program: PathBuf,
    args: Vec<String>,
}

pub(super) async fn run(
    deployment: &DeploymentRef,
    service: &str,
    local_port: Option<u16>,
) -> Result<PortForwardOutcome> {
    let service = lookup_forward_service(service)?;
    let local_port = local_port.unwrap_or(service.port);
    if local_port == 0 {
        bail!("local port must be between 1 and 65535");
    }

    // Resolve both tools before making a provider call. The AWS CLI owns
    // Session Manager's plugin protocol; keeping it as the subprocess
    // boundary means credentials remain in the operator's normal AWS chain
    // and no session token is exposed through this API.
    let aws_cli = required_executable("aws", "AWS CLI")?;
    required_executable("session-manager-plugin", "Session Manager plugin")?;

    let coordinates = EcsOperationCoordinates::read(deployment)?;
    let clients = AwsClients::load(Some(coordinates.region())).await;
    let target = discover_task_tunnel_target(&clients, &coordinates, service).await?;
    let remote_host = match service
        .forward_target
        .expect("port-forward service has a target mode")
    {
        ForwardTarget::TaskAddress => target.private_ip,
        ForwardTarget::ServiceConnect => format!(
            "{}.{}",
            service.ecs_name,
            coordinates.service_connect_namespace()
        ),
    };
    let instance_id = discover_ec2_instance(
        &clients,
        &coordinates,
        service,
        &target.container_instance_arn,
    )
    .await?;
    let command = ssm_session_command(
        aws_cli,
        coordinates.region(),
        &instance_id,
        &remote_host,
        local_port,
        service.port,
    )?;
    run_session(command).await?;
    Ok(PortForwardOutcome::SessionClosed)
}

fn lookup_forward_service(name: &str) -> Result<Service> {
    let service = lookup_service(name)?;
    if service.forward_target.is_none() {
        bail!(
            "ECS service `{name}` does not expose an operator port-forward; valid services are: {}",
            SERVICES
                .iter()
                .filter(|service| service.forward_target.is_some())
                .map(|service| service.operator_name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(service)
}

async fn discover_task_tunnel_target(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: Service,
) -> Result<TaskTunnelTarget> {
    let task_arns = running_tasks(clients, coordinates, service).await?;
    let output = clients
        .ecs
        .describe_tasks()
        .cluster(coordinates.cluster())
        .set_tasks(Some(task_arns))
        .send()
        .await
        .map_err(|error| provider_error(coordinates, "ecs:DescribeTasks", error))?;
    select_task_tunnel_target(output.tasks(), service)
}

fn select_task_tunnel_target(
    tasks: &[aws_sdk_ecs::types::Task],
    service: Service,
) -> Result<TaskTunnelTarget> {
    let mut tasks = tasks.iter().collect::<Vec<_>>();
    tasks.sort_by_key(|task| task.task_arn().unwrap_or_default());
    tasks
        .into_iter()
        .filter(|task| task.last_status() == Some("RUNNING"))
        .find_map(|task| {
            Some(TaskTunnelTarget {
                container_instance_arn: task.container_instance_arn()?.to_owned(),
                // The application port belongs to this ENI, not the hosting
                // EC2 instance's loopback interface.
                private_ip: task_private_ip(task)?,
            })
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ECS service `{}` has no running EC2 task with an awsvpc private IPv4 address",
                service.ecs_name
            )
        })
}

async fn discover_ec2_instance(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: Service,
    container_instance_arn: &str,
) -> Result<String> {
    let output = clients
        .ecs
        .describe_container_instances()
        .cluster(coordinates.cluster())
        .container_instances(container_instance_arn)
        .send()
        .await
        .map_err(|error| provider_error(coordinates, "ecs:DescribeContainerInstances", error))?;
    let instance = output.container_instances().first().ok_or_else(|| {
        anyhow::anyhow!(
            "ECS container instance `{container_instance_arn}` hosting `{}` was not found",
            service.ecs_name
        )
    })?;
    if instance.status() != Some("ACTIVE") || !instance.agent_connected() {
        bail!(
            "ECS container instance `{container_instance_arn}` hosting `{}` is not active with a connected ECS agent",
            service.ecs_name
        );
    }
    instance
        .ec2_instance_id()
        .filter(|instance_id| !instance_id.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ECS container instance `{container_instance_arn}` hosting `{}` has no EC2 instance id",
                service.ecs_name
            )
        })
}

fn required_executable(name: &str, display_name: &str) -> Result<PathBuf> {
    find_executable_in(env::var_os("PATH").as_deref(), name).ok_or_else(|| {
        anyhow::anyhow!(
            "{display_name} executable `{name}` is not installed or not executable on PATH"
        )
    })
}

fn find_executable_in(path: Option<&OsStr>, name: &str) -> Option<PathBuf> {
    env::split_paths(path?).find_map(|directory| {
        let candidate = directory.join(name);
        is_executable(&candidate).then_some(candidate)
    })
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn ssm_session_command(
    program: PathBuf,
    region: &str,
    instance_id: &str,
    remote_host: &str,
    local_port: u16,
    remote_port: u16,
) -> Result<SessionCommand> {
    let parameters = serde_json::to_string(&serde_json::json!({
        "host": [remote_host],
        "portNumber": [remote_port.to_string()],
        "localPortNumber": [local_port.to_string()],
    }))
    .context("serialize SSM port-forward parameters")?;
    Ok(SessionCommand {
        program,
        // Every value remains a distinct argv entry. Authored coordinates
        // and provider-returned identifiers never enter a shell, preventing
        // metacharacters from becoming executable input.
        args: vec![
            "ssm".to_owned(),
            "start-session".to_owned(),
            "--target".to_owned(),
            instance_id.to_owned(),
            "--document-name".to_owned(),
            SSM_REMOTE_HOST_DOCUMENT.to_owned(),
            "--parameters".to_owned(),
            parameters,
            "--region".to_owned(),
            region.to_owned(),
        ],
    })
}

async fn run_session(command: SessionCommand) -> Result<()> {
    let mut child = tokio::process::Command::new(&command.program)
        .args(&command.args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        // A cancelled provisioner future must not orphan a tunnel even if
        // cancellation happens outside the explicit Ctrl-C branch.
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "failed to start AWS Session Manager through `{}`",
                command.program.display()
            )
        })?;

    wait_for_session(&mut child, tokio::signal::ctrl_c()).await
}

async fn wait_for_session<F>(child: &mut tokio::process::Child, interrupted: F) -> Result<()>
where
    F: Future<Output = io::Result<()>>,
{
    tokio::select! {
        status = child.wait() => check_session_status(status?),
        interrupted = interrupted => {
            interrupted.context("failed to listen for Ctrl-C while the SSM tunnel was active")?;
            // Reap the subprocess after terminating it so the Session Manager
            // plugin cannot remain as either a live tunnel or a zombie.
            if child.try_wait()?.is_none() {
                child.kill().await.context("failed to terminate the AWS Session Manager process")?;
            }
            Ok(())
        }
    }
}

fn check_session_status(status: ExitStatus) -> Result<()> {
    if status.success() {
        Ok(())
    } else {
        bail!("AWS Session Manager exited with status {status}")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    #[test]
    fn admission_preserves_the_supported_service_set() {
        for (name, target) in [
            ("grafana", ForwardTarget::TaskAddress),
            ("mimir", ForwardTarget::TaskAddress),
            ("loki", ForwardTarget::TaskAddress),
            ("edge-api", ForwardTarget::ServiceConnect),
            ("edge-poll", ForwardTarget::ServiceConnect),
            ("controller", ForwardTarget::ServiceConnect),
        ] {
            assert_eq!(
                lookup_forward_service(name)
                    .expect("supported forward service")
                    .forward_target,
                Some(target)
            );
        }
        let error = lookup_forward_service("runtime")
            .expect_err("mapping support does not imply tunnel support");
        assert!(error.to_string().contains("valid services are: edge-api"));
    }

    #[test]
    fn task_target_is_the_running_awsvpc_task_and_its_owner() {
        let pending = ecs_task("task/a", "PENDING", "container/a", "10.0.0.1");
        let running = ecs_task("task/b", "RUNNING", "container/b", "10.0.0.2");

        let target = select_task_tunnel_target(
            &[running, pending],
            lookup_service("grafana").expect("known service"),
        )
        .expect("running task target");

        assert_eq!(
            target,
            TaskTunnelTarget {
                container_instance_arn: "container/b".to_owned(),
                private_ip: "10.0.0.2".to_owned(),
            }
        );
    }

    #[test]
    fn task_target_refuses_a_host_address_for_non_awsvpc_output() {
        let task = aws_sdk_ecs::types::Task::builder()
            .task_arn("task/a")
            .last_status("RUNNING")
            .container_instance_arn("container/a")
            .build();
        let error =
            select_task_tunnel_target(&[task], lookup_service("grafana").expect("known service"))
                .expect_err("task ENI address is required");
        assert!(error.to_string().contains("awsvpc private IPv4 address"));
    }

    #[test]
    fn session_command_uses_structured_remote_host_parameters() {
        let command = ssm_session_command(
            PathBuf::from("/tools/aws"),
            "eu-north-1",
            "i-012345",
            "tokeira-edge-api.mesh.example",
            33000,
            7233,
        )
        .expect("session command");

        assert_eq!(command.program, PathBuf::from("/tools/aws"));
        assert_eq!(command.args[0..2], ["ssm", "start-session"]);
        assert_eq!(
            command.args[3..6],
            ["i-012345", "--document-name", SSM_REMOTE_HOST_DOCUMENT]
        );
        let parameters: serde_json::Value =
            serde_json::from_str(&command.args[7]).expect("parameter JSON");
        assert_eq!(
            parameters,
            serde_json::json!({
                "host": ["tokeira-edge-api.mesh.example"],
                "portNumber": ["7233"],
                "localPortNumber": ["33000"],
            })
        );
        assert_eq!(command.args[8..], ["--region", "eu-north-1"]);
    }

    #[cfg(unix)]
    #[test]
    fn executable_lookup_requires_an_executable_regular_file() {
        let temp = tempfile::tempdir().expect("temporary PATH");
        let executable = temp.path().join("aws");
        std::fs::write(&executable, "").expect("fake executable");
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))
            .expect("mark executable");
        std::fs::create_dir(temp.path().join("session-manager-plugin")).expect("fake directory");
        let search_path = env::join_paths([temp.path()]).expect("PATH value");

        assert_eq!(
            find_executable_in(Some(&search_path), "aws"),
            Some(executable)
        );
        assert_eq!(
            find_executable_in(Some(&search_path), "session-manager-plugin"),
            None
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_interrupt_terminates_and_reaps_the_session_process() {
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", "read value"])
            .stdin(Stdio::piped())
            .spawn()
            .expect("blocking child process");

        wait_for_session(&mut child, std::future::ready(Ok(())))
            .await
            .expect("operator interruption is a clean session close");

        assert!(
            child.try_wait().expect("read child status").is_some(),
            "terminated child is reaped before the operation returns"
        );
    }

    fn ecs_task(
        arn: &str,
        status: &str,
        container_instance_arn: &str,
        private_ip: &str,
    ) -> aws_sdk_ecs::types::Task {
        let detail = aws_sdk_ecs::types::KeyValuePair::builder()
            .name("privateIPv4Address")
            .value(private_ip)
            .build();
        let attachment = aws_sdk_ecs::types::Attachment::builder()
            .details(detail)
            .build();
        aws_sdk_ecs::types::Task::builder()
            .task_arn(arn)
            .last_status(status)
            .container_instance_arn(container_instance_arn)
            .attachments(attachment)
            .build()
    }
}
