//! Interactive command execution in live ECS task containers.
//!
//! Target selection is made from the running tasks of the definition-owned
//! service, not from a capacity-provider guess. The selected task must have
//! ECS Exec enabled and the requested container's execute-command agent must
//! be running before the AWS CLI is launched. The CLI owns the session token
//! and plugin protocol; this module passes every local argument without a
//! shell, while the operator's command is quoted for the remote Linux shell.

use std::{collections::BTreeSet, path::PathBuf};

use anyhow::{Result, bail};
use aws_sdk_ecs::types::{ManagedAgentName, Task};
use tokeira_aws::AwsClients;
use tokeira_platform::declaration::DeploymentRef;

use super::{
    EcsOperationCoordinates, Service, lookup_service, provider_error, running_tasks,
    session_manager::{SessionCommand, require_client_tools, run_session},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExecTarget {
    task_arn: String,
    container_name: String,
}

pub(super) async fn run(
    deployment: &DeploymentRef,
    service: &str,
    container: Option<&str>,
    command: &[String],
) -> Result<()> {
    let service = lookup_service(service)?;
    let container = match container {
        Some(name) if name.trim().is_empty() => bail!("ECS Exec container name must not be empty"),
        Some(name) => name,
        None => service.ecs_name,
    };
    let remote_command = remote_command(command)?;

    // Check the workstation before allocating provider work. The AWS CLI
    // owns creation of the session and hands its opaque token directly to
    // the Session Manager plugin.
    let aws_cli = require_client_tools()?;
    let coordinates = EcsOperationCoordinates::read(deployment)?;
    let clients = AwsClients::load(Some(coordinates.region())).await;
    let target = discover_exec_target(&clients, &coordinates, service, container).await?;
    let session = ecs_exec_command(aws_cli, &coordinates, &target, remote_command);
    run_session(session).await
}

pub(super) async fn discover_exec_target(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: Service,
    container: &str,
) -> Result<ExecTarget> {
    let task_arns = running_tasks(clients, coordinates, service).await?;
    let output = clients
        .ecs
        .describe_tasks()
        .cluster(coordinates.cluster())
        .set_tasks(Some(task_arns))
        .send()
        .await
        .map_err(|error| provider_error(coordinates, "ecs:DescribeTasks", error))?;
    select_exec_target(output.tasks(), service, container)
}

fn select_exec_target(tasks: &[Task], service: Service, container: &str) -> Result<ExecTarget> {
    let mut running = tasks
        .iter()
        .filter(|task| task.last_status() == Some("RUNNING"))
        .collect::<Vec<_>>();
    running.sort_by_key(|task| task.task_arn().unwrap_or_default());
    if running.is_empty() {
        bail!("ECS service `{}` has no running tasks", service.ecs_name);
    }

    let enabled = running
        .into_iter()
        .filter(|task| task.enable_execute_command())
        .collect::<Vec<_>>();
    if enabled.is_empty() {
        bail!(
            "ECS Exec is not enabled on the running tasks for `{}`; redeploy the service before retrying",
            service.ecs_name
        );
    }

    let available = enabled
        .iter()
        .flat_map(|task| task.containers())
        .filter_map(|candidate| candidate.name())
        .collect::<BTreeSet<_>>();
    if !available.contains(container) {
        bail!(
            "container `{container}` was not found in running ECS Exec tasks for `{}`; available containers: {}",
            service.ecs_name,
            available.into_iter().collect::<Vec<_>>().join(", ")
        );
    }

    enabled
        .into_iter()
        .find_map(|task| {
            let candidate = task.containers().iter().find(|candidate| {
                candidate.name() == Some(container)
                    && candidate.last_status() == Some("RUNNING")
                    && candidate.managed_agents().iter().any(|agent| {
                        agent.name() == Some(&ManagedAgentName::ExecuteCommandAgent)
                            && agent.last_status() == Some("RUNNING")
                    })
            })?;
            Some(ExecTarget {
                task_arn: task.task_arn()?.to_owned(),
                container_name: candidate.name()?.to_owned(),
            })
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "container `{container}` for ECS service `{}` has no running ExecuteCommandAgent",
                service.ecs_name
            )
        })
}

fn remote_command(command: &[String]) -> Result<String> {
    if command.is_empty() {
        bail!("no command specified; pass the remote command after `--`");
    }
    Ok(command
        .iter()
        .map(|argument| quote_remote_argument(argument))
        .collect::<Vec<_>>()
        .join(" "))
}

fn quote_remote_argument(argument: &str) -> String {
    if !argument.is_empty()
        && argument
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_+-./:@%=".contains(character))
    {
        argument.to_owned()
    } else {
        format!("'{}'", argument.replace('\'', "'\"'\"'"))
    }
}

fn ecs_exec_command(
    program: PathBuf,
    coordinates: &EcsOperationCoordinates,
    target: &ExecTarget,
    command: String,
) -> SessionCommand {
    SessionCommand {
        program,
        // The remote command is intentionally executable input, but every
        // local value remains one argv entry: authored names and AWS-returned
        // identifiers can never become workstation shell syntax.
        args: vec![
            "ecs".to_owned(),
            "execute-command".to_owned(),
            "--cluster".to_owned(),
            coordinates.cluster().to_owned(),
            "--task".to_owned(),
            target.task_arn.clone(),
            "--container".to_owned(),
            target.container_name.clone(),
            "--interactive".to_owned(),
            "--command".to_owned(),
            command,
            "--region".to_owned(),
            coordinates.region().to_owned(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_ecs::types::{Container, ManagedAgent};

    use super::*;

    #[test]
    fn selects_the_first_ready_task_and_requested_container() {
        let unready = task("task/b", true, container("tokeira-runtime", "STOPPED"));
        let ready = task("task/a", true, container("tokeira-runtime", "RUNNING"));

        let target = select_exec_target(
            &[unready, ready],
            lookup_service("runtime").expect("known service"),
            "tokeira-runtime",
        )
        .expect("ready exec target");

        assert_eq!(
            target,
            ExecTarget {
                task_arn: "task/a".to_owned(),
                container_name: "tokeira-runtime".to_owned(),
            }
        );
    }

    #[test]
    fn refuses_tasks_created_without_exec_enabled() {
        let disabled = task("task/a", false, container("tokeira-runtime", "RUNNING"));

        let error = select_exec_target(
            &[disabled],
            lookup_service("runtime").expect("known service"),
            "tokeira-runtime",
        )
        .expect_err("exec-disabled task is ineligible");

        assert!(error.to_string().contains("ECS Exec is not enabled"));
    }

    #[test]
    fn refuses_an_unknown_container_with_live_alternatives() {
        let ready = task("task/a", true, container("alloy", "RUNNING"));

        let error = select_exec_target(
            &[ready],
            lookup_service("runtime").expect("known service"),
            "missing",
        )
        .expect_err("unknown container is refused");

        assert!(error.to_string().contains("available containers: alloy"));
    }

    #[test]
    fn preserves_remote_argument_boundaries() {
        assert_eq!(
            remote_command(&[
                "sh".to_owned(),
                "-c".to_owned(),
                "printf '%s' \"$VALUE\"".to_owned(),
            ])
            .expect("remote command"),
            "sh -c 'printf '\"'\"'%s'\"'\"' \"$VALUE\"'"
        );
        assert_eq!(
            remote_command(&[String::new()]).expect("empty argument"),
            "''"
        );
    }

    #[test]
    fn command_uses_exact_discovered_coordinates_without_a_local_shell() {
        let coordinates = EcsOperationCoordinates {
            environment: "review".to_owned(),
            region: "eu-north-1".to_owned(),
            cluster: "ops-cluster".to_owned(),
            service_connect_namespace: "mesh.example".to_owned(),
            private_dns_zone: "private.example".to_owned(),
            loki_query_url: "http://loki.private.example:3100".to_owned(),
        };
        let target = ExecTarget {
            task_arn: "arn:aws:ecs:eu-north-1:123:task/cluster/id".to_owned(),
            container_name: "tokeira-runtime".to_owned(),
        };

        let command = ecs_exec_command(
            PathBuf::from("/tools/aws"),
            &coordinates,
            &target,
            "sh -c 'id'".to_owned(),
        );

        assert_eq!(command.program, PathBuf::from("/tools/aws"));
        assert_eq!(
            command.args,
            [
                "ecs",
                "execute-command",
                "--cluster",
                "ops-cluster",
                "--task",
                "arn:aws:ecs:eu-north-1:123:task/cluster/id",
                "--container",
                "tokeira-runtime",
                "--interactive",
                "--command",
                "sh -c 'id'",
                "--region",
                "eu-north-1",
            ]
        );
    }

    fn task(arn: &str, exec_enabled: bool, container: Container) -> Task {
        Task::builder()
            .task_arn(arn)
            .last_status("RUNNING")
            .enable_execute_command(exec_enabled)
            .containers(container)
            .build()
    }

    fn container(name: &str, agent_status: &str) -> Container {
        let agent = ManagedAgent::builder()
            .name(ManagedAgentName::ExecuteCommandAgent)
            .last_status(agent_status)
            .build();
        Container::builder()
            .name(name)
            .last_status("RUNNING")
            .managed_agents(agent)
            .build()
    }
}
