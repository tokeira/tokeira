//! On-demand execution through the ECS admin service.
//!
//! The shipped definition keeps the admin service at zero replicas. This
//! operation temporarily requests one task, waits until that task and its ECS
//! Exec agent are ready, executes the operator's argv through the same
//! hardened Exec path, and restores the desired count it observed before the
//! operation on every ordinary return path. That preserves an explicitly
//! authored or operator-held singleton instead of silently scaling it down.
//! Admission's deployment operation lease serializes this sequence with
//! apply, scale, and another admin command; this module owns no additional
//! lock or deployment state.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use aws_sdk_ecs::types::DesiredStatus;
use tokeira_aws::AwsClients;
use tokeira_platform::declaration::DeploymentRef;

use super::{
    EcsOperationCoordinates, desired_count, exec, lookup_service, provider_error,
    session_manager::require_client_tools,
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_secs(3);

pub(super) async fn run(deployment: &DeploymentRef, command: &[String]) -> Result<()> {
    if command.is_empty() {
        bail!("no admin command specified");
    }
    // Refuse before changing desired capacity if this workstation cannot
    // attach to the session the task is being started to serve.
    require_client_tools()?;

    let coordinates = EcsOperationCoordinates::read(deployment)?;
    let clients = AwsClients::load(Some(coordinates.region())).await;
    let service = lookup_service("admin").expect("the ECS service table always contains admin");
    let current = desired_count(&clients, &coordinates, service).await?;
    if current > 1 {
        bail!(
            "ECS admin service `{}` has desired count {current}; refusing ambiguous on-demand execution",
            service.ecs_name
        );
    }
    let needs_restore = current == 0;

    // Scale-up belongs inside the Ctrl-C race. If interruption cancels an
    // UpdateService request after AWS accepted it but before the response,
    // the restoration below still converges the service to its prior count.
    // During Exec the dropped session future kills its child process first.
    let operation = async {
        if needs_restore {
            update_desired_count(&clients, &coordinates, service.ecs_name, 1).await?;
        }
        wait_until_ready(&clients, &coordinates, service, STARTUP_TIMEOUT).await?;
        exec::run(deployment, "admin", None, command).await
    };
    tokio::pin!(operation);
    let outcome = tokio::select! {
        result = &mut operation => result,
        interrupted = tokio::signal::ctrl_c() => {
            interrupted.context("failed to listen for Ctrl-C during the admin operation")?;
            Err(anyhow::anyhow!("admin operation interrupted"))
        }
    };

    let restore = if needs_restore {
        let prior_count = i32::try_from(current)
            .expect("admin desired count was validated as no greater than one");
        update_desired_count(&clients, &coordinates, service.ecs_name, prior_count)
            .await
            .context("failed to restore the ECS admin service's prior desired count")
    } else {
        Ok(())
    };
    combine_operation_and_restore(outcome, restore)
}

async fn wait_until_ready(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: super::Service,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let readiness =
            exec::discover_exec_target(clients, coordinates, service, service.ecs_name).await;
        match readiness {
            Ok(_) => return Ok(()),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                let stopped_reason = stopped_reason(clients, coordinates, service.ecs_name)
                    .await
                    .map(|reason| format!("; most recent stopped task: {reason}"))
                    .unwrap_or_default();
                bail!(
                    "ECS admin service `{}` did not become Exec-ready within {}s; last readiness error: {error:#}{stopped_reason}",
                    service.ecs_name,
                    timeout.as_secs()
                );
            }
            Err(_) => {}
        }
        tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + POLL_INTERVAL)).await;
    }
}

async fn update_desired_count(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: &str,
    replicas: i32,
) -> Result<()> {
    clients
        .ecs
        .update_service()
        .cluster(coordinates.cluster())
        .service(service)
        .desired_count(replicas)
        .send()
        .await
        .map_err(|error| provider_error(coordinates, "ecs:UpdateService", error))?;
    Ok(())
}

async fn stopped_reason(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: &str,
) -> Option<String> {
    let listed = clients
        .ecs
        .list_tasks()
        .cluster(coordinates.cluster())
        .service_name(service)
        .desired_status(DesiredStatus::Stopped)
        .send()
        .await
        .ok()?;
    if listed.task_arns().is_empty() {
        return None;
    }
    let described = clients
        .ecs
        .describe_tasks()
        .cluster(coordinates.cluster())
        .set_tasks(Some(listed.task_arns().to_vec()))
        .send()
        .await
        .ok()?;
    described
        .tasks()
        .iter()
        .find_map(|task| task.stopped_reason().map(ToOwned::to_owned))
}

fn combine_operation_and_restore(operation: Result<()>, restore: Result<()>) -> Result<()> {
    match (operation, restore) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(operation), Ok(())) => Err(operation),
        (Ok(()), Err(restore)) => Err(restore),
        (Err(operation), Err(restore)) => Err(anyhow::anyhow!(
            "admin operation failed: {operation:#}; cleanup also failed: {restore:#}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_failure_is_never_hidden() {
        let error = combine_operation_and_restore(
            Err(anyhow::anyhow!("exec failed")),
            Err(anyhow::anyhow!("scale down failed")),
        )
        .expect_err("both failures must be reported");

        let message = error.to_string();
        assert!(message.contains("exec failed"));
        assert!(message.contains("scale down failed"));
    }
}
