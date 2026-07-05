//! Deployment readiness polling.
//!
//! Readiness is checked with a short `get_opt` poll loop rather than a
//! `kube::runtime` watcher: the wait is bounded and single-object, so polling
//! avoids pulling the `runtime` feature and its watch/informer machinery for no
//! benefit. The `tokio::time::sleep` here is production polling, not test timing.

use std::time::Duration;

use anyhow::Result;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{Client, api::Api};
use tracing::{info, warn};

/// Default readiness wait when the caller does not specify one.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// Wait until a Deployment reaches full readiness, or the timeout elapses.
///
/// "Ready" requires `ready_replicas >= desired` **and** `updated_replicas >=
/// desired`, so a rollout mid-flight (old replicas still ready, new ones not yet
/// updated) is correctly treated as not-ready. A Deployment scaled to zero is
/// ready immediately (nothing to wait for).
pub(crate) async fn wait_for_deployment_ready(
    client: &Client,
    namespace: &str,
    deployment_name: &str,
    timeout: Option<Duration>,
) -> Result<()> {
    let timeout = timeout.unwrap_or(DEFAULT_TIMEOUT);
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);

    info!(
        deployment = deployment_name,
        namespace,
        timeout_secs = timeout.as_secs(),
        "waiting for deployment readiness"
    );

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timeout waiting for deployment {deployment_name} to become ready after {}s",
                timeout.as_secs()
            );
        }

        match check_deployment_ready(&api, deployment_name).await? {
            ReadinessState::Ready { replicas } => {
                info!(deployment = deployment_name, replicas, "deployment ready");
                return Ok(());
            }
            ReadinessState::NotReady {
                desired,
                ready,
                updated,
            } => info!(
                deployment = deployment_name,
                desired, ready, updated, "waiting for replicas"
            ),
            ReadinessState::NotFound => {
                warn!(
                    deployment = deployment_name,
                    "deployment not found, waiting"
                )
            }
        }

        // Cap each sleep at the remaining budget so the deadline is honored
        // even when it is closer than the poll interval.
        let remaining = deadline - tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_secs(2).min(remaining)).await;
    }
}

/// Compute a Deployment's current readiness relative to its desired replicas.
pub(crate) async fn check_deployment_ready(
    api: &Api<Deployment>,
    name: &str,
) -> Result<ReadinessState> {
    let Some(deploy) = api.get_opt(name).await? else {
        return Ok(ReadinessState::NotFound);
    };

    let desired = deploy.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1) as u32;
    let status = deploy.status.as_ref();
    let ready = status.and_then(|s| s.ready_replicas).unwrap_or(0) as u32;
    let updated = status.and_then(|s| s.updated_replicas).unwrap_or(0) as u32;

    if desired == 0 {
        Ok(ReadinessState::Ready { replicas: 0 })
    } else if ready >= desired && updated >= desired {
        Ok(ReadinessState::Ready { replicas: ready })
    } else {
        Ok(ReadinessState::NotReady {
            desired,
            ready,
            updated,
        })
    }
}

/// Readiness of a Deployment relative to its desired replica count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessState {
    /// All desired replicas are ready and updated (or the Deployment is at 0).
    Ready {
        /// Number of ready replicas.
        replicas: u32,
    },
    /// Fewer replicas are ready/updated than desired.
    NotReady {
        /// Desired replica count from the spec.
        desired: u32,
        /// Currently ready replicas.
        ready: u32,
        /// Replicas running the latest pod template.
        updated: u32,
    },
    /// The Deployment does not exist yet.
    NotFound,
}
