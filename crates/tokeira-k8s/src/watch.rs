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

        // The caller's deadline covers an in-flight request, including any
        // retries configured on a supplied client, not just the poll interval.
        let readiness = tokio::time::timeout_at(
            deadline,
            check_deployment_ready(&api, deployment_name),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timeout waiting for deployment {deployment_name} to become ready after {}s",
                timeout.as_secs()
            )
        })??;
        match readiness {
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
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
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

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use http::{Request, Response, StatusCode};
    use kube::{
        Client,
        client::{Body, retry::RetryPolicy},
    };
    use tokio::time::Instant;
    use tower::{BoxError, ServiceBuilder, retry::RetryLayer, service_fn};

    use super::wait_for_deployment_ready;

    #[tokio::test(start_paused = true)]
    async fn readiness_deadline_bounds_a_stalled_request() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let client = Client::new(
            service_fn(move |_: Request<Body>| {
                observed.fetch_add(1, Ordering::SeqCst);
                pending::<Result<Response<Body>, BoxError>>()
            }),
            "default",
        );
        let started = Instant::now();
        let error =
            wait_for_deployment_ready(&client, "default", "worker", Some(Duration::from_secs(5)))
                .await
                .expect_err("a stalled API request must respect the readiness deadline");
        assert_eq!(
            error.to_string(),
            "timeout waiting for deployment worker to become ready after 5s"
        );
        assert_eq!(Instant::now() - started, Duration::from_secs(5));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_deadline_bounds_server_retry_backoff() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let service = service_fn(move |_: Request<Body>| {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .header("Retry-After", "60")
                        .body(Body::empty())
                        .expect("valid retry response"),
                )
            }
        });
        let client = Client::new(
            ServiceBuilder::new()
                .layer(RetryLayer::new(RetryPolicy::server_retry()))
                .service(service),
            "default",
        );
        let started = Instant::now();
        let error =
            wait_for_deployment_ready(&client, "default", "worker", Some(Duration::from_secs(5)))
                .await
                .expect_err("server backoff must not extend the readiness deadline");
        assert!(error.to_string().contains("after 5s"));
        assert_eq!(Instant::now() - started, Duration::from_secs(5));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_waits_for_updated_replicas() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let client = Client::new(
            service_fn(move |request: Request<Body>| {
                assert_eq!(
                    request.uri().path(),
                    "/apis/apps/v1/namespaces/default/deployments/worker"
                );
                let updated = observed.fetch_add(1, Ordering::SeqCst);
                async move {
                    let deployment = serde_json::json!({
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {"name": "worker"},
                        "spec": {"replicas": 1, "selector": {}, "template": {}},
                        "status": {"readyReplicas": 1, "updatedReplicas": updated},
                    });
                    Ok::<_, BoxError>(Response::new(Body::from(
                        deployment.to_string().into_bytes(),
                    )))
                }
            }),
            "default",
        );
        let started = Instant::now();
        wait_for_deployment_ready(&client, "default", "worker", Some(Duration::from_secs(5)))
            .await
            .expect("the second response completes the rollout");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(Instant::now() - started, Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_preserves_api_errors() {
        let client = Client::new(
            service_fn(|_: Request<Body>| async {
                let status = serde_json::json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": "access denied",
                    "reason": "Forbidden",
                    "code": 403,
                });
                Ok::<_, BoxError>(
                    Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(Body::from(status.to_string().into_bytes()))
                        .expect("valid error response"),
                )
            }),
            "default",
        );
        let started = Instant::now();
        let error =
            wait_for_deployment_ready(&client, "default", "worker", Some(Duration::from_secs(5)))
                .await
                .expect_err("API authorization errors must remain distinguishable from timeouts");
        assert!(
            matches!(error.downcast_ref::<kube::Error>(), Some(kube::Error::Api(status)) if status.code == 403)
        );
        assert_eq!(Instant::now(), started);
    }
}
