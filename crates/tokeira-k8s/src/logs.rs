//! Pod log retrieval and streaming via the `kube` log API.
//!
//! A service is addressed by its `app={service}` label (see
//! [`crate::standard_labels`]); the platform resolves that to a backing pod and
//! reads its logs. This is the operator `logs` day-2 op — no `kubectl` shell-out.

use anyhow::{Context, Result};
use futures::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Client,
    api::{Api, ListParams, LogParams},
};
use tracing::{info, warn};

/// Options for reading or following pod logs.
#[derive(Debug, Clone, Default)]
pub struct LogOptions {
    /// Follow the log stream (`tail -f` semantics).
    pub follow: bool,
    /// Limit to the last N lines; `None` returns all available lines.
    pub tail_lines: Option<i64>,
    /// Target a specific container; `None` uses the pod's default container.
    pub container: Option<String>,
}

impl LogOptions {
    /// Translate into `kube`'s `LogParams`, applying `follow`/`tail`/`container`.
    fn to_params(&self) -> LogParams {
        LogParams {
            follow: self.follow,
            tail_lines: self.tail_lines,
            container: self.container.clone(),
            ..Default::default()
        }
    }
}

/// Fetch a snapshot of a service pod's logs as a single string (non-following).
pub(crate) async fn get_pod_logs(
    client: &Client,
    namespace: &str,
    service_name: &str,
    options: &LogOptions,
) -> Result<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pod_name = find_running_pod(&pods, service_name)
        .await?
        .with_context(|| format!("no running pod found for service {service_name}"))?;
    let logs = pods.logs(&pod_name, &options.to_params()).await?;
    Ok(logs)
}

/// Stream a service pod's logs, invoking `on_line` for each line as it arrives.
pub(crate) async fn stream_service_logs<F>(
    client: &Client,
    namespace: &str,
    service_name: &str,
    options: &LogOptions,
    mut on_line: F,
) -> Result<()>
where
    F: FnMut(&str),
{
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pod_name = find_running_pod(&pods, service_name)
        .await?
        .with_context(|| format!("no running pod found for service {service_name}"))?;

    info!(pod = pod_name, service = service_name, "streaming logs");

    let stream = pods.log_stream(&pod_name, &options.to_params()).await?;
    let mut lines = stream.lines();
    while let Some(line) = lines.try_next().await? {
        on_line(&line);
    }
    Ok(())
}

/// Resolve a service's backing pod by the `app={service_name}` selector.
///
/// Prefers a `Running` pod; if none is running yet it falls back to the first
/// matching pod so an operator can still read startup logs of a not-yet-ready
/// pod. Returns `None` when no pod matches the selector at all.
async fn find_running_pod(pods: &Api<Pod>, service_name: &str) -> Result<Option<String>> {
    let lp = ListParams::default().labels(&format!("app={service_name}"));
    let pod_list = pods.list(&lp).await?;

    for pod in &pod_list.items {
        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("Unknown");
        if phase == "Running"
            && let Some(name) = pod.metadata.name.as_deref()
        {
            return Ok(Some(name.to_string()));
        }
    }

    if let Some(pod) = pod_list.items.first()
        && let Some(name) = pod.metadata.name.as_deref()
    {
        warn!(
            pod = name,
            service = service_name,
            "no running pod found, using first available"
        );
        return Ok(Some(name.to_string()));
    }

    Ok(None)
}
