//! Port-forward sessions via the `kube` port-forward tunnel.
//!
//! Binds a local TCP listener and proxies each accepted connection to a backing
//! pod's port through the Kubernetes API server tunnel (no `kubectl`). This is
//! how operators reach private-only services (there is no Ingress/LoadBalancer;
//! design → "Private-Only Access"): access is always mediated by the API server.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Client,
    api::{Api, ListParams},
};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

/// Configuration for a port-forward session.
#[derive(Debug, Clone)]
pub struct PortForwardConfig {
    /// Namespace of the target service.
    pub(crate) namespace: String,
    /// Service name; the backing pod is found via `app={service_name}`.
    pub(crate) service_name: String,
    /// Remote port on the pod to forward to.
    pub(crate) remote_port: u16,
    /// Local port to bind (on loopback only).
    pub(crate) local_port: u16,
}

impl PortForwardConfig {
    /// Construct a loopback-only forward to one service pod port.
    pub fn new(
        namespace: impl Into<String>,
        service_name: impl Into<String>,
        remote_port: u16,
        local_port: u16,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            service_name: service_name.into(),
            remote_port,
            local_port,
        }
    }
}

/// A running port-forward session handle.
///
/// Dropping [`abort_handle`](Self::abort_handle) (or aborting it) stops the
/// accept loop and tears down the forward.
#[derive(Debug)]
pub struct PortForwardSession {
    /// The bound local loopback address.
    pub local_addr: SocketAddr,
    /// The background accept-loop task; abort it to stop forwarding.
    pub abort_handle: tokio::task::JoinHandle<()>,
}

impl Drop for PortForwardSession {
    fn drop(&mut self) {
        // The listener is a capability owned by this handle. Explicitly abort
        // it so ending an operator session cannot leave a detached loopback
        // listener alive inside a longer-running provisioner process.
        self.abort_handle.abort();
    }
}

/// Start a port-forward session to a pod backing the target service.
///
/// Binds `127.0.0.1:{local_port}` (loopback only — never exposes the forward on
/// a routable interface) and spawns an accept loop that proxies each connection
/// over its own tunnel.
pub(crate) async fn start_port_forward(
    client: &Client,
    config: &PortForwardConfig,
) -> Result<PortForwardSession> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), &config.namespace);
    let pod_name = find_ready_pod(&pods, &config.service_name)
        .await?
        .with_context(|| {
            format!(
                "no ready pod found for service {} in namespace {}",
                config.service_name, config.namespace
            )
        })?;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.local_port))
        .await
        .with_context(|| format!("failed to bind local port {}", config.local_port))?;
    let local_addr = listener.local_addr()?;

    info!(
        pod = pod_name,
        service = config.service_name,
        remote_port = config.remote_port,
        %local_addr,
        "port-forward listening"
    );

    let pods = pods.clone();
    let remote_port = config.remote_port;
    let abort_handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((local_stream, peer)) => {
                    info!(%peer, "port-forward connection accepted");
                    let pods = pods.clone();
                    let pod = pod_name.clone();
                    // Each connection gets its own tunnel so a single failed
                    // connection cannot tear down the whole session.
                    tokio::spawn(async move {
                        if let Err(e) =
                            proxy_connection(local_stream, &pods, &pod, remote_port).await
                        {
                            warn!(error = %e, "port-forward connection error");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "port-forward accept failed, stopping session");
                    break;
                }
            }
        }
    });

    Ok(PortForwardSession {
        local_addr,
        abort_handle,
    })
}

/// Proxy one accepted TCP connection through the pod port-forward tunnel.
async fn proxy_connection(
    mut local_stream: tokio::net::TcpStream,
    pods: &Api<Pod>,
    pod_name: &str,
    remote_port: u16,
) -> Result<()> {
    let mut pf = pods.portforward(pod_name, &[remote_port]).await?;
    let upstream = pf
        .take_stream(remote_port)
        .context("port-forward tunnel did not yield an upstream stream")?;

    let (mut local_read, mut local_write) = local_stream.split();
    let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream);

    // Whichever half closes first ends the connection; the other copy is dropped.
    tokio::select! {
        r = tokio::io::copy(&mut local_read, &mut upstream_write) => {
            if let Err(e) = r { warn!(error = %e, "client->pod copy ended"); }
        }
        r = tokio::io::copy(&mut upstream_read, &mut local_write) => {
            if let Err(e) = r { warn!(error = %e, "pod->client copy ended"); }
        }
    }
    Ok(())
}

/// Resolve a backing pod by `app={service_name}`, preferring a Ready pod.
///
/// Falls back to any `Running` pod when none reports Ready, so a forward can be
/// established during rollout; returns `None` when nothing matches.
async fn find_ready_pod(pods: &Api<Pod>, service_name: &str) -> Result<Option<String>> {
    let lp = ListParams::default().labels(&format!("app={service_name}"));
    let pod_list = pods.list(&lp).await?;

    for pod in &pod_list.items {
        if is_pod_ready(pod)
            && let Some(name) = pod.metadata.name.as_deref()
        {
            return Ok(Some(name.to_string()));
        }
    }
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
    Ok(None)
}

/// True when the pod carries a `Ready` condition set to `True`.
fn is_pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
}
