//! The [`KubePlatform`] handle.
//!
//! Wraps a `kube::Client` and is registered on a
//! [`tokeira_iac::ProvisionContext`] via `set_extension`. Kubernetes-object
//! resource kinds recover it with `ctx.extension::<KubePlatform>()` and drive
//! their lifecycle (`create`/`describe`/`delete`) through its methods, keeping
//! every Kubernetes mutation on the single IaC apply path. The public methods
//! here are the thin, typed surface (returning [`crate::K8sError`]); the
//! `kube`-specific mechanics live in the sibling modules.

use std::time::Duration;

use kube::Client;

use crate::{
    ApplyOptions, K8sError, PlannedManifest,
    logs::LogOptions,
    portforward::{PortForwardConfig, PortForwardSession},
    scale::DeploymentStatus,
};

/// A handle to a live Kubernetes API server.
///
/// Cloneable (the inner `kube::Client` is a cheap `Arc`-backed handle) and
/// `Send + Sync + 'static`, so it is safe to store in the provision context's
/// typed extension map and share across concurrent resource operations.
#[derive(Clone)]
pub struct KubePlatform {
    client: Client,
}

impl std::fmt::Debug for KubePlatform {
    // `kube::Client`'s Debug is not part of our contract and reveals no useful
    // state; expose a stable, opaque shape instead so callers' Debug output does
    // not depend on the client internals.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubePlatform").finish_non_exhaustive()
    }
}

impl KubePlatform {
    /// Connect using the ambient kubeconfig or in-cluster service-account config.
    ///
    /// A failure here is reported as [`K8sError::Unreachable`]: for `tkp` this is
    /// the signal to omit the platform for read-only `plan` yet abort
    /// `apply`/`destroy`.
    pub async fn connect() -> Result<Self, K8sError> {
        let client = Client::try_default()
            .await
            .map_err(|e| K8sError::Unreachable(e.to_string()))?;
        Ok(Self { client })
    }

    /// Wrap an already-constructed client (tests, or a custom-config client).
    pub fn from_client(client: Client) -> Self {
        Self { client }
    }

    /// Borrow the underlying client for resource impls needing the raw `kube` API
    /// (e.g. [`crate::NamespaceResource`], which uses the typed `Namespace` API).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Confirm the API server is actually reachable.
    ///
    /// `tkp` calls this before registering the platform for `apply`/`destroy`.
    /// A single lightweight version call is enough to distinguish "cluster
    /// present" from "no reachable cluster" (design → Property 11).
    pub async fn ensure_reachable(&self) -> Result<(), K8sError> {
        self.client
            .apiserver_version()
            .await
            .map(|_| ())
            .map_err(|e| K8sError::Unreachable(e.to_string()))
    }

    /// Server-side-apply a batch of raw manifests; returns the count applied.
    pub async fn apply(&self, manifests: &[serde_json::Value]) -> Result<usize, K8sError> {
        Ok(crate::apply::apply_manifests(&self.client, manifests).await?)
    }

    /// Server-side-apply pre-planned manifests with explicit options (e.g. to
    /// take over conflicting fields via [`ApplyOptions::force_conflicts`]).
    pub async fn apply_planned(
        &self,
        manifests: &[PlannedManifest],
        options: ApplyOptions,
    ) -> Result<usize, K8sError> {
        Ok(crate::apply::apply_planned_manifests(&self.client, manifests, options).await?)
    }

    /// Fetch a single object's live state as JSON, or `None` if it is absent.
    pub async fn get(
        &self,
        manifest: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>, K8sError> {
        Ok(crate::apply::get_manifest(&self.client, manifest).await?)
    }

    /// Delete a batch of manifests (reverse order, ignoring not-found).
    pub async fn delete(&self, manifests: &[serde_json::Value]) -> Result<usize, K8sError> {
        Ok(crate::apply::delete_manifests(&self.client, manifests).await?)
    }

    /// Wait until a Deployment reaches its desired ready replica count.
    pub async fn wait_ready(
        &self,
        namespace: &str,
        deployment: &str,
        timeout: Option<Duration>,
    ) -> Result<(), K8sError> {
        Ok(
            crate::watch::wait_for_deployment_ready(&self.client, namespace, deployment, timeout)
                .await?,
        )
    }

    /// Patch a Deployment's replica count.
    ///
    /// This is the single-Deployment primitive; the tokeira startup-ordered
    /// scale-up loop lives in the platform's `Ops` (composing `scale` with
    /// [`wait_ready`](Self::wait_ready)), so this crate holds no service ordering.
    pub async fn scale(
        &self,
        namespace: &str,
        deployment: &str,
        replicas: u32,
    ) -> Result<(), K8sError> {
        Ok(crate::scale::patch_replicas(&self.client, namespace, deployment, replicas).await?)
    }

    /// Read a Deployment's current replica/readiness counts, or `None` if absent.
    pub async fn deployment_status(
        &self,
        namespace: &str,
        deployment: &str,
    ) -> Result<Option<DeploymentStatus>, K8sError> {
        Ok(crate::scale::deployment_status(&self.client, namespace, deployment).await?)
    }

    /// Fetch a snapshot of logs from a pod backing the named service.
    pub async fn logs(
        &self,
        namespace: &str,
        service: &str,
        options: &LogOptions,
    ) -> Result<String, K8sError> {
        Ok(crate::logs::get_pod_logs(&self.client, namespace, service, options).await?)
    }

    /// Stream logs from a pod backing the named service, one line per callback.
    pub async fn stream_logs<F>(
        &self,
        namespace: &str,
        service: &str,
        options: &LogOptions,
        on_line: F,
    ) -> Result<(), K8sError>
    where
        F: FnMut(&str),
    {
        Ok(
            crate::logs::stream_service_logs(&self.client, namespace, service, options, on_line)
                .await?,
        )
    }

    /// Establish a local (loopback) TCP port-forward to a pod backing the service.
    pub async fn port_forward(
        &self,
        config: &PortForwardConfig,
    ) -> Result<PortForwardSession, K8sError> {
        Ok(crate::portforward::start_port_forward(&self.client, config).await?)
    }
}
