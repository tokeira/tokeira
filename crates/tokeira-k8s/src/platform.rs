//! The [`KubePlatform`] handle.
//!
//! Wraps a `kube::Client` and is registered on a
//! [`tokeira_iac::ProvisionContext`] via `set_extension`. Kubernetes-object
//! resource kinds recover it with `ctx.extension::<KubePlatform>()` and drive
//! their lifecycle (`create`/`describe`/`delete`) through its methods, keeping
//! every Kubernetes mutation on the single IaC apply path. The public methods
//! here are the thin, typed surface (returning [`crate::K8sError`]); the
//! `kube`-specific mechanics live in the sibling modules.

use std::{sync::Arc, time::Duration};

use kube::Client;
use tokio::sync::OnceCell;

use crate::{
    ApplyOptions, K8sError, PlannedManifest,
    logs::{KubeLogStream, LogOptions},
    portforward::{PortForwardConfig, PortForwardSession},
    scale::DeploymentStatus,
};

/// A handle to a live Kubernetes API server.
///
/// Cloneable and `Send + Sync + 'static`, so it is safe to store in the
/// provision context's typed extension map and share across concurrent
/// resource operations. Ambient client construction is lazy: registering the
/// handle never requires the cluster to exist or be reachable, while the first
/// operation still fails loudly if connection or authentication is unavailable.
#[derive(Clone)]
pub struct KubePlatform {
    client: Arc<OnceCell<Client>>,
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
    /// Construct an ambient-config handle without connecting to Kubernetes.
    ///
    /// The first operation initializes the shared client exactly once. Failed
    /// initialization is not cached, so a later operation can succeed after an
    /// operator repairs kubeconfig, credentials, or routing.
    pub fn lazy() -> Self {
        Self {
            client: Arc::new(OnceCell::new()),
        }
    }

    async fn client(&self) -> Result<Client, K8sError> {
        self.client
            .get_or_try_init(|| async {
                let client = Client::try_default()
                    .await
                    .map_err(|error| K8sError::Unreachable(error.to_string()))?;
                // Reachability is part of lazy initialization, not merely
                // kubeconfig construction. That keeps an offline read-only
                // plan on the typed `Unreachable` path while still letting a
                // first apply connect after its cluster module completes.
                client
                    .apiserver_version()
                    .await
                    .map_err(|error| K8sError::Unreachable(error.to_string()))?;
                Ok(client)
            })
            .await
            .cloned()
    }

    /// Compare exactly the fields a desired manifest owns with one live object.
    ///
    /// Provider-defaulted and controller-owned live fields are ignored. This
    /// pure comparator is shared by infrastructure and service drift checks so
    /// both lifecycle planes use the same ownership rule.
    pub fn desired_fields_match(desired: &serde_json::Value, live: &serde_json::Value) -> bool {
        crate::apply::desired_fields_match(desired, live)
    }

    /// Connect using the ambient kubeconfig or in-cluster service-account config.
    ///
    /// A failure here is reported as [`K8sError::Unreachable`]: read-only
    /// `plan` treats that as unsupported live state, while `apply`/`destroy`
    /// surface it as a failure.
    pub async fn connect() -> Result<Self, K8sError> {
        let platform = Self::lazy();
        platform.client().await?;
        Ok(platform)
    }

    /// Wrap an already-constructed client (tests, or a custom-config client).
    pub fn from_client(client: Client) -> Self {
        Self {
            client: Arc::new(OnceCell::from(client)),
        }
    }

    /// Confirm the API server is actually reachable.
    ///
    /// A single lightweight version call is enough to distinguish "cluster
    /// present" from "no reachable cluster" when a caller needs an explicit
    /// health check beyond the operation-local lazy initialization.
    pub async fn ensure_reachable(&self) -> Result<(), K8sError> {
        self.client()
            .await?
            .apiserver_version()
            .await
            .map(|_| ())
            .map_err(|e| K8sError::Unreachable(e.to_string()))
    }

    /// Server-side-apply a batch of raw manifests; returns the count applied.
    pub async fn apply(&self, manifests: &[serde_json::Value]) -> Result<usize, K8sError> {
        let client = self.client().await?;
        Ok(crate::apply::apply_manifests(&client, manifests).await?)
    }

    /// Server-side-apply pre-planned manifests with explicit options (e.g. to
    /// take over conflicting fields via `ApplyOptions::force_conflicts`).
    pub async fn apply_planned(
        &self,
        manifests: &[PlannedManifest],
        options: ApplyOptions,
    ) -> Result<usize, K8sError> {
        let client = self.client().await?;
        Ok(crate::apply::apply_planned_manifests(&client, manifests, options).await?)
    }

    /// Fetch a single object's live state as JSON, or `None` if it is absent.
    pub async fn get(
        &self,
        manifest: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>, K8sError> {
        let client = self.client().await?;
        Ok(crate::apply::get_manifest(&client, manifest).await?)
    }

    /// Report whether every field owned by the desired manifest set still
    /// matches the live objects.
    ///
    /// Kubernetes adds defaults and controller state after apply, so this
    /// compares the desired documents as recursive subsets of live state.
    pub async fn manifests_current(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<bool, K8sError> {
        let client = self.client().await?;
        Ok(crate::apply::manifests_current(&client, manifests).await?)
    }

    /// Delete a batch of manifests (reverse order, ignoring not-found).
    pub async fn delete(&self, manifests: &[serde_json::Value]) -> Result<usize, K8sError> {
        let client = self.client().await?;
        Ok(crate::apply::delete_manifests(&client, manifests).await?)
    }

    /// Wait until a Deployment reaches its desired ready replica count.
    pub async fn wait_ready(
        &self,
        namespace: &str,
        deployment: &str,
        timeout: Option<Duration>,
    ) -> Result<(), K8sError> {
        let client = self.client().await?;
        Ok(
            crate::watch::wait_for_deployment_ready(&client, namespace, deployment, timeout)
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
        let client = self.client().await?;
        Ok(crate::scale::patch_replicas(&client, namespace, deployment, replicas).await?)
    }

    /// Read a Deployment's current replica/readiness counts, or `None` if absent.
    pub async fn deployment_status(
        &self,
        namespace: &str,
        deployment: &str,
    ) -> Result<Option<DeploymentStatus>, K8sError> {
        let client = self.client().await?;
        Ok(crate::scale::deployment_status(&client, namespace, deployment).await?)
    }

    /// Fetch a snapshot of logs from a pod backing the named service.
    pub async fn logs(
        &self,
        namespace: &str,
        service: &str,
        options: &LogOptions,
    ) -> Result<String, K8sError> {
        let client = self.client().await?;
        Ok(crate::logs::get_pod_logs(&client, namespace, service, options).await?)
    }

    /// Open an owned line stream from a pod backing the named service.
    pub async fn log_stream(
        &self,
        namespace: &str,
        service: &str,
        options: &LogOptions,
    ) -> Result<KubeLogStream, K8sError> {
        let client = self.client().await?;
        Ok(crate::logs::open_service_log_stream(&client, namespace, service, options).await?)
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
        let client = self.client().await?;
        Ok(crate::logs::stream_service_logs(&client, namespace, service, options, on_line).await?)
    }

    /// Establish a local (loopback) TCP port-forward to a pod backing the service.
    pub async fn port_forward(
        &self,
        config: &PortForwardConfig,
    ) -> Result<PortForwardSession, K8sError> {
        let client = self.client().await?;
        Ok(crate::portforward::start_port_forward(&client, config).await?)
    }
}
