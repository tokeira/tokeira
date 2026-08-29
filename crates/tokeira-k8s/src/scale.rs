//! Deployment replica scaling primitives.
//!
//! These are deliberately single-Deployment operations. Any startup-ordered,
//! multi-service scale-up sequence belongs to the consuming platform's `Ops`
//! (which composes [`patch_replicas`] with
//! [`crate::watch::wait_for_deployment_ready`] in the tokeira service order), so
//! this crate carries no service-name ordering and stays topology-agnostic.

use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    Client,
    api::{Api, Patch, PatchParams},
};

/// Patch a Deployment's replica count.
///
/// A strategic/merge patch of just `spec.replicas` is used (not server-side
/// apply) so scaling does not disturb field ownership of the rest of the spec —
/// scaling is orthogonal to the manifest apply that `tkp` owns.
pub(crate) async fn patch_replicas(
    client: &Client,
    namespace: &str,
    deployment_name: &str,
    replicas: u32,
) -> Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({ "spec": { "replicas": replicas } });
    api.patch(
        deployment_name,
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .with_context(|| format!("failed to patch replicas for {deployment_name}"))?;
    Ok(())
}

/// Current replica/readiness counts for a single Deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentStatus {
    /// Deployment name.
    pub(crate) name: String,
    /// Desired replicas from the spec.
    pub(crate) desired: u32,
    /// Replicas that have passed their readiness probe.
    pub(crate) ready: u32,
    /// Replicas available for at least `minReadySeconds`.
    pub(crate) available: u32,
    /// Replicas running the latest pod template.
    pub(crate) updated: u32,
}

/// Read a Deployment's current status, or `None` if it does not exist.
pub(crate) async fn deployment_status(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<Option<DeploymentStatus>> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let Some(deploy) = api.get_opt(name).await? else {
        return Ok(None);
    };

    let desired = deploy.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0) as u32;
    let status = deploy.status.as_ref();
    Ok(Some(DeploymentStatus {
        name: name.to_string(),
        desired,
        ready: status.and_then(|s| s.ready_replicas).unwrap_or(0) as u32,
        available: status.and_then(|s| s.available_replicas).unwrap_or(0) as u32,
        updated: status.and_then(|s| s.updated_replicas).unwrap_or(0) as u32,
    }))
}
