//! A bundle of Kubernetes manifests modeled as one IaC [`iac::Resource`].
//!
//! This is the seam that keeps EKS on the single `InfraEngine` apply path: a
//! Kubernetes object (or a small co-owned set — a service's Deployment, Service,
//! ServiceAccount, and ConfigMap) is an ordinary `iac::Resource` whose lifecycle
//! methods drive the context's [`KubePlatform`], exactly as a compose container
//! is an infra resource applied via `ComposePlatform` (design → "single
//! InfraEngine path"). There is no separate `DeployEngine`/manifest-only channel.
//!
//! `describe` distinguishes "no platform registered" ([`DescribeResult::Unsupported`],
//! never prune — the read-only `plan`-without-cluster path) from a confirmed-absent
//! object ([`DescribeResult::Absent`]); `diff` re-applies whenever the desired
//! manifest set changes (server-side apply is idempotent, so re-apply reconciles).

use async_trait::async_trait;
use tokeira_iac::{
    self as iac, DescribeResult, InternalChange, ProvisionContext, ResourceId, ResourceState,
    ResourceType, error::IacError,
};
use tokeira_k8s::{K8sError, KubePlatform};

/// Opaque resource-type tag recorded in state for a manifest bundle.
const RESOURCE_TYPE: &str = "K8sManifest";

/// Convert a `tokeira-k8s` error into the engine's error type, preserving the
/// context chain. `K8sError` is `Send + Sync + 'static`, so it wraps cleanly.
fn to_iac(err: K8sError) -> IacError {
    IacError::Other(anyhow::Error::new(err))
}

/// One or more Kubernetes manifests applied together as a single IaC resource.
///
/// The manifests are applied in the given order (so a ServiceAccount/ConfigMap
/// precedes the Deployment that references it) and deleted in reverse.
#[derive(Debug)]
pub struct K8sManifestResource {
    id: ResourceId,
    module: String,
    dependencies: Vec<ResourceId>,
    manifests: Vec<serde_json::Value>,
}

impl K8sManifestResource {
    /// Build a manifest-bundle resource.
    ///
    /// `dependencies` are the IaC resources that must exist first — typically the
    /// EKS cluster and the target namespace, so a workload is never applied
    /// before its cluster/namespace exist.
    pub fn new(
        id: impl Into<String>,
        module: impl Into<String>,
        dependencies: Vec<ResourceId>,
        manifests: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            id: ResourceId(id.into()),
            module: module.into(),
            dependencies,
            manifests,
        }
    }

    /// Recover the live Kubernetes platform, or error if it was not registered.
    /// Mutating operations require it; `describe` handles its absence itself.
    fn platform<'a>(&self, ctx: &'a ProvisionContext) -> Result<&'a KubePlatform, IacError> {
        ctx.extension::<KubePlatform>().ok_or_else(|| {
            IacError::Other(anyhow::anyhow!(
                "KubePlatform is not registered on the provision context; \
                 a reachable cluster is required to apply Kubernetes manifests"
            ))
        })
    }

    /// The persisted state. The desired manifests are recorded so `diff` can
    /// detect a changed manifest set and trigger a re-apply.
    fn state(&self) -> ResourceState {
        ResourceState {
            resource_type: ResourceType::new(RESOURCE_TYPE),
            physical_id: self.id.0.clone(),
            properties: serde_json::json!({ "manifests": self.manifests }),
            dependencies: self.dependencies.clone(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.module.clone(),
        }
    }
}

#[async_trait]
impl iac::Resource for K8sManifestResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new(RESOURCE_TYPE)
    }

    fn resource_id(&self) -> ResourceId {
        self.id.clone()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.dependencies.clone()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        self.platform(ctx)?
            .apply(&self.manifests)
            .await
            .map_err(to_iac)?;
        Ok(self.state())
    }

    async fn update(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        // Server-side apply is idempotent and reconciles field-by-field, so an
        // update is just a re-apply of the desired manifest set.
        self.platform(ctx)?
            .apply(&self.manifests)
            .await
            .map_err(to_iac)?;
        Ok(self.state())
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        // `delete` tolerates not-found and removes in reverse order.
        self.platform(ctx)?
            .delete(&self.manifests)
            .await
            .map_err(to_iac)?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        // No platform → existence is unknowable, not absent: return `Unsupported`
        // so the engine never prunes on a read-only `plan` with no reachable
        // cluster (design → Property 11). Every desired manifest then diffs as a
        // Create.
        let Some(platform) = ctx.extension::<KubePlatform>() else {
            return Ok(DescribeResult::Unsupported);
        };
        let mut any_present = false;
        for manifest in &self.manifests {
            if platform.get(manifest).await.map_err(to_iac)?.is_some() {
                any_present = true;
            }
        }
        // Any live object of the bundle means it exists; re-apply reconciles the
        // rest. Only a fully-absent bundle is a genuine `Absent`.
        Ok(if any_present {
            DescribeResult::Present(self.state())
        } else {
            DescribeResult::Absent
        })
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // Re-apply whenever the desired manifest set differs from what was last
        // recorded. Comparing the recorded manifests (not a checksum) keeps the
        // diff exact and dependency-free.
        let desired = serde_json::json!({ "manifests": self.manifests });
        if current.properties == desired {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: "kubernetes manifests changed".to_string(),
            }
        }
    }
}
