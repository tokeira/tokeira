//! A bundle of Kubernetes manifests modeled as one IaC [`iac::Resource`].
//!
//! This is the seam that keeps EKS on the single `InfraEngine` apply path: a
//! Kubernetes object (or a small co-owned set — a service's Deployment, Service,
//! ServiceAccount, and ConfigMap) is an ordinary `iac::Resource` whose lifecycle
//! methods drive the context's [`KubePlatform`]. There is no separate
//! manifest-only infrastructure channel.
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
    resource_type: &'static str,
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
    pub(crate) fn new(
        resource_type: &'static str,
        id: impl Into<String>,
        module: impl Into<String>,
        dependencies: Vec<ResourceId>,
        manifests: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            resource_type,
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
    fn state_with_manifests(&self, manifests: &[serde_json::Value]) -> ResourceState {
        ResourceState {
            resource_type: ResourceType::new(self.resource_type),
            physical_id: self.id.0.clone(),
            properties: serde_json::json!({ "manifests": manifests }),
            dependencies: self.dependencies.clone(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.module.clone(),
        }
    }

    fn state(&self) -> ResourceState {
        self.state_with_manifests(&self.manifests)
    }
}

#[async_trait]
impl iac::Resource for K8sManifestResource {
    fn change_semantics(&self, ctx: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
        const CREATE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::create — server-side apply of the bundle's manifests through \
             the registered KubePlatform"
        ));
        const UPDATE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::update — a re-apply of the desired manifest set: server-side \
             apply reconciles field-by-field; Kubernetes rolls workloads whose \
             pod template changed under their own update strategy"
        ));
        const DELETE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::delete — deletes the bundle's objects in reverse order \
             (not-found tolerated); whatever they were running stops"
        ));
        let claims = |operation,
                      disruption: iac::Confidence<iac::Disruption>,
                      citation: iac::Citation| iac::ChangeSemantics {
            operation: iac::Confidence::EngineFact {
                value: operation,
                citation: citation.clone(),
            },
            replacement: iac::Confidence::EngineFact {
                value: iac::ReplacementPolicy::NotRequired,
                citation: citation.clone(),
            },
            disruption,
            data_effect: iac::Confidence::EngineFact {
                value: iac::DataEffect::NoDataHeld,
                citation: citation.clone(),
            },
            reversibility: iac::Confidence::EngineFact {
                value: iac::Reversibility::Reversible,
                citation,
            },
            statement: None,
            provider_assigned: Vec::new(),
        };
        match ctx.kind {
            iac::ChangeKind::Create => claims(
                iac::LifecycleOperation::Created,
                iac::Confidence::EngineFact {
                    value: iac::Disruption::None,
                    citation: CREATE,
                },
                CREATE,
            ),
            // The roll is Kubernetes executing each workload's own update
            // strategy after our re-apply — derived, not issued.
            iac::ChangeKind::Update | iac::ChangeKind::Replace => claims(
                iac::LifecycleOperation::UpdatedInPlace,
                iac::Confidence::Inference {
                    value: iac::Disruption::Rolling,
                    citation: UPDATE,
                },
                UPDATE,
            ),
            iac::ChangeKind::Delete => claims(
                iac::LifecycleOperation::Deleted,
                iac::Confidence::EngineFact {
                    value: iac::Disruption::UnavailableDuringChange,
                    citation: DELETE,
                },
                DELETE,
            ),
            iac::ChangeKind::NoChange => iac::ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new(self.resource_type)
    }

    fn resource_id(&self) -> ResourceId {
        self.id.clone()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.dependencies.clone()
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({ "manifests": self.manifests })
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
        let mut live_manifests = Vec::with_capacity(self.manifests.len());
        for manifest in &self.manifests {
            match platform.get(manifest).await.map_err(to_iac)? {
                Some(live) => {
                    any_present = true;
                    live_manifests.push(live);
                }
                None => live_manifests.push(serde_json::Value::Null),
            }
        }
        // Any live object of the bundle means it exists; re-apply reconciles the
        // rest. Only a fully-absent bundle is a genuine `Absent`.
        Ok(if any_present {
            DescribeResult::Present(self.state_with_manifests(&live_manifests))
        } else {
            DescribeResult::Absent
        })
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let live_matches =
            current
                .properties
                .get("manifests")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|live| {
                    live.len() == self.manifests.len()
                        && self.manifests.iter().zip(live).all(|(desired, live)| {
                            KubePlatform::desired_fields_match(desired, live)
                        })
                });
        if live_matches {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "kubernetes manifests changed",
                )],
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokeira_iac::Resource as _;

    use super::*;

    fn resource() -> K8sManifestResource {
        K8sManifestResource::new(
            "Fixture",
            "fixture/demo",
            "cluster",
            Vec::new(),
            vec![serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": "demo", "namespace": "default" }
            })],
        )
    }

    // Feature: platform-eks, Property 8
    #[tokio::test]
    async fn plan_without_a_cluster_preserves_unknown_live_state() {
        let described = resource()
            .describe(&ProvisionContext::default())
            .await
            .expect("describe without a cluster is non-fatal");
        assert!(matches!(described, DescribeResult::Unsupported));
    }

    // Feature: platform-eks, Property 8
    #[tokio::test]
    async fn apply_without_a_registered_cluster_refuses_loudly() {
        let error = resource()
            .create(&ProvisionContext::default())
            .await
            .expect_err("apply needs a live cluster handle");
        let message = error.to_string();
        assert!(message.contains("KubePlatform is not registered"));
        assert!(message.contains("reachable cluster is required"));
    }

    #[test]
    fn diff_ignores_server_fields_but_detects_owned_drift_and_missing_objects() {
        let resource = resource();
        let mut live = resource.state();
        live.properties["manifests"][0]["metadata"]["resourceVersion"] = serde_json::json!("42");
        assert!(matches!(
            resource.diff(&live, &ProvisionContext::default()),
            InternalChange::NoChange { .. }
        ));

        live.properties["manifests"][0]["metadata"]["name"] = serde_json::json!("retargeted");
        assert!(matches!(
            resource.diff(&live, &ProvisionContext::default()),
            InternalChange::Update { .. }
        ));

        live.properties["manifests"][0] = serde_json::Value::Null;
        assert!(matches!(
            resource.diff(&live, &ProvisionContext::default()),
            InternalChange::Update { .. }
        ));
    }
}
