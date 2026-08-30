//! A Kubernetes namespace modeled as an IaC [`Resource`].
//!
//! Recovers the [`KubePlatform`] from the provision context and applies the
//! namespace lifecycle through it, so namespaces flow through the same engine
//! path as every other resource (design → "Single apply path"). The
//! [`describe`](NamespaceResource::describe) distinguishes a confirmed-absent
//! namespace ([`DescribeResult::Absent`], prunable) from "no platform
//! registered" ([`DescribeResult::Unsupported`], never prune) — this is what
//! lets a read-only `plan` run against no reachable cluster without the engine
//! orphaning persisted state.

use async_trait::async_trait;
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    IacError, InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource,
    ResourceId, ResourceState, ResourceType, Reversibility, SemanticsContext,
};

use crate::{K8sError, KubePlatform};

/// Opaque resource-type tag recorded in state for a namespace.
const RESOURCE_TYPE: &str = "Namespace";

/// Configuration for a namespace resource.
#[derive(Debug, Clone)]
pub struct NamespaceConfig {
    /// The EKS cluster this namespace lives in. Declaring it as a dependency
    /// keeps namespace creation ordered strictly after the cluster exists (there
    /// is no API server to create a namespace in before then).
    pub eks_cluster_dependency: ResourceId,
    /// Name of the owning module, recorded for state attribution.
    pub module: String,
}

/// A Kubernetes namespace managed by the IaC engine.
#[derive(Debug)]
pub struct NamespaceResource {
    name: String,
    config: NamespaceConfig,
    project: String,
}

impl NamespaceResource {
    /// Build a namespace resource for `name` under `project`.
    pub fn new(
        name: impl Into<String>,
        config: NamespaceConfig,
        project: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            config,
            project: project.into(),
        }
    }

    /// Recover the live Kubernetes platform, or error if it was not registered.
    ///
    /// Mutating operations (`create`/`delete`) require it; `describe` handles the
    /// absent-platform case itself (returning `Unsupported`) rather than calling
    /// this, so planning can proceed without a cluster.
    fn platform<'a>(&self, ctx: &'a ProvisionContext) -> Result<&'a KubePlatform, IacError> {
        ctx.extension::<KubePlatform>().ok_or_else(|| {
            IacError::Other(anyhow::anyhow!(
                "KubePlatform is not registered on the provision context; \
                 a reachable cluster is required for this operation"
            ))
        })
    }

    /// The persisted state for this namespace at the current instant.
    fn current_state(&self) -> ResourceState {
        self.state_with_manifest(self.desired_manifest())
    }

    fn state_with_manifest(&self, manifest: serde_json::Value) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.name.clone(),
            properties: serde_json::json!({
                "namespace": self.name,
                "manifest": manifest,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.config.module.clone(),
        }
    }
}

#[async_trait]
impl Resource for NamespaceResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — Namespace create through the registered KubePlatform \
             client, with standard labels"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — server-side apply reconciles the namespace labels \
             owned by Tokeira without replacing the namespace"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — Namespace delete through the registered KubePlatform \
             client; Kubernetes garbage-collects every object scoped to the \
             namespace with it"
        ));
        let claims = |operation,
                      disruption,
                      data_effect: Confidence<DataEffect>,
                      reversibility: Confidence<Reversibility>,
                      citation: Citation| ChangeSemantics {
            operation: Confidence::EngineFact {
                value: operation,
                citation: citation.clone(),
            },
            replacement: Confidence::EngineFact {
                value: ReplacementPolicy::NotRequired,
                citation: citation.clone(),
            },
            disruption: Confidence::EngineFact {
                value: disruption,
                citation,
            },
            data_effect,
            reversibility,
            statement: None,
            provider_assigned: Vec::new(),
        };
        match ctx.kind {
            ChangeKind::Create => claims(
                LifecycleOperation::Created,
                Disruption::None,
                Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: CREATE,
                },
                Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: CREATE,
                },
                CREATE,
            ),
            ChangeKind::Update | ChangeKind::Replace => claims(
                LifecycleOperation::UpdatedInPlace,
                Disruption::None,
                Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: UPDATE,
                },
                Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: UPDATE,
                },
                UPDATE,
            ),
            // The cascade is Kubernetes's behaviour, not a call we issue —
            // the data claims are derived: our delete removes one object;
            // everything scoped to it (workloads, config, claims) goes by
            // the platform's garbage collection, and only what a definition
            // re-applies comes back.
            ChangeKind::Delete => claims(
                LifecycleOperation::Deleted,
                Disruption::UnavailableDuringChange,
                Confidence::Inference {
                    value: DataEffect::Destroyed,
                    citation: DELETE,
                },
                Confidence::Inference {
                    value: Reversibility::ReversibleWithDataLoss,
                    citation: DELETE,
                },
                DELETE,
            ),
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new(RESOURCE_TYPE)
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("namespace/{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.config.eks_cluster_dependency.clone()]
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": self.name,
                "labels": crate::standard_labels(&self.name, &self.project),
            },
        })
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        self.platform(ctx)?
            .apply(&[self.desired_manifest()])
            .await
            .map_err(|error| IacError::Other(anyhow::Error::new(error)))?;

        Ok(self.current_state())
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        self.platform(ctx)?
            .apply(&[self.desired_manifest()])
            .await
            .map_err(|error| IacError::Other(anyhow::Error::new(error)))?;
        Ok(ResourceState {
            updated_at: chrono::Utc::now().to_rfc3339(),
            dependencies: self.dependencies(),
            module: self.config.module.clone(),
            properties: self.current_state().properties,
            ..current.clone()
        })
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        self.platform(ctx)?
            .delete(&[self.desired_manifest()])
            .await
            .map(|_| ())
            .map_err(|error| IacError::Other(anyhow::Error::new(error)))
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        // A missing handle or a lazy handle whose first connection cannot
        // reach the cluster makes existence unknowable, not absent. Returning
        // `Unsupported` keeps read-only plan provider-pure and prevents state
        // pruning; mutations still surface the same connection failure loudly.
        let Some(platform) = ctx.extension::<KubePlatform>() else {
            return Ok(DescribeResult::Unsupported);
        };

        match platform.get(&self.desired_manifest()).await {
            Ok(Some(live)) => Ok(DescribeResult::Present(self.state_with_manifest(live))),
            Ok(None) => Ok(DescribeResult::Absent),
            Err(K8sError::Unreachable(_)) => Ok(DescribeResult::Unsupported),
            Err(error) => Err(IacError::Other(anyhow::Error::new(error))),
        }
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let matches = current
            .properties
            .get("manifest")
            .is_some_and(|live| KubePlatform::desired_fields_match(&self.desired_manifest(), live));
        if matches {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "namespace labels changed",
                )],
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource() -> NamespaceResource {
        NamespaceResource::new(
            "runtime",
            NamespaceConfig {
                eks_cluster_dependency: ResourceId("eks/demo".into()),
                module: "cluster".into(),
            },
            "demo",
        )
    }

    #[test]
    fn diff_ignores_server_fields_and_detects_owned_label_drift() {
        let resource = resource();
        let mut live = resource.current_state();
        live.properties["manifest"]["metadata"]["resourceVersion"] = serde_json::json!("42");
        assert!(matches!(
            resource.diff(&live, &ProvisionContext::default()),
            InternalChange::NoChange { .. }
        ));

        live.properties["manifest"]["metadata"]["labels"]["app.kubernetes.io/managed-by"] =
            serde_json::json!("someone-else");
        assert!(matches!(
            resource.diff(&live, &ProvisionContext::default()),
            InternalChange::Update { .. }
        ));
    }
}
