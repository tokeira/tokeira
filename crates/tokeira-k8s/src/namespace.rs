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
use k8s_openapi::api::core::v1::Namespace as K8sNamespace;
use kube::api::{Api, DeleteParams, ObjectMeta, PostParams};
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    IacError, InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource,
    ResourceId, ResourceState, ResourceType, Reversibility, SemanticsContext,
};

use crate::KubePlatform;

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
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.name.clone(),
            properties: serde_json::json!({ "namespace": self.name }),
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
            "::update — bookkeeping only: a namespace is name-only from the \
             engine's view and `diff` never reports a change"
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

    fn module(&self) -> &str {
        &self.config.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let client = self.platform(ctx)?.client().clone();
        let ns_api: Api<K8sNamespace> = Api::all(client);

        let ns = K8sNamespace {
            metadata: ObjectMeta {
                name: Some(self.name.clone()),
                labels: Some(crate::standard_labels(&self.name, &self.project)),
                ..Default::default()
            },
            ..Default::default()
        };

        match ns_api.create(&PostParams::default(), &ns).await {
            Ok(_) => {}
            // Adopt a pre-existing namespace rather than fail: from the engine's
            // perspective create must be idempotent, so a namespace that already
            // exists (e.g. re-apply after a partial run) is a success, not drift.
            Err(kube::Error::Api(ref e)) if e.code == 409 => {
                tracing::warn!(namespace = %self.name, "namespace already exists, adopting");
            }
            Err(e) => return Err(IacError::Other(anyhow::anyhow!("k8s CreateNamespace: {e}"))),
        }

        Ok(self.current_state())
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        // A namespace has no engine-tracked mutable fields (see `diff`), so
        // `update` is only ever reached for bookkeeping; refresh the timestamp
        // and re-derived fields, preserving the original creation time.
        Ok(ResourceState {
            updated_at: chrono::Utc::now().to_rfc3339(),
            dependencies: self.dependencies(),
            module: self.config.module.clone(),
            ..current.clone()
        })
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let client = self.platform(ctx)?.client().clone();
        let ns_api: Api<K8sNamespace> = Api::all(client);

        match ns_api.delete(&self.name, &DeleteParams::default()).await {
            Ok(_) => Ok(()),
            // Already gone is success: delete is idempotent.
            Err(kube::Error::Api(ref e)) if e.code == 404 => {
                tracing::warn!(namespace = %self.name, "namespace already absent, skipping");
                Ok(())
            }
            Err(e) => Err(IacError::Other(anyhow::anyhow!("k8s DeleteNamespace: {e}"))),
        }
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        // No platform means existence is unknowable, not absent. Returning
        // `Unsupported` (rather than `Absent`) stops the engine from pruning
        // persisted state, and is precisely the path a read-only `plan` takes
        // when no cluster is reachable (design → Property 11).
        let Some(platform) = ctx.extension::<KubePlatform>() else {
            return Ok(DescribeResult::Unsupported);
        };

        let ns_api: Api<K8sNamespace> = Api::all(platform.client().clone());
        match ns_api.get_opt(&self.name).await {
            Ok(Some(_)) => Ok(DescribeResult::Present(self.current_state())),
            // A positive "not found" from the API server is a genuine `Absent`.
            Ok(None) => Ok(DescribeResult::Absent),
            Err(e) => Err(IacError::Other(anyhow::anyhow!("k8s GetNamespace: {e}"))),
        }
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // A namespace is name-only from the engine's view — nothing it tracks can
        // drift — so it never reports a change once created.
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
