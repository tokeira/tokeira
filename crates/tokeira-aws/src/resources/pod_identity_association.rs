use std::{collections::HashMap, time::Duration};

use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
    error::IacError,
};

/// Configuration for a single EKS Pod Identity Association provider resource.
#[derive(Debug)]
pub struct PodIdentityAssociationConfig {
    pub eks_cluster_dependency: ResourceId,
    pub namespace: String,
    pub service_account: String,
    pub iam_role_dependency: ResourceId,
    pub module: String,
}

/// Generic provider resource that provisions exactly one EKS Pod Identity Association.
///
/// Associates a Kubernetes service account with an IAM role so pods using
/// that service account automatically receive the role's credentials.
#[derive(Debug)]
pub struct PodIdentityAssociation {
    pub config: PodIdentityAssociationConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl PodIdentityAssociation {
    pub fn new(config: PodIdentityAssociationConfig, rctx: &crate::ResourceContext) -> Self {
        Self {
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    /// Extract the cluster short name from the EKS cluster dependency ResourceId.
    /// E.g. "my-project-eks-cluster" → "my-project-eks"
    fn cluster_name(&self) -> String {
        let dep = &self.config.eks_cluster_dependency.0;
        // The EKS cluster resource_id is "{project}-eks-cluster", cluster name is "{project}-eks"
        dep.strip_suffix("-cluster").unwrap_or(dep).to_string()
    }

    async fn wait_until_present(
        &self,
        ctx: &ProvisionContext,
        expected_association_id: &str,
    ) -> Result<(), IacError> {
        let cluster_name = self.cluster_name();
        let namespace = self.config.namespace.clone();
        let service_account = self.config.service_account.clone();
        let expected_association_id = expected_association_id.to_string();
        let eks_client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks;
        super::poll_until(
            Duration::from_secs(2),
            Duration::from_secs(120),
            ctx,
            super::PollTarget {
                resource_desc: "Pod Identity association",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for Pod Identity association creation",
            },
            || async {
                let output = eks_client
                    .list_pod_identity_associations()
                    .cluster_name(&cluster_name)
                    .namespace(&namespace)
                    .service_account(&service_account)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "eks:ListPodIdentityAssociations: {}",
                            e.into_service_error()
                        ))
                    })?;
                Ok(output.associations().iter().any(|association| {
                    association.association_id() == Some(expected_association_id.as_str())
                }))
            },
        )
        .await
    }

    async fn wait_until_deleted(
        &self,
        ctx: &ProvisionContext,
        deleted_association_id: &str,
    ) -> Result<(), IacError> {
        let cluster_name = self.cluster_name();
        let namespace = self.config.namespace.clone();
        let service_account = self.config.service_account.clone();
        let deleted_association_id = deleted_association_id.to_string();
        let eks_client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks;
        super::poll_until(
            Duration::from_secs(2),
            Duration::from_secs(120),
            ctx,
            super::PollTarget {
                resource_desc: "Pod Identity association deletion",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for Pod Identity association deletion",
            },
            || async {
                let output = eks_client
                    .list_pod_identity_associations()
                    .cluster_name(&cluster_name)
                    .namespace(&namespace)
                    .service_account(&service_account)
                    .send()
                    .await
                    .map_err(|e| {
                        let svc_err = e.into_service_error();
                        if svc_err.is_resource_not_found_exception() {
                            IacError::ResourceNotFound {
                                resource_type: "eks cluster".into(),
                                resource_id: cluster_name.clone(),
                            }
                        } else {
                            IacError::AwsSdk(format!("eks:ListPodIdentityAssociations: {svc_err}"))
                        }
                    });
                match output {
                    Ok(output) => Ok(!output.associations().iter().any(|association| {
                        association.association_id() == Some(deleted_association_id.as_str())
                    })),
                    Err(IacError::ResourceNotFound { .. }) => Ok(true),
                    Err(err) => Err(err),
                }
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Resource for PodIdentityAssociation {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("PodIdentityAssociation")
    }

    fn resource_id(&self) -> ResourceId {
        let cluster = self.cluster_name();
        ResourceId(format!(
            "pod-id-{}-{}-{}",
            cluster, self.config.namespace, self.config.service_account
        ))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            self.config.eks_cluster_dependency.clone(),
            self.config.iam_role_dependency.clone(),
        ]
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // Pod identity associations are immutable — recreate to change.
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let cluster_name = self.cluster_name();
        let tags = ctx.resource_tags(&self.resource_id().0);

        // Look up the IAM role ARN from state
        let iam_state = ctx.get_resource_state(&self.config.iam_role_dependency)?;
        let role_arn = iam_state
            .properties
            .get("role_arn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound(format!(
                    "role_arn not found in state for {}",
                    self.config.iam_role_dependency.0
                ))
            })?
            .to_string();

        // eks:CreatePodIdentityAssociation
        let association_id = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .create_pod_identity_association()
            .cluster_name(&cluster_name)
            .namespace(&self.config.namespace)
            .service_account(&self.config.service_account)
            .role_arn(&role_arn)
            .set_tags(Some(tags.clone()))
            .send()
            .await
        {
            Ok(output) => output
                .association()
                .and_then(|a| a.association_id())
                .unwrap_or_default()
                .to_string(),
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_in_use_exception() {
                    let existing = self.describe(ctx).await?.ok_or_else(|| {
                        IacError::AwsSdk(
                            "eks:CreatePodIdentityAssociation: association already exists but could not be described"
                                .into(),
                        )
                    })?;
                    existing.physical_id
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "eks:CreatePodIdentityAssociation: {svc_err}"
                    )));
                }
            }
        };

        self.wait_until_present(ctx, &association_id).await?;

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("PodIdentityAssociation"),
            physical_id: association_id,
            properties: serde_json::json!({
                "cluster_name": cluster_name,
                "namespace": self.config.namespace,
                "service_account": self.config.service_account,
                "role_arn": role_arn,
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        })
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        // Pod identity associations are immutable — update is a no-op.
        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: current.properties.clone(),
            dependencies: self.dependencies(),
            created_at: current.created_at.clone(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        })
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let cluster_name = self.cluster_name();
        let association_id = &current.physical_id;

        if association_id.is_empty() {
            return Ok(());
        }

        // eks:DeletePodIdentityAssociation
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .delete_pod_identity_association()
            .cluster_name(&cluster_name)
            .association_id(association_id)
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    tracing::warn!(
                        association = %association_id,
                        "pod identity association not found, skipping deletion"
                    );
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "eks:DeletePodIdentityAssociation: {svc_err}"
                    )));
                }
            }
        }

        self.wait_until_deleted(ctx, association_id).await
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError> {
        if !ctx
            .state
            .resources
            .contains_key(&self.config.eks_cluster_dependency)
        {
            return Ok(None);
        }

        let cluster_name = self.cluster_name();

        // eks:ListPodIdentityAssociations filtered by namespace + service account
        let list_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .list_pod_identity_associations()
            .cluster_name(&cluster_name)
            .namespace(&self.config.namespace)
            .service_account(&self.config.service_account)
            .send()
            .await
            .map_err(|e| {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    IacError::ResourceNotFound {
                        resource_type: "eks cluster".into(),
                        resource_id: cluster_name.clone(),
                    }
                } else {
                    IacError::AwsSdk(format!("eks:ListPodIdentityAssociations: {svc_err}"))
                }
            });
        let list_output = match list_output {
            Ok(output) => output,
            Err(IacError::ResourceNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        if let Some(assoc) = list_output.associations().first() {
            let association_id = assoc.association_id().unwrap_or_default().to_string();
            let now = chrono::Utc::now().to_rfc3339();
            Ok(Some(ResourceState {
                resource_type: ResourceType::new("PodIdentityAssociation"),
                physical_id: association_id,
                properties: serde_json::json!({
                    "cluster_name": cluster_name,
                    "namespace": self.config.namespace,
                    "service_account": self.config.service_account,
                }),
                dependencies: self.dependencies(),
                created_at: now.clone(),
                updated_at: now,
                module: self.module().to_owned(),
            }))
        } else {
            Ok(None)
        }
    }
}
