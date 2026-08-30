use std::time::Duration;

use aws_sdk_eks::client::Waiters as EksWaiters;
use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, DescribeResult, Disruption,
    InternalChange, LifecycleOperation, ProvisionContext, ReplacementPolicy, Resource, ResourceId,
    ResourceState, ResourceType, Reversibility, SemanticsContext, error::IacError,
};

/// EKS cluster with Auto Mode.
/// Depends on VPC, EKS node security group, and cluster/node IAM roles.
#[derive(Debug)]
pub struct EksClusterResource {
    project: String,
    version: String,
    namespace: String,
    kms_key_arn: Option<String>,
    deletion_protection: bool,
    bootstrap_admin_permissions: bool,
    cluster_admin_principal_arn: Option<String>,
    module: String,
}

/// EKS-specific configuration passed by the caller.
#[derive(Debug, Clone)]
pub struct EksConfig {
    pub version: String,
    pub namespace: String,
    pub kms_key_arn: Option<String>,
    pub deletion_protection: bool,
    pub bootstrap_admin_permissions: bool,
    pub cluster_admin_principal_arn: Option<String>,
}

impl EksClusterResource {
    pub fn new(rctx: &crate::ResourceContext, eks: EksConfig, module: impl Into<String>) -> Self {
        Self {
            project: rctx.project.clone(),
            version: eks.version,
            namespace: eks.namespace,
            kms_key_arn: eks.kms_key_arn,
            deletion_protection: eks.deletion_protection,
            bootstrap_admin_permissions: eks.bootstrap_admin_permissions,
            cluster_admin_principal_arn: eks.cluster_admin_principal_arn,
            module: module.into(),
        }
    }

    fn cluster_name(&self) -> String {
        format!("{}-eks", self.project)
    }

    /// Returns a deterministic idempotency token for CreateCluster.
    /// Same cluster name always produces the same token.
    pub(crate) fn idempotency_token(&self) -> String {
        format!("{}-create", self.cluster_name())
    }

    /// Whether this resource supports adopt-on-conflict for ResourceInUseException.
    pub fn supports_adopt_on_conflict(&self) -> bool {
        true
    }

    fn access_entry_conflict(err: &str) -> bool {
        let lower = err.to_lowercase();
        lower.contains("resourceinuseexception") || lower.contains("already exists")
    }

    fn access_policy_conflict(err: &str) -> bool {
        let lower = err.to_lowercase();
        lower.contains("resourceinuseexception")
            || lower.contains("already associated")
            || lower.contains("already exists")
    }

    async fn admin_principal_arn(&self, ctx: &ProvisionContext) -> Result<String, IacError> {
        match &self.cluster_admin_principal_arn {
            Some(arn) if !arn.is_empty() => Ok(arn.clone()),
            _ => {
                let caller = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .sts
                    .get_caller_identity()
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "sts:GetCallerIdentity: {}",
                            e.into_service_error()
                        ))
                    })?;
                Ok(caller.arn().unwrap_or_default().to_string())
            }
        }
    }

    async fn ensure_cluster_admin_access(
        &self,
        ctx: &ProvisionContext,
        cluster_name: &str,
        admin_arn: &str,
    ) -> Result<(), IacError> {
        if admin_arn.is_empty() {
            return Ok(());
        }

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .create_access_entry()
            .cluster_name(cluster_name)
            .principal_arn(admin_arn)
            .r#type("STANDARD")
            .send()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let svc_err = e.into_service_error();
                let err_text = format!("{svc_err}");
                if !Self::access_entry_conflict(&err_text) {
                    return Err(IacError::AwsSdk(format!(
                        "eks:CreateAccessEntry: {svc_err}"
                    )));
                }
                tracing::warn!(cluster = %cluster_name, principal = %admin_arn, "access entry already exists, adopting");
            }
        }

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .associate_access_policy()
            .cluster_name(cluster_name)
            .principal_arn(admin_arn)
            .policy_arn("arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy")
            .access_scope(
                aws_sdk_eks::types::AccessScope::builder()
                    .r#type(aws_sdk_eks::types::AccessScopeType::Cluster)
                    .build(),
            )
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let svc_err = e.into_service_error();
                let err_text = format!("{svc_err}");
                if Self::access_policy_conflict(&err_text) {
                    tracing::warn!(cluster = %cluster_name, principal = %admin_arn, "access policy already associated, adopting");
                    Ok(())
                } else {
                    Err(IacError::AwsSdk(format!(
                        "eks:AssociateAccessPolicy: {svc_err}"
                    )))
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Resource for EksClusterResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — eks:CreateCluster (existing cluster adopted) over the \
             state-read VPC/SG/roles, wait_until_cluster_active (up to 20m), \
             then admin access entries"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — eks:UpdateClusterVersion in place when the declared \
             version moved (wait_until_cluster_active, up to 20m), then \
             eks:TagResource; nothing in this module touches workloads or \
             cluster state"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — delete recorded access entries, disable deletion \
             protection, eks:DeleteCluster, wait_until_cluster_deleted; \
             everything running on the cluster goes with it"
        ));
        match ctx.kind {
            ChangeKind::Create => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Created,
                    citation: CREATE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: CREATE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: CREATE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: CREATE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: CREATE,
                },
                statement: None,
                provider_assigned: vec!["cluster_arn".into(), "cluster_endpoint".into()],
            },
            // The version upgrade's runtime disruption is EKS's managed
            // behaviour, not something this module's calls establish —
            // deliberately Unknown rather than guessed. Reversibility is
            // derived: neither EKS nor this module offers a version
            // downgrade, so returning means recreating the cluster.
            ChangeKind::Update | ChangeKind::Replace => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::UpdatedInPlace,
                    citation: UPDATE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: UPDATE,
                },
                disruption: Confidence::Unknown,
                data_effect: Confidence::Inference {
                    value: DataEffect::Preserved,
                    citation: UPDATE,
                },
                reversibility: Confidence::Inference {
                    value: Reversibility::ReversibleWithDataLoss,
                    citation: UPDATE,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            ChangeKind::Delete => ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: DELETE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: DELETE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: DELETE,
                },
                data_effect: Confidence::Inference {
                    value: DataEffect::Destroyed,
                    citation: DELETE,
                },
                reversibility: Confidence::Inference {
                    value: Reversibility::ReversibleWithDataLoss,
                    citation: DELETE,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("EksCluster")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("{}-eks-cluster", self.project))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            ResourceId(format!("{}-vpc", self.project)),
            ResourceId(format!("sg-{}-eks-nodes-sg", self.project)),
            ResourceId(format!("iam-role-{}-eks-cluster-role", self.project)),
            ResourceId(format!("iam-role-{}-eks-auto-node-role", self.project)),
        ]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("{}-eks-cluster", self.project));
        let cluster_name = self.cluster_name();

        // Read VPC state for subnet_ids
        let vpc_state = ctx.get_resource_state(&ResourceId(format!("{}-vpc", self.project)))?;
        let subnet_ids: Vec<String> = vpc_state
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // Read security group state for security_group_id
        let sg_state =
            ctx.get_resource_state(&ResourceId(format!("sg-{}-eks-nodes-sg", self.project)))?;
        let sg_id = sg_state
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("security_group_id not found in SG state".into())
            })?
            .to_string();

        // Read IAM state for cluster and node role ARNs
        let cluster_iam_state = ctx.get_resource_state(&ResourceId(format!(
            "iam-role-{}-eks-cluster-role",
            self.project
        )))?;
        let cluster_role_arn = cluster_iam_state
            .properties
            .get("role_arn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("role_arn not found in cluster IAM role state".into())
            })?
            .to_string();

        // Read node role ARN for Auto Mode compute config
        let node_iam_state = ctx.get_resource_state(&ResourceId(format!(
            "iam-role-{}-eks-auto-node-role",
            self.project
        )))?;
        let node_role_arn = node_iam_state
            .properties
            .get("role_arn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("role_arn not found in node IAM role state".into())
            })?
            .to_string();

        // 1. eks:CreateCluster
        let logging = aws_sdk_eks::types::Logging::builder()
            .cluster_logging(
                aws_sdk_eks::types::LogSetup::builder()
                    .enabled(true)
                    .types(aws_sdk_eks::types::LogType::Api)
                    .types(aws_sdk_eks::types::LogType::Audit)
                    .types(aws_sdk_eks::types::LogType::Authenticator)
                    .types(aws_sdk_eks::types::LogType::ControllerManager)
                    .types(aws_sdk_eks::types::LogType::Scheduler)
                    .build(),
            )
            .build();

        let vpc_config = aws_sdk_eks::types::VpcConfigRequest::builder()
            .set_subnet_ids(Some(subnet_ids))
            .security_group_ids(&sg_id)
            .endpoint_public_access(false)
            .endpoint_private_access(true)
            .build();

        let compute_config = aws_sdk_eks::types::ComputeConfigRequest::builder()
            .enabled(true)
            .node_role_arn(&node_role_arn)
            .node_pools("general-purpose")
            .node_pools("system")
            .build();

        let storage_config = aws_sdk_eks::types::StorageConfigRequest::builder()
            .block_storage(
                aws_sdk_eks::types::BlockStorage::builder()
                    .enabled(true)
                    .build(),
            )
            .build();

        let kubernetes_network_config =
            aws_sdk_eks::types::KubernetesNetworkConfigRequest::builder()
                .elastic_load_balancing(
                    aws_sdk_eks::types::ElasticLoadBalancing::builder()
                        .enabled(true)
                        .build(),
                )
                .build();

        // 12.6: Disable bootstrap admin permissions for explicit access entry management
        let access_config = aws_sdk_eks::types::CreateAccessConfigRequest::builder()
            .authentication_mode(aws_sdk_eks::types::AuthenticationMode::Api)
            .bootstrap_cluster_creator_admin_permissions(self.bootstrap_admin_permissions)
            .build();

        // 12.4: Build encryption config when kms_key_arn is configured
        let encryption_configs: Option<Vec<aws_sdk_eks::types::EncryptionConfig>> =
            self.kms_key_arn.as_ref().map(|key_arn| {
                vec![
                    aws_sdk_eks::types::EncryptionConfig::builder()
                        .provider(
                            aws_sdk_eks::types::Provider::builder()
                                .key_arn(key_arn)
                                .build(),
                        )
                        .resources("secrets")
                        .build(),
                ]
            });

        // 12.2: Deterministic idempotency token
        let idempotency_token = self.idempotency_token();

        // Build the CreateCluster request
        let mut create_req = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .create_cluster()
            .name(&cluster_name)
            .version(&self.version)
            .role_arn(&cluster_role_arn)
            .resources_vpc_config(vpc_config)
            .logging(logging)
            .compute_config(compute_config)
            .storage_config(storage_config)
            .kubernetes_network_config(kubernetes_network_config)
            .access_config(access_config)
            .client_request_token(&idempotency_token)
            .deletion_protection(self.deletion_protection)
            .set_tags(Some(tags.clone()));

        // 12.4: Add encryption config when present
        if let Some(enc_configs) = encryption_configs {
            create_req = create_req.set_encryption_config(Some(enc_configs));
        }

        // 12.3: Handle ResourceInUseException — adopt existing cluster
        let adopted = match create_req.send().await {
            Ok(_) => false,
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_in_use_exception() {
                    tracing::warn!(
                        cluster = %cluster_name,
                        "cluster already exists, adopting"
                    );
                    true
                } else {
                    return Err(IacError::AwsSdk(format!("eks:CreateCluster: {svc_err}")));
                }
            }
        };

        if adopted {
            tracing::info!(cluster = %cluster_name, "adopted existing cluster, waiting for ACTIVE");
        }

        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .wait_until_cluster_active()
            .name(&cluster_name)
            .wait(Duration::from_secs(1200))
            .await
            .map_err(|e| IacError::AwsSdk(format!("eks:WaitUntilClusterActive: {e}")))?;

        // Fetch cluster details after it's active
        let desc_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .describe_cluster()
            .name(&cluster_name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("eks:DescribeCluster: {}", e.into_service_error()))
            })?;

        let cluster = desc_output.cluster();
        let cluster_arn = cluster
            .map(|c| c.arn())
            .unwrap_or(Some(""))
            .unwrap_or("")
            .to_string();
        let cluster_endpoint = cluster
            .and_then(|c| c.endpoint())
            .unwrap_or_default()
            .to_string();
        let certificate_authority = cluster
            .and_then(|c| c.certificate_authority())
            .and_then(|ca| ca.data())
            .unwrap_or_default()
            .to_string();

        // 12.6: Create explicit access entry for cluster admin
        // Determine the admin principal ARN — use config or fall back to STS caller identity
        let admin_arn = self.admin_principal_arn(ctx).await?;

        let mut access_entry_principal_arn = String::new();
        if !admin_arn.is_empty() {
            self.ensure_cluster_admin_access(ctx, &cluster_name, &admin_arn)
                .await?;
            access_entry_principal_arn = admin_arn;
        }

        Ok(ResourceState {
            resource_type: ResourceType::new("EksCluster"),
            physical_id: cluster_name.clone(),
            properties: serde_json::json!({
                "cluster_name": cluster_name,
                "cluster_arn": cluster_arn,
                "cluster_endpoint": cluster_endpoint,
                "certificate_authority": certificate_authority,
                "version": self.version,
                "status": "ACTIVE",
                "namespace": self.namespace,
                "auto_mode": true,
                "private_api": true,
                "access_entry_principal_arn": access_entry_principal_arn,
                "deletion_protection": self.deletion_protection,
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        })
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("{}-eks-cluster", self.project));
        let cluster_name = self.cluster_name();

        let current_version = current
            .properties
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // eks:UpdateClusterVersion if version changed
        if current_version != self.version {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .eks
                .update_cluster_version()
                .name(&cluster_name)
                .version(&self.version)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "eks:UpdateClusterVersion: {}",
                        e.into_service_error()
                    ))
                })?;

            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .eks
                .wait_until_cluster_active()
                .name(&cluster_name)
                .wait(Duration::from_secs(1200))
                .await
                .map_err(|e| IacError::AwsSdk(format!("eks:WaitUntilClusterActive: {e}")))?;
        }

        // eks:TagResource for tag updates
        let cluster_arn = current
            .properties
            .get("cluster_arn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if !cluster_arn.is_empty() {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .eks
                .tag_resource()
                .resource_arn(&cluster_arn)
                .set_tags(Some(tags.clone()))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("eks:TagResource: {}", e.into_service_error()))
                })?;
        }

        let admin_arn = match &self.cluster_admin_principal_arn {
            Some(arn) if !arn.is_empty() => arn.clone(),
            _ => current
                .properties
                .get("access_entry_principal_arn")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        };
        if !admin_arn.is_empty() {
            self.ensure_cluster_admin_access(ctx, &cluster_name, &admin_arn)
                .await?;
        }

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "cluster_name": cluster_name,
                "cluster_arn": cluster_arn,
                "cluster_endpoint": current.properties.get("cluster_endpoint").and_then(|v| v.as_str()).unwrap_or(""),
                "certificate_authority": current.properties.get("certificate_authority").and_then(|v| v.as_str()).unwrap_or(""),
                "version": self.version,
                "status": "ACTIVE",
                "namespace": self.namespace,
                "auto_mode": true,
                "private_api": true,
                "access_entry_principal_arn": admin_arn,
                "deletion_protection": self.deletion_protection,
                "tags": tags,
            }),
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

        // 12.6: Delete access entries stored in state
        let access_entry_principal = current
            .properties
            .get("access_entry_principal_arn")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !access_entry_principal.is_empty() {
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .eks
                .delete_access_entry()
                .cluster_name(&cluster_name)
                .principal_arn(access_entry_principal)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let svc_err = e.into_service_error();
                    if svc_err.is_resource_not_found_exception() {
                        tracing::warn!(
                            principal = %access_entry_principal,
                            "access entry not found, skipping"
                        );
                    } else {
                        return Err(IacError::AwsSdk(format!(
                            "eks:DeleteAccessEntry: {svc_err}"
                        )));
                    }
                }
            }
        }

        // 12.5: Disable deletion protection before deleting
        let has_deletion_protection = current
            .properties
            .get("deletion_protection")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if has_deletion_protection {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .eks
                .update_cluster_config()
                .name(&cluster_name)
                .deletion_protection(false)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "eks:UpdateClusterConfig (disable deletion protection): {}",
                        e.into_service_error()
                    ))
                })?;

            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .eks
                .wait_until_cluster_active()
                .name(&cluster_name)
                .wait(Duration::from_secs(300))
                .await
                .map_err(|e| IacError::AwsSdk(format!("eks:WaitUntilClusterActive: {e}")))?;
        }

        // 2. eks:DeleteCluster
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .delete_cluster()
            .name(&cluster_name)
            .send()
            .await
        {
            Ok(_) => {
                ctx.extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .eks
                    .wait_until_cluster_deleted()
                    .name(&cluster_name)
                    .wait(Duration::from_secs(900))
                    .await
                    .map_err(|e| IacError::AwsSdk(format!("eks:WaitUntilClusterDeleted: {e}")))?;
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    tracing::warn!(cluster = %cluster_name, "EKS cluster not found, skipping");
                } else {
                    return Err(IacError::AwsSdk(format!("eks:DeleteCluster: {svc_err}")));
                }
            }
        }

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let cluster_name = self.cluster_name();

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .eks
            .describe_cluster()
            .name(&cluster_name)
            .send()
            .await
        {
            Ok(output) => {
                let cluster = output.cluster();
                let status = cluster
                    .and_then(|c| c.status())
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();

                let cluster_arn = cluster
                    .map(|c| c.arn())
                    .unwrap_or(Some(""))
                    .unwrap_or("")
                    .to_string();
                let cluster_endpoint = cluster
                    .and_then(|c| c.endpoint())
                    .unwrap_or_default()
                    .to_string();
                let certificate_authority = cluster
                    .and_then(|c| c.certificate_authority())
                    .and_then(|ca| ca.data())
                    .unwrap_or_default()
                    .to_string();
                let version = cluster
                    .and_then(|c| c.version())
                    .unwrap_or_default()
                    .to_string();

                Ok(DescribeResult::Present(ResourceState {
                    resource_type: ResourceType::new("EksCluster"),
                    physical_id: cluster_name.clone(),
                    properties: serde_json::json!({
                        "cluster_name": cluster_name,
                        "cluster_arn": cluster_arn,
                        "cluster_endpoint": cluster_endpoint,
                        "certificate_authority": certificate_authority,
                        "version": version,
                        "status": status,
                    }),
                    dependencies: self.dependencies(),
                    created_at: chrono::Utc::now().to_rfc3339(),
                    updated_at: chrono::Utc::now().to_rfc3339(),
                    module: self.module().to_owned(),
                }))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    Ok(DescribeResult::Absent)
                } else {
                    Err(IacError::AwsSdk(format!("eks:DescribeCluster: {svc_err}")))
                }
            }
        }
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_version = current
            .properties
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if current_version != self.version {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff {
                    field: "version".into(),
                    before: Some(current_version.to_string()),
                    after: Some(self.version.clone()),
                }],
            };
        }

        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn eks_resource(version: &str) -> EksClusterResource {
        EksClusterResource::new(
            &crate::ResourceContext {
                project: "tok".into(),
                region: "us-east-1".into(),
                tags: HashMap::new(),
            },
            EksConfig {
                version: version.into(),
                namespace: "tokeira".into(),
                kms_key_arn: None,
                deletion_protection: false,
                bootstrap_admin_permissions: false,
                cluster_admin_principal_arn: None,
            },
            "eks",
        )
    }

    fn cluster_state(properties: serde_json::Value) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: ResourceType::new("EksCluster"),
            physical_id: "tok-eks".into(),
            properties,
            dependencies: Vec::new(),
            created_at: now.clone(),
            updated_at: now,
            module: "eks".into(),
        }
    }

    #[test]
    fn diff_noops_when_version_matches() {
        let resource = eks_resource("1.31");
        let current = cluster_state(serde_json::json!({ "version": "1.31" }));

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::NoChange { .. }));
    }

    #[test]
    fn diff_updates_when_version_changes() {
        let resource = eks_resource("1.32");
        let current = cluster_state(serde_json::json!({ "version": "1.31" }));

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        match change {
            InternalChange::Update { details, .. } => {
                assert_eq!(details[0].field, "version");
                assert_eq!(details[0].before.as_deref(), Some("1.31"));
                assert_eq!(details[0].after.as_deref(), Some("1.32"));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn diff_updates_when_version_missing_from_state() {
        let resource = eks_resource("1.31");
        let current = cluster_state(serde_json::json!({ "cluster_name": "tok-eks" }));

        let change = resource.diff(&current, &ProvisionContext::new("test", HashMap::new()));

        assert!(matches!(change, InternalChange::Update { .. }));
    }
}
