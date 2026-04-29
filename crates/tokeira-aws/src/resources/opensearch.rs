use std::time::Duration;

use tokeira_iac::error::IacError;
use tokeira_iac::{
    InternalChange, ProvisionContext, Resource, ResourceId, ResourceState, ResourceType,
};

/// OpenSearch domain for Temporal visibility.
/// Depends on VPC and the EKS node security group.
pub struct OpenSearchResource {
    project: String,
    instance_type: String,
    instance_count: u32,
    ebs_enabled: bool,
    volume_type: String,
    volume_size_gib: u32,
    module: String,
}

/// OpenSearch-specific configuration passed by the project crate.
#[derive(Debug, Clone)]
pub struct OpenSearchConfig {
    pub instance_type: String,
    pub instance_count: u32,
    pub ebs_enabled: bool,
    pub volume_type: String,
    pub volume_size_gib: u32,
}

impl OpenSearchResource {
    pub fn new(
        rctx: &crate::ResourceContext,
        os: OpenSearchConfig,
        module: impl Into<String>,
    ) -> Self {
        Self {
            project: rctx.project.clone(),
            instance_type: os.instance_type,
            instance_count: os.instance_count,
            ebs_enabled: os.ebs_enabled,
            volume_type: os.volume_type,
            volume_size_gib: os.volume_size_gib,
            module: module.into(),
        }
    }

    fn domain_name(&self) -> String {
        format!("{}-opensearch", self.project)
    }

    fn creation_ready(status: &aws_sdk_opensearch::types::DomainStatus) -> bool {
        let created = status.created().unwrap_or(false);
        let deleted = status.deleted().unwrap_or(false);
        let processing = status.processing().unwrap_or(true);
        let domain_processing_active =
            status.domain_processing_status().is_none_or(|state| {
                *state == aws_sdk_opensearch::types::DomainProcessingStatusType::Active
            });
        let endpoint_present = Self::resolved_endpoint(status).is_some();

        created && !deleted && !processing && domain_processing_active && endpoint_present
    }

    fn creation_status_summary(
        status: &aws_sdk_opensearch::types::DomainStatus,
    ) -> String {
        format!(
            "created={}, deleted={}, processing={}, domain_processing_status={}, endpoint_present={}, endpoint_v2_present={}, endpoints_present={}",
            status.created().unwrap_or(false),
            status.deleted().unwrap_or(false),
            status.processing().unwrap_or(true),
            status
                .domain_processing_status()
                .map(|state| state.as_str().to_string())
                .unwrap_or_else(|| "<none>".into()),
            status
                .endpoint()
                .is_some_and(|endpoint| !endpoint.is_empty()),
            status
                .endpoint_v2()
                .is_some_and(|endpoint| !endpoint.is_empty()),
            status
                .endpoints()
                .is_some_and(|endpoints| !endpoints.is_empty()),
        )
    }

    fn resolved_endpoint(
        status: &aws_sdk_opensearch::types::DomainStatus,
    ) -> Option<String> {
        status
            .endpoint()
            .filter(|endpoint| !endpoint.is_empty())
            .map(str::to_string)
            .or_else(|| {
                status
                    .endpoint_v2()
                    .filter(|endpoint| !endpoint.is_empty())
                    .map(str::to_string)
            })
            .or_else(|| {
                status
                    .endpoints()
                    .and_then(|endpoints| endpoints.values().next().cloned())
                    .filter(|endpoint| !endpoint.is_empty())
            })
    }

    fn state_from_status(
        &self,
        status: &aws_sdk_opensearch::types::DomainStatus,
        tags: serde_json::Value,
        created_at: String,
    ) -> ResourceState {
        let domain_endpoint = Self::resolved_endpoint(status).unwrap_or_default();
        let cluster_config = status.cluster_config();
        let ebs_options = status.ebs_options();
        let instance_type = cluster_config
            .and_then(|cfg| cfg.instance_type())
            .map(|t| t.as_str().to_string())
            .unwrap_or_else(|| self.instance_type.clone());
        let instance_count = cluster_config
            .and_then(|cfg| cfg.instance_count())
            .unwrap_or(self.instance_count as i32) as u32;
        let ebs_enabled = ebs_options
            .and_then(|ebs| ebs.ebs_enabled())
            .unwrap_or(self.ebs_enabled);
        let volume_type = ebs_options
            .and_then(|ebs| ebs.volume_type())
            .map(|t| t.as_str().to_string())
            .unwrap_or_else(|| self.volume_type.clone());
        let volume_size_gib = ebs_options
            .and_then(|ebs| ebs.volume_size())
            .unwrap_or(self.volume_size_gib as i32) as u32;

        ResourceState {
            resource_type: ResourceType::new("OpenSearchDomain"),
            physical_id: self.domain_name(),
            properties: serde_json::json!({
                "domain_arn": status.arn(),
                "domain_endpoint": domain_endpoint,
                "domain_id": status.domain_id(),
                "instance_type": instance_type,
                "instance_count": instance_count,
                "ebs_enabled": ebs_enabled,
                "volume_type": volume_type,
                "volume_size_gib": volume_size_gib,
                "encryption_at_rest": true,
                "node_to_node_encryption": true,
                "enforce_https": true,
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at,
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for OpenSearchResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("OpenSearchDomain")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("{}-opensearch", self.project))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            ResourceId(format!("{}-vpc", self.project)),
            ResourceId(format!("sg-{}-eks-nodes-sg", self.project)),
        ]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("{}-opensearch", self.project));
        let os_tags = super::opensearch_tags(&tags);
        let domain_name = self.domain_name();

        // Read VPC state for subnet_ids
        let vpc_state =
            ctx.get_resource_state(&ResourceId(format!("{}-vpc", self.project)))?;
        let subnet_ids: Vec<String> = vpc_state
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // Read security group state for security_group_id
        let sg_state = ctx.get_resource_state(&ResourceId(format!(
            "sg-{}-eks-nodes-sg",
            self.project
        )))?;
        let sg_id = sg_state
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("security_group_id not found in SG state".into())
            })?
            .to_string();

        // opensearch:CreateDomain
        let instance_type: aws_sdk_opensearch::types::OpenSearchPartitionInstanceType =
            self.instance_type.as_str().into();

        let create_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .opensearch
            .create_domain()
            .domain_name(&domain_name)
            .engine_version("OpenSearch_2.17")
            .cluster_config(
                aws_sdk_opensearch::types::ClusterConfig::builder()
                    .instance_type(instance_type)
                    .instance_count(self.instance_count as i32)
                    .dedicated_master_enabled(false)
                    .zone_awareness_enabled(subnet_ids.len() > 1)
                    .build(),
            )
            .ebs_options(
                aws_sdk_opensearch::types::EbsOptions::builder()
                    .ebs_enabled(self.ebs_enabled)
                    .volume_type(self.volume_type.as_str().into())
                    .volume_size(self.volume_size_gib as i32)
                    .build(),
            )
            .vpc_options(
                aws_sdk_opensearch::types::VpcOptions::builder()
                    .set_subnet_ids(Some(subnet_ids))
                    .security_group_ids(&sg_id)
                    .build(),
            )
            .encryption_at_rest_options(
                aws_sdk_opensearch::types::EncryptionAtRestOptions::builder()
                    .enabled(true)
                    .build(),
            )
            .node_to_node_encryption_options(
                aws_sdk_opensearch::types::NodeToNodeEncryptionOptions::builder()
                    .enabled(true)
                    .build(),
            )
            .domain_endpoint_options(
                aws_sdk_opensearch::types::DomainEndpointOptions::builder()
                    .enforce_https(true)
                    .tls_security_policy(
                        aws_sdk_opensearch::types::TlsSecurityPolicy::PolicyMinTls12201907,
                    )
                    .build(),
            )
            .set_tag_list(Some(os_tags))
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "opensearch:CreateDomain: {}",
                    e.into_service_error()
                ))
            })?;

        let domain_arn = create_output
            .domain_status()
            .map(|d| d.arn().to_string())
            .unwrap_or_default();

        let domain_id = create_output
            .domain_status()
            .map(|d| d.domain_id().to_string())
            .unwrap_or_default();

        // Poll until domain is active (processing=false), 30s interval, 30min timeout
        let dn = domain_name.clone();
        let os_client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .opensearch;
        super::poll_until(
            Duration::from_secs(30),
            Duration::from_secs(1800),
            ctx,
            super::PollTarget {
                resource_desc: "OpenSearch domain",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for domain to become active",
            },
            || async {
                let desc = os_client
                    .describe_domain()
                    .domain_name(&dn)
                    .send()
                    .await
                    .map_err(|e| {
                        let svc_err = e.into_service_error();
                        if svc_err.is_resource_not_found_exception() {
                            IacError::ResourceCreationFailed {
                                resource_type: "OpenSearch domain".into(),
                                resource_id: dn.clone(),
                                details: "domain disappeared during provisioning".into(),
                            }
                        } else {
                            IacError::AwsSdk(format!(
                                "opensearch:DescribeDomain: {svc_err}"
                            ))
                        }
                    })?;

                let status = desc.domain_status().ok_or_else(|| {
                    IacError::ResourceCreationFailed {
                        resource_type: "OpenSearch domain".into(),
                        resource_id: dn.clone(),
                        details: "DescribeDomain returned no domain status".into(),
                    }
                })?;

                if status.deleted().unwrap_or(false) {
                    return Err(IacError::ResourceCreationFailed {
                        resource_type: "OpenSearch domain".into(),
                        resource_id: dn.clone(),
                        details: format!(
                            "domain entered deleted state during provisioning ({})",
                            Self::creation_status_summary(status)
                        ),
                    });
                }

                Ok(Self::creation_ready(status))
            },
        )
        .await?;

        // Fetch the endpoint after domain is active
        let desc_output = os_client
            .describe_domain()
            .domain_name(&domain_name)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "opensearch:DescribeDomain: {}",
                    e.into_service_error()
                ))
            })?;

        let final_status = desc_output.domain_status().ok_or_else(|| {
            IacError::ResourceCreationFailed {
                resource_type: "OpenSearch domain".into(),
                resource_id: domain_name.clone(),
                details: "DescribeDomain returned no final domain status".into(),
            }
        })?;

        if !Self::creation_ready(final_status) {
            return Err(IacError::ResourceCreationFailed {
                resource_type: "OpenSearch domain".into(),
                resource_id: domain_name.clone(),
                details: format!(
                    "domain did not reach a usable created state ({})",
                    Self::creation_status_summary(final_status)
                ),
            });
        }

        let created_at = chrono::Utc::now().to_rfc3339();
        let mut state = self.state_from_status(
            final_status,
            serde_json::to_value(&tags).unwrap_or(serde_json::Value::Null),
            created_at,
        );
        if state
            .properties
            .get("domain_arn")
            .and_then(|v| v.as_str())
            .is_none_or(str::is_empty)
        {
            state.properties["domain_arn"] = serde_json::Value::String(domain_arn);
        }
        if state
            .properties
            .get("domain_id")
            .and_then(|v| v.as_str())
            .is_none_or(str::is_empty)
        {
            state.properties["domain_id"] = serde_json::Value::String(domain_id);
        }

        Ok(state)
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("{}-opensearch", self.project));
        let domain_name = self.domain_name();

        let current_type = current
            .properties
            .get("instance_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let current_count = current
            .properties
            .get("instance_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let current_ebs_enabled = current
            .properties
            .get("ebs_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let current_volume_type = current
            .properties
            .get("volume_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let current_volume_size_gib = current
            .properties
            .get("volume_size_gib")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        // opensearch:UpdateDomainConfig for instance changes
        if current_type != self.instance_type
            || current_count != self.instance_count
            || current_ebs_enabled != self.ebs_enabled
            || current_volume_type != self.volume_type
            || current_volume_size_gib != self.volume_size_gib
        {
            let instance_type: aws_sdk_opensearch::types::OpenSearchPartitionInstanceType =
                self.instance_type.as_str().into();

            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .opensearch
                .update_domain_config()
                .domain_name(&domain_name)
                .cluster_config(
                    aws_sdk_opensearch::types::ClusterConfig::builder()
                        .instance_type(instance_type)
                        .instance_count(self.instance_count as i32)
                        .build(),
                )
                .ebs_options(
                    aws_sdk_opensearch::types::EbsOptions::builder()
                        .ebs_enabled(self.ebs_enabled)
                        .volume_type(self.volume_type.as_str().into())
                        .volume_size(self.volume_size_gib as i32)
                        .build(),
                )
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "opensearch:UpdateDomainConfig: {}",
                        e.into_service_error()
                    ))
                })?;

            let os_client = &ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .opensearch;
            let dn = domain_name.clone();
            super::poll_until(
                Duration::from_secs(30),
                Duration::from_secs(1800),
                ctx,
                super::PollTarget {
                    resource_desc: "OpenSearch domain update",
                    resource_id: &self.resource_id(),
                    resource_type: self.resource_type(),
                    phase: "waiting for domain update to become active",
                },
                || async {
                    let desc = os_client
                        .describe_domain()
                        .domain_name(&dn)
                        .send()
                        .await
                        .map_err(|e| {
                            let svc_err = e.into_service_error();
                            if svc_err.is_resource_not_found_exception() {
                                IacError::ResourceCreationFailed {
                                    resource_type: "OpenSearch domain update".into(),
                                    resource_id: dn.clone(),
                                    details: "domain disappeared during update".into(),
                                }
                            } else {
                                IacError::AwsSdk(format!(
                                    "opensearch:DescribeDomain: {svc_err}"
                                ))
                            }
                        })?;

                    let status = desc.domain_status().ok_or_else(|| {
                        IacError::ResourceCreationFailed {
                            resource_type: "OpenSearch domain update".into(),
                            resource_id: dn.clone(),
                            details: "DescribeDomain returned no domain status".into(),
                        }
                    })?;

                    if status.deleted().unwrap_or(false) {
                        return Err(IacError::ResourceCreationFailed {
                            resource_type: "OpenSearch domain update".into(),
                            resource_id: dn.clone(),
                            details: format!(
                                "domain entered deleted state during update ({})",
                                Self::creation_status_summary(status)
                            ),
                        });
                    }

                    Ok(Self::creation_ready(status))
                },
            )
            .await?;
        }

        // opensearch:AddTags for tag updates
        let domain_arn = current
            .properties
            .get("domain_arn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if !domain_arn.is_empty() {
            let os_tags = super::opensearch_tags(&tags);
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .opensearch
                .add_tags()
                .arn(&domain_arn)
                .set_tag_list(Some(os_tags))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "opensearch:AddTags: {}",
                        e.into_service_error()
                    ))
                })?;
        }

        let desc_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .opensearch
            .describe_domain()
            .domain_name(&domain_name)
            .send()
            .await
            .map_err(|e| {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    IacError::ResourceCreationFailed {
                        resource_type: "OpenSearch domain update".into(),
                        resource_id: domain_name.clone(),
                        details: "domain disappeared before final refresh".into(),
                    }
                } else {
                    IacError::AwsSdk(format!("opensearch:DescribeDomain: {svc_err}"))
                }
            })?;

        let final_status = desc_output.domain_status().ok_or_else(|| {
            IacError::ResourceCreationFailed {
                resource_type: "OpenSearch domain update".into(),
                resource_id: domain_name.clone(),
                details: "DescribeDomain returned no final domain status".into(),
            }
        })?;

        if !Self::creation_ready(final_status) {
            return Err(IacError::ResourceCreationFailed {
                resource_type: "OpenSearch domain update".into(),
                resource_id: domain_name.clone(),
                details: format!(
                    "domain did not reach a usable updated state ({})",
                    Self::creation_status_summary(final_status)
                ),
            });
        }

        let mut state = self.state_from_status(
            final_status,
            serde_json::to_value(&tags).unwrap_or(serde_json::Value::Null),
            current.created_at.clone(),
        );
        if state
            .properties
            .get("domain_arn")
            .and_then(|v| v.as_str())
            .is_none_or(str::is_empty)
        {
            state.properties["domain_arn"] = serde_json::Value::String(domain_arn);
        }
        state.physical_id = current.physical_id.clone();
        Ok(state)
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let domain_name = self.domain_name();

        // opensearch:DeleteDomain
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .opensearch
            .delete_domain()
            .domain_name(&domain_name)
            .send()
            .await
        {
            Ok(_) => {
                // Poll until the domain is fully gone (30s interval, 30min timeout).
                // `deleted=true` is only an in-progress state; VPC attachments can still exist.
                let dn = domain_name.clone();
                let os_client = &ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .opensearch;
                super::poll_until(
                    Duration::from_secs(30),
                    Duration::from_secs(1800),
                    ctx,
                    super::PollTarget {
                        resource_desc: "OpenSearch domain deletion",
                        resource_id: &self.resource_id(),
                        resource_type: self.resource_type(),
                        phase: "waiting for domain deletion",
                    },
                    || async {
                        match os_client.describe_domain().domain_name(&dn).send().await {
                            Ok(_) => Ok(false),
                            Err(e) => {
                                let svc_err = e.into_service_error();
                                if svc_err.is_resource_not_found_exception() {
                                    Ok(true) // Domain is gone
                                } else {
                                    Err(IacError::AwsSdk(format!(
                                        "opensearch:DescribeDomain: {svc_err}"
                                    )))
                                }
                            }
                        }
                    },
                )
                .await?;
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    tracing::warn!(domain = %domain_name, "OpenSearch domain not found, skipping");
                } else {
                    return Err(IacError::AwsSdk(format!(
                        "opensearch:DeleteDomain: {svc_err}"
                    )));
                }
            }
        }

        Ok(())
    }

    async fn describe(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<Option<ResourceState>, IacError> {
        let domain_name = self.domain_name();

        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .opensearch
            .describe_domain()
            .domain_name(&domain_name)
            .send()
            .await
        {
            Ok(output) => {
                let status = output.domain_status().ok_or_else(|| {
                    IacError::AwsSdk(
                        "opensearch:DescribeDomain: response contained no domain status"
                            .into(),
                    )
                })?;
                Ok(Some(self.state_from_status(
                    status,
                    serde_json::Value::Null,
                    chrono::Utc::now().to_rfc3339(),
                )))
            }
            Err(e) => {
                let svc_err = e.into_service_error();
                if svc_err.is_resource_not_found_exception() {
                    Ok(None)
                } else {
                    Err(IacError::AwsSdk(format!(
                        "opensearch:DescribeDomain: {svc_err}"
                    )))
                }
            }
        }
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_type = current
            .properties
            .get("instance_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let current_count = current
            .properties
            .get("instance_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let current_ebs_enabled = current
            .properties
            .get("ebs_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let current_volume_type = current
            .properties
            .get("volume_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let current_volume_size_gib = current
            .properties
            .get("volume_size_gib")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        if current_type != self.instance_type
            || current_count != self.instance_count
            || current_ebs_enabled != self.ebs_enabled
            || current_volume_type != self.volume_type
            || current_volume_size_gib != self.volume_size_gib
        {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: format!(
                    "instance/ebs config changed: {}x{} {}:{}:{} → {}x{} {}:{}:{}",
                    current_count,
                    current_type,
                    current_ebs_enabled,
                    current_volume_type,
                    current_volume_size_gib,
                    self.instance_count,
                    self.instance_type,
                    self.ebs_enabled,
                    self.volume_type,
                    self.volume_size_gib,
                ),
            };
        }

        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
