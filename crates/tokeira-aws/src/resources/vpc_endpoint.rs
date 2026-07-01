use std::{collections::HashMap, time::Duration};

use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType, error::IacError,
};

/// Whether a VPC endpoint uses the Gateway or Interface type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointType {
    Gateway,
    Interface,
}

/// Configuration for a single VPC endpoint provider resource.
#[derive(Debug)]
pub struct VpcEndpointConfig {
    /// Full AWS service name (e.g., "com.amazonaws.us-east-1.s3").
    pub service_name: String,
    /// Gateway (S3, DynamoDB) or Interface (everything else).
    pub endpoint_type: EndpointType,
    /// Dependency on the VPC resource for state lookups (vpc_id, subnet_ids, route_table_id).
    pub vpc_dependency: ResourceId,
    /// Optional dependency on a SecurityGroup resource for Interface endpoints.
    /// When present, the SG ID is looked up from state.
    pub security_group_dependency: Option<ResourceId>,
    pub resource_id: Option<ResourceId>,
    pub module: String,
}

/// Generic provider resource that provisions exactly one VPC endpoint.
///
/// Each instance manages a single endpoint. The composition layer creates
/// one `VpcEndpoint` per AWS service that needs a VPC endpoint.
#[derive(Debug)]
pub struct VpcEndpoint {
    /// Short name used for ResourceId (e.g., "s3", "ecr-api", "ecr-dkr").
    pub service_short_name: String,
    pub config: VpcEndpointConfig,
    pub project: String,
    pub region: String,
    pub tags: HashMap<String, String>,
}

impl VpcEndpoint {
    pub fn new(
        service_short_name: String,
        config: VpcEndpointConfig,
        rctx: &crate::ResourceContext,
    ) -> Self {
        Self {
            service_short_name,
            config,
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
        }
    }

    fn is_vpc_endpoint_not_found_error(message: &str) -> bool {
        message.contains("InvalidVpcEndpointId.NotFound")
    }

    fn endpoint_state_summary(endpoint: &aws_sdk_ec2::types::VpcEndpoint) -> &str {
        endpoint
            .state()
            .map(|state| state.as_str())
            .unwrap_or("unknown")
    }

    fn endpoint_is_usable(endpoint: &aws_sdk_ec2::types::VpcEndpoint) -> Result<bool, IacError> {
        match Self::endpoint_state_summary(endpoint) {
            "available" => Ok(true),
            "pending" => Ok(false),
            "pendingAcceptance" => Ok(false),
            "deleting" => Ok(false),
            "deleted" => Ok(false),
            "failed" | "rejected" | "expired" => Err(IacError::ResourceCreationFailed {
                resource_type: "VPC endpoint".into(),
                resource_id: endpoint.vpc_endpoint_id().unwrap_or_default().to_string(),
                details: format!(
                    "endpoint entered terminal failure state '{}'",
                    Self::endpoint_state_summary(endpoint)
                ),
            }),
            _ => Ok(false),
        }
    }

    async fn wait_until_usable(
        &self,
        endpoint_id: &str,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let endpoint_id = endpoint_id.to_string();
        let ec2_client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2;
        super::poll_until(
            Duration::from_secs(5),
            Duration::from_secs(300),
            ctx,
            super::PollTarget {
                resource_desc: "VPC endpoint",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for VPC endpoint availability",
            },
            || async {
                let output = match ec2_client
                    .describe_vpc_endpoints()
                    .vpc_endpoint_ids(&endpoint_id)
                    .send()
                    .await
                {
                    Ok(output) => output,
                    Err(e) => {
                        let svc_err = e.into_service_error();
                        let message = format!("{svc_err}");
                        if Self::is_vpc_endpoint_not_found_error(&message) {
                            return Ok(false);
                        } else {
                            return Err(IacError::AwsSdk(format!(
                                "ec2:DescribeVpcEndpoints: {svc_err}"
                            )));
                        }
                    }
                };
                let Some(endpoint) = output.vpc_endpoints().first() else {
                    return Ok(false);
                };
                Self::endpoint_is_usable(endpoint)
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Resource for VpcEndpoint {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("VpcEndpoint")
    }

    fn resource_id(&self) -> ResourceId {
        self.config
            .resource_id
            .clone()
            .unwrap_or_else(|| ResourceId(format!("vpce-{}", self.service_short_name)))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        let mut deps = vec![self.config.vpc_dependency.clone()];
        if let Some(sg_dep) = &self.config.security_group_dependency {
            deps.push(sg_dep.clone());
        }
        deps
    }

    fn module(&self) -> &str {
        &self.config.module
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // Endpoints are mostly static once created.
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let tag_name = format!("{}-vpce-{}", self.project, self.service_short_name);
        let tags = ctx.resource_tags(&tag_name);

        // Idempotency/adoption path: if an endpoint for this service already
        // exists in the target VPC, adopt it instead of attempting create.
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            tracing::warn!(
                service = %self.config.service_name,
                endpoint = %existing.physical_id,
                "VPC endpoint already exists, adopting"
            );
            let endpoint_id = existing.physical_id.clone();
            self.wait_until_usable(&endpoint_id, ctx).await?;
            return Ok(ResourceState {
                resource_type: ResourceType::new("VpcEndpoint"),
                physical_id: existing.physical_id,
                properties: serde_json::json!({
                    "endpoint_id": existing.properties.get("endpoint_id").and_then(|v| v.as_str()).unwrap_or_default(),
                    "service_name": self.config.service_name,
                    "endpoint_type": match self.config.endpoint_type {
                        EndpointType::Gateway => "Gateway",
                        EndpointType::Interface => "Interface",
                    },
                    "tags": tags,
                }),
                dependencies: self.dependencies(),
                created_at: existing.created_at,
                updated_at: chrono::Utc::now().to_rfc3339(),
                module: self.module().to_owned(),
            });
        }

        let ec2_tags = super::ec2_tags(&tags);

        // Read VPC state for vpc_id, subnet_ids, route_table_id.
        let vpc_state = ctx.get_resource_state(&self.config.vpc_dependency)?;
        let vpc_id = vpc_state
            .properties
            .get("vpc_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IacError::StateNotFound("vpc_id not found in VPC state".into()))?
            .to_string();

        let ep_tag_spec = aws_sdk_ec2::types::TagSpecification::builder()
            .resource_type(aws_sdk_ec2::types::ResourceType::VpcEndpoint)
            .set_tags(Some(ec2_tags))
            .build();

        let endpoint_id = match self.config.endpoint_type {
            EndpointType::Gateway => {
                let route_table_ids: Vec<String> = vpc_state
                    .properties
                    .get("private_route_table_ids")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_else(|| {
                        // Older VPC state documents exposed a singleton
                        // route_table_id. Keep the fallback so in-flight
                        // deployments can roll forward without state surgery.
                        vpc_state
                            .properties
                            .get("route_table_id")
                            .and_then(|v| v.as_str())
                            .filter(|id| !id.is_empty())
                            .map(|id| vec![id.to_string()])
                            .unwrap_or_default()
                    });

                // ec2:CreateVpcEndpoint — Gateway type with route table association.
                let mut builder = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .ec2
                    .create_vpc_endpoint()
                    .vpc_id(&vpc_id)
                    .service_name(&self.config.service_name)
                    .vpc_endpoint_type(aws_sdk_ec2::types::VpcEndpointType::Gateway)
                    .tag_specifications(ep_tag_spec);
                for route_table_id in &route_table_ids {
                    builder = builder.route_table_ids(route_table_id);
                }
                let output = builder.send().await.map_err(|e| {
                    IacError::AwsSdk(format!("ec2:CreateVpcEndpoint: {}", e.into_service_error()))
                })?;

                output
                    .vpc_endpoint()
                    .and_then(|ep| ep.vpc_endpoint_id())
                    .unwrap_or_default()
                    .to_string()
            }
            EndpointType::Interface => {
                let subnet_ids: Vec<String> = vpc_state
                    .properties
                    .get("subnet_ids")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();

                // Look up security group ID from state if dependency is declared.
                let security_group_ids: Vec<String> =
                    if let Some(sg_dep) = &self.config.security_group_dependency {
                        let sg_state = ctx.get_resource_state(sg_dep)?;
                        let sg_id = sg_state
                            .properties
                            .get("security_group_id")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                IacError::StateNotFound(
                                    "security_group_id not found in SG state".into(),
                                )
                            })?
                            .to_string();
                        vec![sg_id]
                    } else {
                        vec![]
                    };

                // ec2:CreateVpcEndpoint — Interface type with SG, subnets, private DNS.
                let mut builder = ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .ec2
                    .create_vpc_endpoint()
                    .vpc_id(&vpc_id)
                    .service_name(&self.config.service_name)
                    .vpc_endpoint_type(aws_sdk_ec2::types::VpcEndpointType::Interface)
                    .private_dns_enabled(true)
                    .tag_specifications(ep_tag_spec);

                for sg_id in &security_group_ids {
                    builder = builder.security_group_ids(sg_id);
                }
                for subnet_id in &subnet_ids {
                    builder = builder.subnet_ids(subnet_id);
                }

                let output = builder.send().await.map_err(|e| {
                    IacError::AwsSdk(format!("ec2:CreateVpcEndpoint: {}", e.into_service_error()))
                })?;

                output
                    .vpc_endpoint()
                    .and_then(|ep| ep.vpc_endpoint_id())
                    .unwrap_or_default()
                    .to_string()
            }
        };

        self.wait_until_usable(&endpoint_id, ctx).await?;

        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: ResourceType::new("VpcEndpoint"),
            physical_id: endpoint_id.clone(),
            properties: serde_json::json!({
                "endpoint_id": endpoint_id,
                "service_name": self.config.service_name,
                "endpoint_type": match self.config.endpoint_type {
                    EndpointType::Gateway => "Gateway",
                    EndpointType::Interface => "Interface",
                },
                "vpc_id": vpc_id,
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
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let tag_name = format!("{}-vpce-{}", self.project, self.service_short_name);
        let tags = ctx.resource_tags(&tag_name);
        let ec2_tags = super::ec2_tags(&tags);

        let endpoint_id = current
            .properties
            .get("endpoint_id")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.physical_id)
            .to_string();

        // ec2:CreateTags to update tags on the endpoint.
        if !endpoint_id.is_empty() {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .create_tags()
                .resources(&endpoint_id)
                .set_tags(Some(ec2_tags))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("ec2:CreateTags: {}", e.into_service_error()))
                })?;
        }

        Ok(ResourceState {
            resource_type: current.resource_type.clone(),
            physical_id: current.physical_id.clone(),
            properties: serde_json::json!({
                "endpoint_id": endpoint_id,
                "service_name": self.config.service_name,
                "endpoint_type": match self.config.endpoint_type {
                    EndpointType::Gateway => "Gateway",
                    EndpointType::Interface => "Interface",
                },
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
        let endpoint_id = current
            .properties
            .get("endpoint_id")
            .and_then(|v| v.as_str())
            .unwrap_or(&current.physical_id)
            .to_string();

        if endpoint_id.is_empty() {
            return Ok(());
        }

        // ec2:DeleteVpcEndpoints
        match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .delete_vpc_endpoints()
            .vpc_endpoint_ids(&endpoint_id)
            .send()
            .await
        {
            Ok(_) => {
                let ec2_client = &ctx
                    .extension::<crate::AwsClients>()
                    .expect("AwsClients")
                    .ec2;
                let endpoint_id_for_poll = endpoint_id.clone();
                super::poll_until(
                    Duration::from_secs(5),
                    Duration::from_secs(300),
                    ctx,
                    super::PollTarget {
                        resource_desc: "VPC endpoint deletion",
                        resource_id: &self.resource_id(),
                        resource_type: self.resource_type(),
                        phase: "waiting for VPC endpoint deletion",
                    },
                    || async {
                        match ec2_client
                            .describe_vpc_endpoints()
                            .vpc_endpoint_ids(&endpoint_id_for_poll)
                            .send()
                            .await
                        {
                            Ok(output) => Ok(output.vpc_endpoints().is_empty()),
                            Err(e) => {
                                let msg = format!("{}", e.into_service_error());
                                if Self::is_vpc_endpoint_not_found_error(&msg) {
                                    Ok(true)
                                } else {
                                    Err(IacError::AwsSdk(format!(
                                        "ec2:DescribeVpcEndpoints: {msg}"
                                    )))
                                }
                            }
                        }
                    },
                )
                .await
            }
            Err(e) => {
                let msg = format!("{}", e.into_service_error());
                if Self::is_vpc_endpoint_not_found_error(&msg) {
                    tracing::warn!(
                        endpoint = %endpoint_id,
                        "VPC endpoint not found, skipping deletion"
                    );
                    Ok(())
                } else {
                    Err(IacError::AwsSdk(format!("ec2:DeleteVpcEndpoints: {msg}")))
                }
            }
        }
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        // Read VPC state for vpc_id to filter endpoints.
        let vpc_state = ctx.get_resource_state(&self.config.vpc_dependency);
        let vpc_id = vpc_state.ok().and_then(|s| {
            s.properties
                .get("vpc_id")
                .and_then(|v| v.as_str())
                .map(String::from)
        });

        // ec2:DescribeVpcEndpoints filtered by service-name (and vpc-id if available).
        let mut req = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .describe_vpc_endpoints()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("service-name")
                    .values(&self.config.service_name)
                    .build(),
            );

        if let Some(ref vid) = vpc_id {
            req = req.filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("vpc-id")
                    .values(vid)
                    .build(),
            );
        }

        let desc_output = req.send().await.map_err(|e| {
            IacError::AwsSdk(format!(
                "ec2:DescribeVpcEndpoints: {}",
                e.into_service_error()
            ))
        })?;

        let endpoints = desc_output.vpc_endpoints();
        if endpoints.is_empty() {
            return Ok(DescribeResult::Absent);
        }

        let ep = endpoints
            .iter()
            .find(|endpoint| {
                endpoint.vpc_endpoint_id().is_some()
                    && !matches!(
                        Self::endpoint_state_summary(endpoint),
                        "deleted" | "deleting"
                    )
            })
            .unwrap_or(&endpoints[0]);
        let endpoint_id = ep.vpc_endpoint_id().unwrap_or_default().to_string();
        let now = chrono::Utc::now().to_rfc3339();

        Ok(DescribeResult::Present(ResourceState {
            resource_type: ResourceType::new("VpcEndpoint"),
            physical_id: endpoint_id.clone(),
            properties: serde_json::json!({
                "endpoint_id": endpoint_id,
                "service_name": self.config.service_name,
                "endpoint_type": match self.config.endpoint_type {
                    EndpointType::Gateway => "Gateway",
                    EndpointType::Interface => "Interface",
                },
                "endpoint_state": Self::endpoint_state_summary(ep),
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        }))
    }
}
