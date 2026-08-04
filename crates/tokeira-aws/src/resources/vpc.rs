use std::time::Duration;

use tokeira_iac::{
    ChangeKind, ChangeSemantics, Citation, DescribeResult, InternalChange, ProvisionContext,
    Resource, ResourceId, ResourceState, ResourceType, SemanticsContext, error::IacError,
};

/// VPC resource: creates VPC, private subnets, and route tables.
/// Private-only design — no NAT Gateway, no IGW, no public subnets.
/// All AWS service traffic routes through VPC endpoints.
/// Root resource with no dependencies.
#[derive(Debug)]
pub struct VpcResource {
    project: String,
    cidr: String,
    availability_zones: Vec<String>,
    module: String,
}

/// VPC-specific configuration passed by the caller.
#[derive(Debug, Clone)]
pub struct VpcConfig {
    pub cidr: String,
    pub availability_zones: Vec<String>,
}

impl VpcResource {
    pub fn new(rctx: &crate::ResourceContext, vpc: VpcConfig, module: impl Into<String>) -> Self {
        Self {
            project: rctx.project.clone(),
            cidr: vpc.cidr,
            availability_zones: vpc.availability_zones,
            module: module.into(),
        }
    }

    /// Returns whether the VPC create() explicitly enables DNS hostnames.
    pub fn enables_dns_hostnames(&self) -> bool {
        true
    }

    /// Returns whether the VPC create() explicitly enables DNS support.
    pub fn enables_dns_support(&self) -> bool {
        true
    }

    fn is_route_table_not_found(message: &str) -> bool {
        message.contains("InvalidRouteTableID.NotFound")
    }

    fn is_subnet_not_found(message: &str) -> bool {
        message.contains("InvalidSubnetID.NotFound")
    }

    fn is_vpc_not_found(message: &str) -> bool {
        message.contains("InvalidVpcID.NotFound")
    }
}

/// Calculate a /24 subnet CIDR from the VPC CIDR base for a given AZ index.
/// E.g., for "10.0.0.0/16" with index 0 → "10.0.0.0/24", index 1 → "10.0.1.0/24".
fn subnet_cidr(vpc_cidr: &str, index: usize) -> String {
    let parts: Vec<&str> = vpc_cidr.split('.').collect();
    format!("{}.{}.{}.0/24", parts[0], parts[1], index)
}

#[async_trait::async_trait]
impl Resource for VpcResource {
    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics {
        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::create — ec2:CreateVpc, private subnets per AZ, route tables and \
             associations (private-only: no IGW, no NAT, no public subnets)"
        ));
        const UPDATE: Citation = Citation::code(concat!(
            module_path!(),
            "::update — tags only (ec2:CreateTags on the VPC and each subnet); \
             the CIDR is immutable and the subnet/route-table topology is fixed \
             at create"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::delete — tear down associations, route tables, subnets, then \
             ec2:DeleteVpc"
        ));
        let mut semantics = super::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE);
        if matches!(ctx.kind, ChangeKind::Create) {
            semantics.provider_assigned = vec![
                "vpc_id".into(),
                "subnet_ids".into(),
                "route_table_id".into(),
                "route_table_association_ids".into(),
            ];
        }
        semantics
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("Vpc")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("{}-vpc", self.project))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let tags = ctx.resource_tags(&format!("{}-vpc", self.project));
        let ec2_tags = super::ec2_tags(&tags);

        // 1. ec2:CreateVpc
        let vpc_tag_spec = aws_sdk_ec2::types::TagSpecification::builder()
            .resource_type(aws_sdk_ec2::types::ResourceType::Vpc)
            .set_tags(Some(ec2_tags.clone()))
            .build();

        let create_vpc_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .create_vpc()
            .cidr_block(&self.cidr)
            .tag_specifications(vpc_tag_spec)
            .send()
            .await
            .map_err(|e| IacError::AwsSdk(format!("ec2:CreateVpc: {}", e.into_service_error())))?;

        let vpc_id = create_vpc_output
            .vpc()
            .and_then(|v| v.vpc_id())
            .ok_or_else(|| IacError::AwsSdk("ec2:CreateVpc: no VPC ID in response".into()))?
            .to_string();

        // 2. ec2:ModifyVpcAttribute — explicitly enable DNS hostnames and DNS support
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .modify_vpc_attribute()
            .vpc_id(&vpc_id)
            .enable_dns_hostnames(
                aws_sdk_ec2::types::AttributeBooleanValue::builder()
                    .value(true)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ec2:ModifyVpcAttribute(EnableDnsHostnames): {}",
                    e.into_service_error()
                ))
            })?;

        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .modify_vpc_attribute()
            .vpc_id(&vpc_id)
            .enable_dns_support(
                aws_sdk_ec2::types::AttributeBooleanValue::builder()
                    .value(true)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ec2:ModifyVpcAttribute(EnableDnsSupport): {}",
                    e.into_service_error()
                ))
            })?;

        // 3. ec2:CreateSubnet for each AZ (private only)
        let mut subnet_ids: Vec<String> = Vec::new();
        for (i, az) in self.availability_zones.iter().enumerate() {
            let cidr = subnet_cidr(&self.cidr, i);
            let mut subnet_tags = tags.clone();
            subnet_tags.insert("kubernetes.io/role/internal-elb".into(), "1".into());
            let subnet_tag_spec = aws_sdk_ec2::types::TagSpecification::builder()
                .resource_type(aws_sdk_ec2::types::ResourceType::Subnet)
                .set_tags(Some(super::ec2_tags(&subnet_tags)))
                .build();

            let create_subnet_output = ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .create_subnet()
                .vpc_id(&vpc_id)
                .cidr_block(&cidr)
                .availability_zone(az)
                .tag_specifications(subnet_tag_spec)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("ec2:CreateSubnet: {}", e.into_service_error()))
                })?;

            let subnet_id = create_subnet_output
                .subnet()
                .and_then(|s| s.subnet_id())
                .ok_or_else(|| {
                    IacError::AwsSdk("ec2:CreateSubnet: no subnet ID in response".into())
                })?
                .to_string();

            subnet_ids.push(subnet_id);
        }

        // 4. ec2:CreateRouteTable (local VPC route is added automatically by AWS)
        let rt_tag_spec = aws_sdk_ec2::types::TagSpecification::builder()
            .resource_type(aws_sdk_ec2::types::ResourceType::RouteTable)
            .set_tags(Some(ec2_tags.clone()))
            .build();

        let rt_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .create_route_table()
            .vpc_id(&vpc_id)
            .tag_specifications(rt_tag_spec)
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ec2:CreateRouteTable: {}", e.into_service_error()))
            })?;

        let route_table_id = rt_output
            .route_table()
            .and_then(|rt| rt.route_table_id())
            .ok_or_else(|| {
                IacError::AwsSdk("ec2:CreateRouteTable: no route table ID in response".into())
            })?
            .to_string();

        // 5. ec2:AssociateRouteTable for each private subnet
        let mut association_ids: Vec<String> = Vec::new();
        for subnet_id in &subnet_ids {
            let assoc_output = ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .associate_route_table()
                .route_table_id(&route_table_id)
                .subnet_id(subnet_id)
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!(
                        "ec2:AssociateRouteTable: {}",
                        e.into_service_error()
                    ))
                })?;

            if let Some(assoc_id) = assoc_output.association_id() {
                association_ids.push(assoc_id.to_string());
            }
        }

        Ok(ResourceState {
            resource_type: ResourceType::new("Vpc"),
            physical_id: vpc_id.clone(),
            properties: serde_json::json!({
                "vpc_id": vpc_id,
                "subnet_ids": subnet_ids,
                "route_table_id": route_table_id,
                "private_route_table_ids": [route_table_id],
                "route_table_association_ids": association_ids,
                "cidr": self.cidr,
                "availability_zones": self.availability_zones,
                "tags": tags,
            }),
            dependencies: vec![],
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
        let tags = ctx.resource_tags(&format!("{}-vpc", self.project));
        let ec2_tags = super::ec2_tags(&tags);

        // VPC CIDR is immutable — only tags can be updated.
        let vpc_id = current
            .properties
            .get("vpc_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let subnet_ids: Vec<String> = current
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // ec2:CreateTags on VPC
        if !vpc_id.is_empty() {
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .create_tags()
                .resources(&vpc_id)
                .set_tags(Some(ec2_tags.clone()))
                .send()
                .await
                .map_err(|e| {
                    IacError::AwsSdk(format!("ec2:CreateTags: {}", e.into_service_error()))
                })?;
        }

        // ec2:CreateTags on subnets
        for subnet_id in &subnet_ids {
            let mut subnet_tags = tags.clone();
            subnet_tags.insert("kubernetes.io/role/internal-elb".into(), "1".into());
            ctx.extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .create_tags()
                .resources(subnet_id)
                .set_tags(Some(super::ec2_tags(&subnet_tags)))
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
                "vpc_id": vpc_id,
                "subnet_ids": subnet_ids,
                "route_table_id": current.properties.get("route_table_id").and_then(|v| v.as_str()).unwrap_or(""),
                "private_route_table_ids": current.properties.get("private_route_table_ids").cloned().unwrap_or_else(|| {
                    let route_table_id = current.properties.get("route_table_id").and_then(|v| v.as_str()).unwrap_or("");
                    serde_json::json!([route_table_id])
                }),
                "route_table_association_ids": current.properties.get("route_table_association_ids").cloned().unwrap_or(serde_json::json!([])),
                "cidr": self.cidr,
                "availability_zones": self.availability_zones,
                "tags": tags,
            }),
            dependencies: vec![],
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
        let vpc_id = current
            .properties
            .get("vpc_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let subnet_ids: Vec<String> = current
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let route_table_id = current
            .properties
            .get("route_table_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let association_ids: Vec<String> = current
            .properties
            .get("route_table_association_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        // 1. ec2:DisassociateRouteTable for each association
        for assoc_id in &association_ids {
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .disassociate_route_table()
                .association_id(assoc_id)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if msg.contains("InvalidAssociationID.NotFound") {
                        tracing::warn!(association = %assoc_id, "route table association not found, skipping");
                    } else {
                        return Err(IacError::AwsSdk(format!(
                            "ec2:DisassociateRouteTable: {msg}"
                        )));
                    }
                }
            }
        }

        if !route_table_id.is_empty() {
            let ec2_client = &ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2;
            let route_table_id_for_poll = route_table_id.clone();
            super::poll_until(
                Duration::from_secs(5),
                Duration::from_secs(600),
                ctx,
                super::PollTarget {
                    resource_desc: "route table disassociation",
                    resource_id: &self.resource_id(),
                    resource_type: self.resource_type(),
                    phase: "waiting for route table disassociation",
                },
                || async {
                    match ec2_client
                        .describe_route_tables()
                        .route_table_ids(&route_table_id_for_poll)
                        .send()
                        .await
                    {
                        Ok(output) => {
                            let associations_cleared = output
                                .route_tables()
                                .first()
                                .map(|rt| {
                                    rt.associations()
                                        .iter()
                                        .filter(|assoc| !assoc.main().unwrap_or(false))
                                        .count()
                                        == 0
                                })
                                .unwrap_or(true);
                            Ok(associations_cleared)
                        }
                        Err(e) => {
                            let msg = format!("{}", e.into_service_error());
                            if Self::is_route_table_not_found(&msg) {
                                Ok(true)
                            } else {
                                Err(IacError::AwsSdk(format!("ec2:DescribeRouteTables: {msg}")))
                            }
                        }
                    }
                },
            )
            .await?;
        }

        // 2. ec2:DeleteRouteTable
        if !route_table_id.is_empty() {
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .delete_route_table()
                .route_table_id(&route_table_id)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if msg.contains("InvalidRouteTableID.NotFound") {
                        tracing::warn!(route_table = %route_table_id, "route table not found, skipping");
                    } else {
                        return Err(IacError::AwsSdk(format!("ec2:DeleteRouteTable: {msg}")));
                    }
                }
            }
        }

        // 3. ec2:DeleteSubnet for each subnet
        for subnet_id in &subnet_ids {
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .delete_subnet()
                .subnet_id(subnet_id)
                .send()
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if msg.contains("InvalidSubnetID.NotFound") {
                        tracing::warn!(subnet = %subnet_id, "subnet not found, skipping");
                    } else {
                        return Err(IacError::AwsSdk(format!("ec2:DeleteSubnet: {msg}")));
                    }
                }
            }

            let ec2_client = &ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2;
            let subnet_id_for_poll = subnet_id.clone();
            super::poll_until(
                Duration::from_secs(5),
                Duration::from_secs(600),
                ctx,
                super::PollTarget {
                    resource_desc: "subnet deletion",
                    resource_id: &self.resource_id(),
                    resource_type: self.resource_type(),
                    phase: "waiting for subnet deletion",
                },
                || async {
                    match ec2_client
                        .describe_subnets()
                        .subnet_ids(&subnet_id_for_poll)
                        .send()
                        .await
                    {
                        Ok(output) => Ok(output.subnets().is_empty()),
                        Err(e) => {
                            let msg = format!("{}", e.into_service_error());
                            if Self::is_subnet_not_found(&msg) {
                                Ok(true)
                            } else {
                                Err(IacError::AwsSdk(format!("ec2:DescribeSubnets: {msg}")))
                            }
                        }
                    }
                },
            )
            .await?;
        }

        // 4. ec2:DeleteVpc
        if !vpc_id.is_empty() {
            match ctx
                .extension::<crate::AwsClients>()
                .expect("AwsClients")
                .ec2
                .delete_vpc()
                .vpc_id(&vpc_id)
                .send()
                .await
            {
                Ok(_) => {
                    let ec2_client = &ctx
                        .extension::<crate::AwsClients>()
                        .expect("AwsClients")
                        .ec2;
                    let vpc_id_for_poll = vpc_id.clone();
                    super::poll_until(
                        Duration::from_secs(5),
                        Duration::from_secs(600),
                        ctx,
                        super::PollTarget {
                            resource_desc: "VPC deletion",
                            resource_id: &self.resource_id(),
                            resource_type: self.resource_type(),
                            phase: "waiting for VPC deletion",
                        },
                        || async {
                            match ec2_client
                                .describe_vpcs()
                                .vpc_ids(&vpc_id_for_poll)
                                .send()
                                .await
                            {
                                Ok(output) => Ok(output.vpcs().is_empty()),
                                Err(e) => {
                                    let msg = format!("{}", e.into_service_error());
                                    if Self::is_vpc_not_found(&msg) {
                                        Ok(true)
                                    } else {
                                        Err(IacError::AwsSdk(format!("ec2:DescribeVpcs: {msg}")))
                                    }
                                }
                            }
                        },
                    )
                    .await?;
                }
                Err(e) => {
                    let msg = format!("{}", e.into_service_error());
                    if Self::is_vpc_not_found(&msg) {
                        tracing::warn!(vpc = %vpc_id, "VPC not found, skipping");
                    } else {
                        return Err(IacError::AwsSdk(format!("ec2:DeleteVpc: {msg}")));
                    }
                }
            }
        }

        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let vpc_name = format!("{}-vpc", self.project);

        // ec2:DescribeVpcs with tag filter Name={project}-vpc
        let desc_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .describe_vpcs()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("tag:Name")
                    .values(&vpc_name)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ec2:DescribeVpcs: {}", e.into_service_error()))
            })?;

        let vpcs = desc_output.vpcs();
        if vpcs.is_empty() {
            return Ok(DescribeResult::Absent);
        }

        let vpc = &vpcs[0];
        let vpc_id = vpc.vpc_id().unwrap_or_default().to_string();

        // Describe subnets in this VPC
        let subnets_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .describe_subnets()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("vpc-id")
                    .values(&vpc_id)
                    .build(),
            )
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("tag:Name")
                    .values(&vpc_name)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("ec2:DescribeSubnets: {}", e.into_service_error()))
            })?;

        let subnet_ids: Vec<String> = subnets_output
            .subnets()
            .iter()
            .filter_map(|s| s.subnet_id().map(|id| id.to_string()))
            .collect();

        // Describe route tables in this VPC (non-main)
        let rt_output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .ec2
            .describe_route_tables()
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("vpc-id")
                    .values(&vpc_id)
                    .build(),
            )
            .filters(
                aws_sdk_ec2::types::Filter::builder()
                    .name("tag:Name")
                    .values(&vpc_name)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "ec2:DescribeRouteTables: {}",
                    e.into_service_error()
                ))
            })?;

        let route_table_id = rt_output
            .route_tables()
            .first()
            .and_then(|rt| rt.route_table_id())
            .unwrap_or_default()
            .to_string();
        let route_table_association_ids: Vec<String> = rt_output
            .route_tables()
            .first()
            .map(|rt| {
                rt.associations()
                    .iter()
                    .filter(|assoc| !assoc.main().unwrap_or(false))
                    .filter_map(|assoc| assoc.route_table_association_id().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        Ok(DescribeResult::Present(ResourceState {
            resource_type: ResourceType::new("Vpc"),
            physical_id: vpc_id.clone(),
            properties: serde_json::json!({
                "vpc_id": vpc_id,
                "subnet_ids": subnet_ids,
                "route_table_id": route_table_id,
                "private_route_table_ids": [route_table_id],
                "route_table_association_ids": route_table_association_ids,
                "cidr": vpc.cidr_block().unwrap_or_default(),
                "availability_zones": self.availability_zones,
            }),
            dependencies: vec![],
            created_at: chrono::Utc::now().to_rfc3339(),
            updated_at: chrono::Utc::now().to_rfc3339(),
            module: self.module().to_owned(),
        }))
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        // VPC CIDR is immutable. Check if AZs or tags changed.
        let current_cidr = current
            .properties
            .get("cidr")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if current_cidr != self.cidr {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff {
                    field: "cidr".into(),
                    before: Some(current_cidr.to_string()),
                    after: Some(self.cidr.clone()),
                }],
            };
        }

        let current_azs: Vec<String> = current
            .properties
            .get("availability_zones")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        if current_azs != self.availability_zones {
            return InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "availability zones changed",
                )],
            };
        }

        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
