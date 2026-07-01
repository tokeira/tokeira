use std::time::Duration;

use aws_sdk_elasticloadbalancingv2::types::{
    Action, ActionTypeEnum, LoadBalancerSchemeEnum, LoadBalancerStateEnum, LoadBalancerTypeEnum,
    Matcher, ProtocolEnum, RuleCondition, TargetTypeEnum,
};
use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType,
    error::IacError,
};

/// Listener mode for the internal edge ALB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlbListenerMode {
    Http2,
    Https,
}

impl AlbListenerMode {
    pub fn listener_port(self) -> u16 {
        match self {
            Self::Http2 => 80,
            Self::Https => 443,
        }
    }

    fn listener_protocol(self) -> ProtocolEnum {
        match self {
            Self::Http2 => ProtocolEnum::Http,
            Self::Https => ProtocolEnum::Https,
        }
    }
}

/// Internal application load balancer for private edge ingress.
#[derive(Debug)]
pub struct AlbResource {
    name: String,
    module: String,
    vpc_dependency: ResourceId,
    security_group_dependency: ResourceId,
}

impl AlbResource {
    pub fn new(
        name: String,
        module: String,
        vpc_dependency: ResourceId,
        security_group_dependency: ResourceId,
    ) -> Self {
        Self {
            name,
            module,
            vpc_dependency,
            security_group_dependency,
        }
    }

    fn state_from_load_balancer(
        &self,
        load_balancer: &aws_sdk_elasticloadbalancingv2::types::LoadBalancer,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let arn = load_balancer
            .load_balancer_arn()
            .ok_or_else(|| IacError::AwsSdk("elbv2:LoadBalancer missing ARN".into()))?;
        let dns_name = load_balancer.dns_name().unwrap_or_default();
        let subnet_ids: Vec<String> = load_balancer
            .availability_zones()
            .iter()
            .filter_map(|zone| zone.subnet_id().map(str::to_owned))
            .collect();
        let security_group_ids: Vec<String> = load_balancer
            .security_groups()
            .iter()
            .map(ToString::to_string)
            .collect();
        let tags = ctx.resource_tags(&self.name);
        let now = chrono::Utc::now().to_rfc3339();

        Ok(ResourceState {
            resource_type: self.resource_type(),
            physical_id: arn.to_owned(),
            properties: serde_json::json!({
                "load_balancer_arn": arn,
                "dns_name": dns_name,
                "scheme": "internal",
                "subnet_ids": subnet_ids,
                "security_group_ids": security_group_ids,
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        })
    }

    fn is_not_found(message: &str) -> bool {
        message.contains("LoadBalancerNotFound")
    }

    fn is_active(load_balancer: &aws_sdk_elasticloadbalancingv2::types::LoadBalancer) -> bool {
        load_balancer
            .state()
            .and_then(|state| state.code())
            .is_some_and(|code| matches!(code, LoadBalancerStateEnum::Active))
    }

    async fn wait_until_active(
        &self,
        load_balancer_arn: &str,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        let load_balancer_arn = load_balancer_arn.to_owned();
        let client = &ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2;
        super::poll_until(
            Duration::from_secs(5),
            Duration::from_secs(300),
            ctx,
            super::PollTarget {
                resource_desc: "ALB",
                resource_id: &self.resource_id(),
                resource_type: self.resource_type(),
                phase: "waiting for ALB to become active",
            },
            || async {
                let output = client
                    .describe_load_balancers()
                    .load_balancer_arns(&load_balancer_arn)
                    .send()
                    .await
                    .map_err(|e| {
                        IacError::AwsSdk(format!(
                            "elbv2:DescribeLoadBalancers: {}",
                            e.into_service_error()
                        ))
                    })?;
                Ok(output.load_balancers().first().is_some_and(Self::is_active))
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Resource for AlbResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("Alb")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("alb-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            self.vpc_dependency.clone(),
            self.security_group_dependency.clone(),
        ]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            self.wait_until_active(&existing.physical_id, ctx).await?;
            return Ok(existing);
        }

        let vpc_state = ctx.get_resource_state(&self.vpc_dependency)?;
        let subnet_ids: Vec<String> = vpc_state
            .properties
            .get("subnet_ids")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let sg_state = ctx.get_resource_state(&self.security_group_dependency)?;
        let security_group_id = sg_state
            .properties
            .get("security_group_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("security_group_id not found in ALB SG state".into())
            })?;
        let tags = super::elbv2_tags(&ctx.resource_tags(&self.name))?;

        let mut builder = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .create_load_balancer()
            .name(&self.name)
            .scheme(LoadBalancerSchemeEnum::Internal)
            .r#type(LoadBalancerTypeEnum::Application)
            .security_groups(security_group_id)
            .set_tags(Some(tags));
        for subnet_id in &subnet_ids {
            builder = builder.subnets(subnet_id);
        }

        let output = builder.send().await.map_err(|e| {
            IacError::AwsSdk(format!(
                "elbv2:CreateLoadBalancer: {}",
                e.into_service_error()
            ))
        })?;
        let load_balancer = output
            .load_balancers()
            .first()
            .ok_or_else(|| IacError::AwsSdk("elbv2:CreateLoadBalancer: no ALB returned".into()))?;
        let state = self.state_from_load_balancer(load_balancer, ctx)?;
        self.wait_until_active(&state.physical_id, ctx).await?;
        Ok(state)
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .delete_load_balancer()
            .load_balancer_arn(&current.physical_id)
            .send()
            .await
            .map_err(|e| {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if Self::is_not_found(&message) {
                    IacError::AwsSdk(message)
                } else {
                    IacError::AwsSdk(format!("elbv2:DeleteLoadBalancer: {svc_err}"))
                }
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let output = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .describe_load_balancers()
            .names(&self.name)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if Self::is_not_found(&message) {
                    return Ok(DescribeResult::Absent);
                }
                return Err(IacError::AwsSdk(format!(
                    "elbv2:DescribeLoadBalancers: {svc_err}"
                )));
            }
        };
        match output.load_balancers().first() {
            Some(load_balancer) => Ok(DescribeResult::Present(
                self.state_from_load_balancer(load_balancer, ctx)?,
            )),
            None => Ok(DescribeResult::Unsupported),
        }
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

/// IP target group for one edge service behind the internal ALB.
#[derive(Debug)]
pub struct AlbTargetGroupResource {
    name: String,
    port: u16,
    health_check_path: String,
    health_check_interval_secs: u64,
    module: String,
    vpc_dependency: ResourceId,
}

impl AlbTargetGroupResource {
    pub fn new(
        name: String,
        port: u16,
        health_check_path: String,
        health_check_interval_secs: u64,
        module: String,
        vpc_dependency: ResourceId,
    ) -> Self {
        Self {
            name,
            port,
            health_check_path,
            health_check_interval_secs,
            module,
            vpc_dependency,
        }
    }

    fn is_not_found(message: &str) -> bool {
        message.contains("TargetGroupNotFound")
    }

    fn state_from_target_group(
        &self,
        target_group: &aws_sdk_elasticloadbalancingv2::types::TargetGroup,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        let arn = target_group
            .target_group_arn()
            .ok_or_else(|| IacError::AwsSdk("elbv2:TargetGroup missing ARN".into()))?;
        let tags = ctx.resource_tags(&self.name);
        let now = chrono::Utc::now().to_rfc3339();
        Ok(ResourceState {
            resource_type: self.resource_type(),
            physical_id: arn.to_owned(),
            properties: serde_json::json!({
                "target_group_arn": arn,
                "name": self.name,
                "port": self.port,
                "protocol_version": "GRPC",
                "health_check_path": self.health_check_path,
                "health_check_interval_secs": self.health_check_interval_secs,
                "tags": tags,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        })
    }
}

#[async_trait::async_trait]
impl Resource for AlbTargetGroupResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("AlbTargetGroup")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("alb-tg-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.vpc_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            return Ok(existing);
        }

        let vpc_state = ctx.get_resource_state(&self.vpc_dependency)?;
        let vpc_id = vpc_state
            .properties
            .get("vpc_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| IacError::StateNotFound("vpc_id not found in VPC state".into()))?;
        let tags = super::elbv2_tags(&ctx.resource_tags(&self.name))?;
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .create_target_group()
            .name(&self.name)
            .protocol(ProtocolEnum::Http)
            .protocol_version("GRPC")
            .port(self.port as i32)
            .target_type(TargetTypeEnum::Ip)
            .vpc_id(vpc_id)
            .health_check_protocol(ProtocolEnum::Http)
            .health_check_path(&self.health_check_path)
            .health_check_interval_seconds(self.health_check_interval_secs as i32)
            .matcher(Matcher::builder().grpc_code("0").build())
            .set_tags(Some(tags))
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!(
                    "elbv2:CreateTargetGroup: {}",
                    e.into_service_error()
                ))
            })?;
        let target_group = output.target_groups().first().ok_or_else(|| {
            IacError::AwsSdk("elbv2:CreateTargetGroup: no target group returned".into())
        })?;
        self.state_from_target_group(target_group, ctx)
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .delete_target_group()
            .target_group_arn(&current.physical_id)
            .send()
            .await
            .map_err(|e| {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if Self::is_not_found(&message) {
                    IacError::AwsSdk(message)
                } else {
                    IacError::AwsSdk(format!("elbv2:DeleteTargetGroup: {svc_err}"))
                }
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let output = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .describe_target_groups()
            .names(&self.name)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if Self::is_not_found(&message) {
                    return Ok(DescribeResult::Absent);
                }
                return Err(IacError::AwsSdk(format!(
                    "elbv2:DescribeTargetGroups: {svc_err}"
                )));
            }
        };
        match output.target_groups().first() {
            Some(target_group) => Ok(DescribeResult::Present(
                self.state_from_target_group(target_group, ctx)?,
            )),
            None => Ok(DescribeResult::Unsupported),
        }
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

/// Single ALB listener plus host-header rules for edge-api and edge-poll.
#[derive(Debug)]
pub struct AlbListenerResource {
    name: String,
    mode: AlbListenerMode,
    certificate_arn: Option<String>,
    private_dns_zone: String,
    module: String,
    alb_dependency: ResourceId,
    edge_api_target_group_dependency: ResourceId,
    edge_poll_target_group_dependency: ResourceId,
}

impl AlbListenerResource {
    pub fn new(
        name: String,
        mode: AlbListenerMode,
        certificate_arn: Option<String>,
        private_dns_zone: String,
        module: String,
        alb_dependency: ResourceId,
        edge_api_target_group_dependency: ResourceId,
        edge_poll_target_group_dependency: ResourceId,
    ) -> Self {
        Self {
            name,
            mode,
            certificate_arn,
            private_dns_zone,
            module,
            alb_dependency,
            edge_api_target_group_dependency,
            edge_poll_target_group_dependency,
        }
    }

    fn is_not_found(message: &str) -> bool {
        message.contains("ListenerNotFound") || message.contains("LoadBalancerNotFound")
    }

    fn edge_api_host(&self) -> String {
        format!("edge-api.{}", self.private_dns_zone)
    }

    fn edge_poll_host(&self) -> String {
        format!("edge-poll.{}", self.private_dns_zone)
    }

    fn state(
        &self,
        listener_arn: String,
        edge_api_rule_arn: Option<String>,
        edge_poll_rule_arn: Option<String>,
    ) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: listener_arn.clone(),
            properties: serde_json::json!({
                "listener_arn": listener_arn,
                "port": self.mode.listener_port(),
                "protocol": match self.mode {
                    AlbListenerMode::Http2 => "HTTP",
                    AlbListenerMode::Https => "HTTPS",
                },
                "edge_api_host": self.edge_api_host(),
                "edge_poll_host": self.edge_poll_host(),
                "edge_api_rule_arn": edge_api_rule_arn,
                "edge_poll_rule_arn": edge_poll_rule_arn,
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module().to_owned(),
        }
    }

    async fn create_host_rule(
        &self,
        ctx: &ProvisionContext,
        listener_arn: &str,
        target_group_arn: &str,
        host: String,
        priority: i32,
    ) -> Result<String, IacError> {
        let output = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .create_rule()
            .listener_arn(listener_arn)
            .priority(priority)
            .conditions(
                RuleCondition::builder()
                    .field("host-header")
                    .values(host)
                    .build(),
            )
            .actions(
                Action::builder()
                    .r#type(ActionTypeEnum::Forward)
                    .target_group_arn(target_group_arn)
                    .build(),
            )
            .send()
            .await
            .map_err(|e| {
                IacError::AwsSdk(format!("elbv2:CreateRule: {}", e.into_service_error()))
            })?;
        output
            .rules()
            .first()
            .and_then(|rule| rule.rule_arn())
            .map(str::to_owned)
            .ok_or_else(|| IacError::AwsSdk("elbv2:CreateRule: no rule ARN returned".into()))
    }
}

#[async_trait::async_trait]
impl Resource for AlbListenerResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new("AlbListener")
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId(format!("alb-listener-{}", self.name))
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![
            self.alb_dependency.clone(),
            self.edge_api_target_group_dependency.clone(),
            self.edge_poll_target_group_dependency.clone(),
        ]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let DescribeResult::Present(existing) = self.describe(ctx).await? {
            return Ok(existing);
        }

        let alb_state = ctx.get_resource_state(&self.alb_dependency)?;
        let edge_api_state = ctx.get_resource_state(&self.edge_api_target_group_dependency)?;
        let edge_poll_state = ctx.get_resource_state(&self.edge_poll_target_group_dependency)?;
        let edge_api_target_group_arn = edge_api_state
            .properties
            .get("target_group_arn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("edge-api target_group_arn not found in state".into())
            })?;
        let edge_poll_target_group_arn = edge_poll_state
            .properties
            .get("target_group_arn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                IacError::StateNotFound("edge-poll target_group_arn not found in state".into())
            })?;

        let mut builder = ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .create_listener()
            .load_balancer_arn(&alb_state.physical_id)
            .protocol(self.mode.listener_protocol())
            .port(self.mode.listener_port() as i32)
            .default_actions(
                Action::builder()
                    .r#type(ActionTypeEnum::Forward)
                    .target_group_arn(edge_api_target_group_arn)
                    .build(),
            );
        if let Some(certificate_arn) = &self.certificate_arn {
            builder = builder.certificates(
                aws_sdk_elasticloadbalancingv2::types::Certificate::builder()
                    .certificate_arn(certificate_arn)
                    .build(),
            );
        }

        let output = builder.send().await.map_err(|e| {
            IacError::AwsSdk(format!("elbv2:CreateListener: {}", e.into_service_error()))
        })?;
        let listener_arn = output
            .listeners()
            .first()
            .and_then(|listener| listener.listener_arn())
            .ok_or_else(|| IacError::AwsSdk("elbv2:CreateListener: no listener ARN".into()))?
            .to_owned();
        let edge_api_rule_arn = self
            .create_host_rule(
                ctx,
                &listener_arn,
                edge_api_target_group_arn,
                self.edge_api_host(),
                10,
            )
            .await?;
        let edge_poll_rule_arn = self
            .create_host_rule(
                ctx,
                &listener_arn,
                edge_poll_target_group_arn,
                self.edge_poll_host(),
                20,
            )
            .await?;

        Ok(self.state(
            listener_arn,
            Some(edge_api_rule_arn),
            Some(edge_poll_rule_arn),
        ))
    }

    async fn update(
        &self,
        current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        ctx.extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .delete_listener()
            .listener_arn(&current.physical_id)
            .send()
            .await
            .map_err(|e| {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if Self::is_not_found(&message) {
                    IacError::AwsSdk(message)
                } else {
                    IacError::AwsSdk(format!("elbv2:DeleteListener: {svc_err}"))
                }
            })?;
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        let alb_state = match ctx.get_resource_state(&self.alb_dependency) {
            Ok(state) => state,
            Err(_) => return Ok(DescribeResult::Unsupported),
        };
        let output = match ctx
            .extension::<crate::AwsClients>()
            .expect("AwsClients")
            .elbv2
            .describe_listeners()
            .load_balancer_arn(&alb_state.physical_id)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                let svc_err = e.into_service_error();
                let message = format!("{svc_err}");
                if Self::is_not_found(&message) {
                    return Ok(DescribeResult::Absent);
                }
                return Err(IacError::AwsSdk(format!(
                    "elbv2:DescribeListeners: {svc_err}"
                )));
            }
        };
        let listener = output.listeners().iter().find(|listener| {
            listener
                .port()
                .is_some_and(|port| port == self.mode.listener_port() as i32)
        });
        Ok(listener
            .and_then(|listener| listener.listener_arn())
            .map(|listener_arn| {
                DescribeResult::Present(self.state(listener_arn.to_owned(), None, None))
            })
            .unwrap_or(DescribeResult::Unsupported))
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}
