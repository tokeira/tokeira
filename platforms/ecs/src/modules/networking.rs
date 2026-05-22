use tokeira_aws::{
    ResourceContext,
    resources::{
        elbv2::{AlbListenerMode, AlbListenerResource, AlbResource, AlbTargetGroupResource},
        security_group::{SecurityGroup, SecurityGroupConfig, SecurityRule},
        vpc::{VpcConfig, VpcResource},
        vpc_endpoint::{EndpointType, VpcEndpoint, VpcEndpointConfig},
    },
};
use tokeira_iac::{IacError, Module, ModuleContext, Resource, ResourceId};

use crate::config::{AlbListenerProtocol, EcsConfig, optional_vpc_endpoints};

#[derive(Debug, Clone)]
pub struct NetworkingModule {
    config: EcsConfig,
}

impl NetworkingModule {
    pub fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for NetworkingModule {
    fn name(&self) -> &str {
        "networking"
    }

    fn dependencies(&self) -> &[&str] {
        &["remote-state"]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let rctx = resource_context(&self.config);
        let vpc_id = vpc_resource_id(&self.config);
        let alb_sg_id = ResourceId("sg-alb".to_owned());
        let endpoint_sg_id = ResourceId("sg-vpc-endpoints".to_owned());
        let listener_mode = listener_mode(self.config.alb.listener_protocol);
        let alb_id = ResourceId(format!("alb-{}", self.config.alb.name));
        let edge_api_tg_id = ResourceId("alb-tg-tokeira-edge-api".to_owned());
        let edge_poll_tg_id = ResourceId("alb-tg-tokeira-edge-poll".to_owned());
        let mut resources: Vec<Box<dyn Resource>> = vec![
            Box::new(VpcResource::new(
                &rctx,
                VpcConfig {
                    cidr: self.config.networking.vpc_cidr.clone(),
                    availability_zones: self.config.networking.availability_zones.clone(),
                },
                self.name(),
            )),
            Box::new(security_group(
                "alb",
                "internal ALB",
                listener_mode.listener_port(),
                &vpc_id,
                &rctx,
            )),
            Box::new(security_group(
                "edge",
                "edge services",
                7233,
                &vpc_id,
                &rctx,
            )),
            Box::new(security_group(
                "runtime",
                "runtime services",
                7241,
                &vpc_id,
                &rctx,
            )),
            Box::new(security_group(
                "projection",
                "projection services",
                7242,
                &vpc_id,
                &rctx,
            )),
            Box::new(security_group(
                "control",
                "control services",
                7240,
                &vpc_id,
                &rctx,
            )),
            Box::new(security_group(
                "vpc-endpoints",
                "VPC endpoints",
                443,
                &vpc_id,
                &rctx,
            )),
        ];

        for endpoint in required_endpoint_specs(&self.config.region) {
            resources.push(Box::new(vpc_endpoint(
                endpoint.short_name,
                endpoint.service_name,
                endpoint.endpoint_type,
                &vpc_id,
                &endpoint_sg_id,
                &rctx,
            )));
        }
        for endpoint in optional_vpc_endpoints(&self.config) {
            let short_name = endpoint
                .rsplit('.')
                .next()
                .unwrap_or(&endpoint)
                .replace('.', "-");
            resources.push(Box::new(vpc_endpoint(
                short_name,
                endpoint,
                EndpointType::Interface,
                &vpc_id,
                &endpoint_sg_id,
                &rctx,
            )));
        }
        resources.push(Box::new(AlbResource::new(
            self.config.alb.name.clone(),
            self.name().to_owned(),
            vpc_id.clone(),
            alb_sg_id,
        )));
        let edge_api_grpc_port =
            required_grpc_port("edge-api", self.config.services.edge_api.grpc_port)?;
        let edge_poll_grpc_port =
            required_grpc_port("edge-poll", self.config.services.edge_poll.grpc_port)?;
        resources.push(Box::new(AlbTargetGroupResource::new(
            "tokeira-edge-api".to_owned(),
            edge_api_grpc_port,
            self.config.alb.health_check_path.clone(),
            self.config.alb.health_check_interval_secs,
            self.name().to_owned(),
            vpc_id.clone(),
        )));
        resources.push(Box::new(AlbTargetGroupResource::new(
            "tokeira-edge-poll".to_owned(),
            edge_poll_grpc_port,
            self.config.alb.health_check_path.clone(),
            self.config.alb.health_check_interval_secs,
            self.name().to_owned(),
            vpc_id.clone(),
        )));
        resources.push(Box::new(AlbListenerResource::new(
            self.config.alb.name.clone(),
            listener_mode,
            self.config.alb.certificate_arn.clone(),
            self.config.networking.private_dns_zone.clone(),
            self.name().to_owned(),
            alb_id,
            edge_api_tg_id,
            edge_poll_tg_id,
        )));
        Ok(resources)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointSpec {
    pub short_name: String,
    pub service_name: String,
    pub endpoint_type: EndpointType,
}

pub fn required_endpoint_specs(region: &str) -> Vec<EndpointSpec> {
    [
        ("ecs", "ecs", EndpointType::Interface),
        ("ecs-agent", "ecs-agent", EndpointType::Interface),
        ("ecs-telemetry", "ecs-telemetry", EndpointType::Interface),
        ("ecr-api", "ecr.api", EndpointType::Interface),
        ("ecr-dkr", "ecr.dkr", EndpointType::Interface),
        ("s3", "s3", EndpointType::Gateway),
        ("autoscaling", "autoscaling", EndpointType::Interface),
        (
            "servicediscovery",
            "servicediscovery",
            EndpointType::Interface,
        ),
        ("ssm", "ssm", EndpointType::Interface),
        ("ssmmessages", "ssmmessages", EndpointType::Interface),
        ("ec2messages", "ec2messages", EndpointType::Interface),
    ]
    .into_iter()
    .map(|(short_name, service, endpoint_type)| EndpointSpec {
        short_name: short_name.to_owned(),
        service_name: format!("com.amazonaws.{region}.{service}"),
        endpoint_type,
    })
    .collect()
}

fn resource_context(config: &EcsConfig) -> ResourceContext {
    ResourceContext {
        project: config.project_name.clone(),
        region: config.region.clone(),
        tags: config.tags.clone(),
    }
}

fn vpc_resource_id(config: &EcsConfig) -> ResourceId {
    ResourceId(format!("{}-vpc", config.project_name))
}

fn security_group(
    name: &str,
    description: &str,
    port: u16,
    vpc_dependency: &ResourceId,
    rctx: &ResourceContext,
) -> SecurityGroup {
    SecurityGroup::new(
        name.to_owned(),
        SecurityGroupConfig {
            vpc_dependency: vpc_dependency.clone(),
            description: format!("Tokeira ECS {description} security group"),
            ingress_rules: vec![SecurityRule {
                description: format!("self {description} traffic"),
                protocol: "tcp".into(),
                from_port: port,
                to_port: port,
                source: "self".into(),
            }],
            module: "networking".into(),
        },
        rctx,
    )
}

fn listener_mode(protocol: AlbListenerProtocol) -> AlbListenerMode {
    match protocol {
        AlbListenerProtocol::Http2 => AlbListenerMode::Http2,
        AlbListenerProtocol::Https => AlbListenerMode::Https,
    }
}

fn required_grpc_port(service: &str, port: Option<u16>) -> Result<u16, IacError> {
    port.ok_or_else(|| {
        IacError::DependencyResolution(format!(
            "{service}.grpc_port is required for ALB target group registration"
        ))
    })
}

fn vpc_endpoint(
    short_name: impl Into<String>,
    service_name: String,
    endpoint_type: EndpointType,
    vpc_dependency: &ResourceId,
    endpoint_sg_dependency: &ResourceId,
    rctx: &ResourceContext,
) -> VpcEndpoint {
    let security_group_dependency = match endpoint_type {
        EndpointType::Gateway => None,
        EndpointType::Interface => Some(endpoint_sg_dependency.clone()),
    };
    VpcEndpoint::new(
        short_name.into(),
        VpcEndpointConfig {
            service_name,
            endpoint_type,
            vpc_dependency: vpc_dependency.clone(),
            security_group_dependency,
            resource_id: None,
            module: "networking".into(),
        },
        rctx,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::config::required_vpc_endpoints;
    use tokeira_iac::InfraState;

    #[test]
    fn networking_module_enumerates_vpc_security_groups_and_required_endpoints() {
        let config = EcsConfig::default();
        let module = NetworkingModule::new(config);
        let extensions = std::collections::HashMap::new();
        let state = InfraState::default();
        let ctx = ModuleContext::new(&state, &extensions);
        let resources = module.resources(&ctx).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert!(ids.contains(&"tokeira-vpc".to_owned()));
        assert!(ids.contains(&"sg-vpc-endpoints".to_owned()));
        assert!(ids.contains(&"vpce-s3".to_owned()));
        assert!(ids.contains(&"vpce-ssm".to_owned()));
        assert!(ids.contains(&"vpce-ssmmessages".to_owned()));
        assert!(ids.contains(&"vpce-ec2messages".to_owned()));
        assert!(ids.contains(&"alb-tokeira-internal".to_owned()));
        assert!(ids.contains(&"alb-tg-tokeira-edge-api".to_owned()));
        assert!(ids.contains(&"alb-tg-tokeira-edge-poll".to_owned()));
        assert!(ids.contains(&"alb-listener-tokeira-internal".to_owned()));
    }

    #[test]
    fn required_endpoint_specs_mark_s3_as_gateway_only() {
        let endpoints = required_endpoint_specs("eu-west-2");
        let s3 = endpoints
            .iter()
            .find(|endpoint| endpoint.short_name == "s3")
            .expect("s3 endpoint");

        assert_eq!(s3.endpoint_type, EndpointType::Gateway);
        assert_eq!(endpoints.len(), 11);
    }

    #[test]
    fn static_endpoint_specs_match_config_without_dsql_categories() {
        let region = "eu-west-2";
        let actual: BTreeSet<String> = required_endpoint_specs(region)
            .into_iter()
            .map(|endpoint| endpoint.service_name)
            .collect();
        let expected: BTreeSet<String> = required_vpc_endpoints(region)
            .into_iter()
            .filter(|service| {
                service != &format!("com.amazonaws.{region}.dsql")
                    && service != &format!("com.amazonaws.{region}.dsql-control")
            })
            .collect();

        assert_eq!(actual, expected);
    }
}
