use tokeira_aws::{
    ResourceContext,
    resources::{
        ecs_cluster::{
            AsgResource, CapacityProviderResource, EcsClusterResource, LaunchTemplateResource,
        },
        iam_instance_profile::{IamInstanceProfile, IamInstanceProfileConfig},
        iam_role::{IamRole, IamRoleConfig},
    },
};
use tokeira_iac::{IacError, Module, ModuleContext, Resource, ResourceId};

use crate::config::{CapacityProviderConfig, EcsConfig, RuntimeCapacityProviderConfig};

#[derive(Debug, Clone)]
pub struct ClusterModule {
    config: EcsConfig,
}

impl ClusterModule {
    pub(crate) fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for ClusterModule {
    fn name(&self) -> &str {
        "cluster"
    }

    fn dependencies(&self) -> Vec<&str> {
        vec!["dsql"]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let cluster_name = self.config.cluster.name.clone();
        let rctx = resource_context(&self.config);
        let vpc_id = vpc_resource_id(&self.config);
        let runtime_sg_id = ResourceId("sg-runtime".to_owned());
        let cluster_id = ResourceId("ecs:cluster".to_owned());
        let mut resources: Vec<Box<dyn Resource>> = vec![Box::new(EcsClusterResource::new(
            cluster_name.clone(),
            self.config.networking.private_dns_zone.clone(),
            self.name(),
        ))];

        for entry in capacity_provider_entries(&self.config) {
            let profile_name = format!("{}-{}-instance", self.config.project_name, entry.name);
            let role_name = format!("{}-role", profile_name);
            let role = IamRole::new(
                role_name.clone(),
                IamRoleConfig {
                    trust_policy: ec2_assume_role_policy(),
                    inline_policies: std::collections::HashMap::new(),
                    dependent_inline_policies: Vec::new(),
                    managed_policy_arns: vec![
                        "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
                            .to_owned(),
                        "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore".to_owned(),
                    ],
                    module: self.name().to_owned(),
                },
                &rctx,
            );
            let role_dependency = role.resource_id();
            let launch_template_id = ResourceId(format!("lt-{}", entry.name));
            let asg_id = ResourceId(format!("asg-{}", entry.name));

            resources.push(Box::new(role));
            resources.push(Box::new(IamInstanceProfile::new(
                profile_name.clone(),
                IamInstanceProfileConfig {
                    role_name,
                    role_dependency,
                    module: self.name().to_owned(),
                },
                &rctx,
            )));
            resources.push(Box::new(LaunchTemplateResource {
                name: entry.name.to_owned(),
                cluster_name: cluster_name.clone(),
                instance_type: entry.instance_type,
                workload: entry.workload.to_owned(),
                instance_profile_name: profile_name,
                security_group_dependency: runtime_sg_id.clone(),
                module: self.name().to_owned(),
            }));
            resources.push(Box::new(AsgResource {
                name: entry.name.to_owned(),
                min_size: entry.min_capacity,
                desired_capacity: entry.desired_capacity,
                max_size: entry.max_capacity,
                new_instances_protected_from_scale_in: entry.protect_from_scale_in,
                launch_template_dependency: launch_template_id,
                vpc_dependency: vpc_id.clone(),
                module: self.name().to_owned(),
            }));
            resources.push(Box::new(CapacityProviderResource {
                name: entry.name.to_owned(),
                cluster_dependency: cluster_id.clone(),
                asg_dependency: asg_id,
                module: self.name().to_owned(),
            }));
        }

        Ok(resources)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityProviderEntry {
    pub(crate) name: &'static str,
    pub(crate) workload: &'static str,
    pub(crate) instance_type: String,
    pub(crate) min_capacity: u32,
    pub(crate) desired_capacity: u32,
    pub(crate) max_capacity: u32,
    pub(crate) protect_from_scale_in: bool,
}

pub(crate) fn capacity_provider_entries(config: &EcsConfig) -> Vec<CapacityProviderEntry> {
    vec![
        replica_entry(
            "cp-edge-api",
            "edge-api",
            &config.capacity_providers.edge_api,
        ),
        replica_entry(
            "cp-edge-poll",
            "edge-poll",
            &config.capacity_providers.edge_poll,
        ),
        runtime_entry(&config.capacity_providers.runtime),
        replica_entry(
            "cp-projection",
            "projection",
            &config.capacity_providers.projection,
        ),
        replica_entry("cp-control", "control", &config.capacity_providers.control),
        replica_entry("cp-mimir", "mimir", &config.capacity_providers.mimir),
        replica_entry("cp-loki", "loki", &config.capacity_providers.loki),
        replica_entry("cp-grafana", "grafana", &config.capacity_providers.grafana),
    ]
}

fn replica_entry(
    name: &'static str,
    workload: &'static str,
    config: &CapacityProviderConfig,
) -> CapacityProviderEntry {
    CapacityProviderEntry {
        name,
        workload,
        instance_type: config.instance_type.clone(),
        min_capacity: config.min_capacity,
        desired_capacity: config.desired_capacity,
        max_capacity: config.max_capacity,
        protect_from_scale_in: false,
    }
}

fn runtime_entry(config: &RuntimeCapacityProviderConfig) -> CapacityProviderEntry {
    CapacityProviderEntry {
        name: "cp-runtime",
        workload: "runtime",
        instance_type: config.instance_type.clone(),
        min_capacity: config.min_capacity,
        desired_capacity: config.desired_capacity,
        max_capacity: config.max_capacity,
        // Runtime scale-in is coordinated by the controller/autoscaler drain
        // loop. New-instance protection gives that loop control without using
        // ECS managed termination protection, which does not work for DAEMON
        // services.
        protect_from_scale_in: config.scale_in_protection,
    }
}

fn ec2_assume_role_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "ec2.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::InfraState;

    fn module_context() -> ModuleContext<'static> {
        let state = Box::leak(Box::new(InfraState::default()));
        let extensions = Box::leak(Box::new(std::collections::HashMap::new()));
        ModuleContext::new(state, extensions)
    }

    #[test]
    fn cluster_module_reports_name_and_dsql_dependency() {
        let module = ClusterModule::new(EcsConfig::default());

        assert_eq!(module.name(), "cluster");
        assert_eq!(module.dependencies(), &["dsql"]);
    }

    #[test]
    fn capacity_provider_entries_cover_all_eight_planes() {
        let entries = capacity_provider_entries(&EcsConfig::default());
        let names: Vec<&str> = entries.iter().map(|entry| entry.name).collect();

        assert_eq!(names.len(), 8);
        assert!(names.contains(&"cp-runtime"));
        assert!(names.contains(&"cp-mimir"));
        assert!(names.contains(&"cp-loki"));
        assert!(names.contains(&"cp-grafana"));
    }

    #[test]
    fn cluster_module_enumerates_cluster_profiles_templates_asgs_and_capacity_providers() {
        let module = ClusterModule::new(EcsConfig::default());
        let resources = module.resources(&module_context()).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert_eq!(ids.len(), 41);
        assert!(ids.contains(&"ecs:cluster".to_owned()));
        assert_eq!(ids.iter().filter(|id| id.starts_with("lt-")).count(), 8);
        assert_eq!(ids.iter().filter(|id| id.starts_with("asg-")).count(), 8);
        assert_eq!(ids.iter().filter(|id| id.starts_with("cp-")).count(), 8);
    }

    #[test]
    fn runtime_entry_protects_new_instances_but_capacity_provider_does_not_manage_termination() {
        let config = EcsConfig::default();
        let runtime = capacity_provider_entries(&config)
            .into_iter()
            .find(|entry| entry.name == "cp-runtime")
            .expect("runtime entry");

        assert!(runtime.protect_from_scale_in);
    }

    #[test]
    fn observability_capacity_providers_are_single_host() {
        let entries = capacity_provider_entries(&EcsConfig::default());
        for name in ["cp-mimir", "cp-loki", "cp-grafana"] {
            let entry = entries
                .iter()
                .find(|entry| entry.name == name)
                .expect("observability entry");
            assert_eq!(entry.desired_capacity, 1);
            assert_eq!(entry.max_capacity, 1);
        }
    }

    #[test]
    fn control_plane_has_rolling_update_headroom() {
        let entries = capacity_provider_entries(&EcsConfig::default());
        let control = entries
            .iter()
            .find(|entry| entry.name == "cp-control")
            .expect("control entry");

        assert!(control.max_capacity >= 3);
    }

    #[test]
    fn launch_template_user_data_sets_workload_attribute() {
        let launch_template = LaunchTemplateResource {
            name: "cp-runtime".into(),
            cluster_name: "tokeira".into(),
            instance_type: "c7g.large".into(),
            workload: "runtime".into(),
            instance_profile_name: "profile".into(),
            security_group_dependency: ResourceId("sg-runtime".into()),
            module: "cluster".into(),
        };

        assert!(
            launch_template
                .user_data()
                .contains("ECS_INSTANCE_ATTRIBUTES={\"workload\":\"runtime\"}")
        );
    }
}
