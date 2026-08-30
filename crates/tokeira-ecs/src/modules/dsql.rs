use std::collections::HashMap;

use tokeira_aws::{
    ResourceContext,
    resources::{
        dsql_cluster::{DsqlCluster, DsqlClusterMode as AwsDsqlClusterMode},
        dsql_connection_endpoint::{DsqlConnectionEndpoint, DsqlConnectionEndpointConfig},
        iam_role::{IamRole, IamRoleConfig},
        vpc_endpoint::{EndpointType, VpcEndpoint, VpcEndpointConfig},
    },
};
use tokeira_iac::{
    DescribeResult, IacError, InternalChange, Module, ModuleContext, ProvisionContext, Resource,
    ResourceId, ResourceState, ResourceType,
};

use crate::config::{DsqlClusterMode, DsqlConfig, EcsConfig};

const DSQL_CLUSTER_ID: &str = "dsql:cluster";
const DSQL_MANAGEMENT_ENDPOINT_ID: &str = "dsql:management-endpoint";
const DSQL_CONNECTION_ENDPOINT_ID: &str = "dsql:connection-endpoint";
const DSQL_RUNTIME_ROLE_ID: &str = "dsql:runtime-role";
const DSQL_ADMIN_ROLE_ID: &str = "dsql:admin-role";

#[derive(Debug, Clone)]
pub struct DsqlModule {
    config: EcsConfig,
}

impl DsqlModule {
    pub(crate) fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for DsqlModule {
    fn name(&self) -> &str {
        "dsql"
    }

    fn dependencies(&self) -> Vec<&str> {
        vec!["networking"]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        validate_preexisting(&self.config.dsql)?;
        let rctx = resource_context(&self.config);
        let vpc_id = vpc_resource_id(&self.config);
        let endpoint_sg_id = ResourceId("sg-vpc-endpoints".to_owned());
        let cluster_id = ResourceId(DSQL_CLUSTER_ID.to_owned());
        let mode = match self.config.dsql.mode {
            DsqlClusterMode::Managed => AwsDsqlClusterMode::Managed,
            DsqlClusterMode::Preexisting => AwsDsqlClusterMode::Preexisting,
        };
        let mut resources: Vec<Box<dyn Resource>> = vec![Box::new(DsqlCluster {
            identity: format!("{}-{}", self.config.project_name, self.config.environment),
            mode,
            endpoint: self.config.dsql.endpoint.clone(),
            resource_id: Some(cluster_id.clone()),
            module: self.name().to_owned(),
            project: rctx.project.clone(),
            region: rctx.region.clone(),
            tags: rctx.tags.clone(),
            ..Default::default()
        })];

        match self.config.dsql.mode {
            DsqlClusterMode::Managed => {
                resources.push(Box::new(VpcEndpoint::new(
                    "dsql-management".to_owned(),
                    VpcEndpointConfig {
                        service_name: format!("com.amazonaws.{}.dsql-control", self.config.region),
                        endpoint_type: EndpointType::Interface,
                        vpc_dependency: vpc_id.clone(),
                        security_group_dependency: Some(endpoint_sg_id.clone()),
                        resource_id: Some(ResourceId(DSQL_MANAGEMENT_ENDPOINT_ID.to_owned())),
                        module: self.name().to_owned(),
                    },
                    &rctx,
                )));
                resources.push(Box::new(DsqlConnectionEndpoint::new(
                    format!("{}-connection", self.config.project_name),
                    DsqlConnectionEndpointConfig {
                        vpc_dependency: vpc_id.clone(),
                        security_group_dependency: endpoint_sg_id,
                        dsql_cluster_dependency: cluster_id.clone(),
                        resource_id: Some(ResourceId(DSQL_CONNECTION_ENDPOINT_ID.to_owned())),
                        module: self.name().to_owned(),
                    },
                    &rctx,
                )));
                resources.push(Box::new(DsqlIamRoleResource::managed(
                    ResourceId(DSQL_RUNTIME_ROLE_ID.to_owned()),
                    format!("{}-dsql-runtime", self.config.project_name),
                    "dsql-runtime",
                    "dsql:DbConnect",
                    cluster_id.clone(),
                    self.name().to_owned(),
                    rctx.clone(),
                )));
                resources.push(Box::new(DsqlIamRoleResource::managed(
                    ResourceId(DSQL_ADMIN_ROLE_ID.to_owned()),
                    format!("{}-dsql-admin", self.config.project_name),
                    "dsql-admin",
                    "dsql:DbConnectAdmin",
                    cluster_id,
                    self.name().to_owned(),
                    rctx,
                )));
            }
            DsqlClusterMode::Preexisting => {
                resources.push(Box::new(AdoptedDsqlResource::endpoint(
                    ResourceId(DSQL_MANAGEMENT_ENDPOINT_ID.to_owned()),
                    self.config
                        .dsql
                        .management_endpoint_id
                        .clone()
                        .unwrap_or_default(),
                    self.name(),
                )));
                resources.push(Box::new(AdoptedDsqlResource::endpoint(
                    ResourceId(DSQL_CONNECTION_ENDPOINT_ID.to_owned()),
                    self.config
                        .dsql
                        .connection_endpoint_id
                        .clone()
                        .unwrap_or_default(),
                    self.name(),
                )));
                resources.push(Box::new(DsqlIamRoleResource::preexisting(
                    ResourceId(DSQL_RUNTIME_ROLE_ID.to_owned()),
                    self.config
                        .dsql
                        .runtime_role_arn
                        .clone()
                        .unwrap_or_default(),
                    self.name().to_owned(),
                )));
                resources.push(Box::new(DsqlIamRoleResource::preexisting(
                    ResourceId(DSQL_ADMIN_ROLE_ID.to_owned()),
                    self.config.dsql.admin_role_arn.clone().unwrap_or_default(),
                    self.name().to_owned(),
                )));
            }
        }

        Ok(resources)
    }
}

#[derive(Debug)]
pub(crate) struct AdoptedDsqlResource {
    resource_id: ResourceId,
    endpoint_id: String,
    module: String,
}

impl AdoptedDsqlResource {
    pub(crate) fn endpoint(resource_id: ResourceId, endpoint_id: String, module: &str) -> Self {
        Self {
            resource_id,
            endpoint_id,
            module: module.to_owned(),
        }
    }

    fn state(&self) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.endpoint_id.clone(),
            properties: serde_json::json!({
                "endpoint_id": self.endpoint_id,
                "mode": "preexisting",
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for AdoptedDsqlResource {
    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        const RECORD_ONLY: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::{create,update,delete} — record-only: no provider call is ever \
             made; the preexisting DSQL cluster is referenced, never managed, \
             and the delete retires the record leaving the cluster running"
        ));
        let claims = |operation| tokeira_iac::ChangeSemantics {
            operation: tokeira_iac::Confidence::EngineFact {
                value: operation,
                citation: RECORD_ONLY,
            },
            replacement: tokeira_iac::Confidence::EngineFact {
                value: tokeira_iac::ReplacementPolicy::NotRequired,
                citation: RECORD_ONLY,
            },
            disruption: tokeira_iac::Confidence::EngineFact {
                value: tokeira_iac::Disruption::None,
                citation: RECORD_ONLY,
            },
            data_effect: tokeira_iac::Confidence::EngineFact {
                value: tokeira_iac::DataEffect::Preserved,
                citation: RECORD_ONLY,
            },
            reversibility: tokeira_iac::Confidence::EngineFact {
                value: tokeira_iac::Reversibility::Reversible,
                citation: RECORD_ONLY,
            },
            statement: None,
            provider_assigned: Vec::new(),
        };
        match ctx.kind {
            tokeira_iac::ChangeKind::Create => claims(tokeira_iac::LifecycleOperation::Created),
            tokeira_iac::ChangeKind::Update | tokeira_iac::ChangeKind::Replace => {
                claims(tokeira_iac::LifecycleOperation::UpdatedInPlace)
            }
            tokeira_iac::ChangeKind::Delete => claims(tokeira_iac::LifecycleOperation::Deleted),
            tokeira_iac::ChangeKind::NoChange => tokeira_iac::ChangeSemantics::default(),
        }
    }

    fn resource_type(&self) -> ResourceType {
        ResourceType::new("DsqlEndpoint")
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        // The adopted endpoint's one fact, sourced by the definition's
        // DSQL writebacks in preexisting mode.
        &["endpoint_id"]
    }

    fn resource_id(&self) -> ResourceId {
        self.resource_id.clone()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, _ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        Ok(self.state())
    }

    async fn update(
        &self,
        _current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        Ok(self.state())
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        Ok(())
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        Ok(DescribeResult::Present(self.state()))
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let current_endpoint = current
            .properties
            .get("endpoint_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if current_endpoint == self.endpoint_id {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: vec![tokeira_iac::FieldDiff::observation(
                    "preexisting DSQL endpoint changed",
                )],
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct DsqlIamRoleResource {
    resource_id: ResourceId,
    role_name: String,
    policy_name: String,
    action: String,
    cluster_dependency: Option<ResourceId>,
    preexisting_role_arn: Option<String>,
    module: String,
    rctx: Option<ResourceContext>,
}

impl DsqlIamRoleResource {
    pub(crate) fn managed(
        resource_id: ResourceId,
        role_name: String,
        policy_name: &str,
        action: &str,
        cluster_dependency: ResourceId,
        module: String,
        rctx: ResourceContext,
    ) -> Self {
        Self {
            resource_id,
            role_name,
            policy_name: policy_name.to_owned(),
            action: action.to_owned(),
            cluster_dependency: Some(cluster_dependency),
            preexisting_role_arn: None,
            module,
            rctx: Some(rctx),
        }
    }

    pub(crate) fn preexisting(resource_id: ResourceId, role_arn: String, module: String) -> Self {
        Self {
            resource_id,
            role_name: role_arn.clone(),
            policy_name: String::new(),
            action: String::new(),
            cluster_dependency: None,
            preexisting_role_arn: Some(role_arn),
            module,
            rctx: None,
        }
    }

    fn assume_role_policy() -> String {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "ecs-tasks.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        })
        .to_string()
    }

    fn dsql_policy(&self, cluster_arn: &str) -> String {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": self.action,
                "Resource": cluster_arn
            }]
        })
        .to_string()
    }

    fn preexisting_state(&self, role_arn: &str) -> ResourceState {
        let now = chrono::Utc::now().to_rfc3339();
        ResourceState {
            resource_type: self.resource_type(),
            physical_id: role_arn.to_owned(),
            properties: serde_json::json!({
                "role_arn": role_arn,
                "mode": "preexisting",
            }),
            dependencies: self.dependencies(),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Resource for DsqlIamRoleResource {
    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        const PREEXISTING: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::{create,update,delete} (preexisting role) — record-only: the \
             configured role ARN is referenced, never managed"
        ));
        const CREATE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::create (managed) — delegates to the generic IamRole create: \
             iam:CreateRole with the DSQL trust policy, then the cluster-ARN \
             inline policy"
        ));
        const UPDATE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::update — a recorded no-op; the role's policy is fixed at create"
        ));
        const DELETE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::delete (managed) — delegates to the generic IamRole delete: \
             detach and delete policies, then iam:DeleteRole"
        ));
        if self.preexisting_role_arn.is_some() {
            let mut semantics = tokeira_aws::resources::control_plane_semantics(
                ctx.kind,
                PREEXISTING,
                PREEXISTING,
                PREEXISTING,
            );
            // Record-only: nothing the provider holds is touched either way.
            if matches!(ctx.kind, tokeira_iac::ChangeKind::Delete) {
                semantics.data_effect = tokeira_iac::Confidence::EngineFact {
                    value: tokeira_iac::DataEffect::Preserved,
                    citation: PREEXISTING,
                };
            }
            semantics
        } else {
            tokeira_aws::resources::control_plane_semantics(ctx.kind, CREATE, UPDATE, DELETE)
        }
    }

    fn resource_type(&self) -> ResourceType {
        // "DsqlIamRole", not "IamRole": kind names must equal the realized
        // resource_type and stay unique across namespaces, and the generic
        // tokeira_aws namespace owns "IamRole". Existing recorded state that
        // carries the old string re-plans as a type change once — accepted
        // for the definition-driven cutover.
        ResourceType::new("DsqlIamRole")
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        // The role's one fact, sourced by the definition's DSQL writebacks
        // in both managed and preexisting modes.
        &["role_arn"]
    }

    fn resource_id(&self) -> ResourceId {
        self.resource_id.clone()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.cluster_dependency.iter().cloned().collect()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        if let Some(role_arn) = &self.preexisting_role_arn {
            return Ok(self.preexisting_state(role_arn));
        }

        let cluster_dependency = self.cluster_dependency.as_ref().ok_or_else(|| {
            IacError::DependencyResolution("managed DSQL role missing cluster dependency".into())
        })?;
        let cluster_state = ctx.get_resource_state(cluster_dependency)?;
        let cluster_arn = cluster_state
            .properties
            .get("cluster_arn")
            .and_then(|v| v.as_str())
            .filter(|arn| !arn.is_empty())
            .ok_or_else(|| {
                IacError::StateNotFound("cluster_arn not found in DSQL cluster state".into())
            })?;
        let rctx = self.rctx.as_ref().ok_or_else(|| {
            IacError::DependencyResolution("managed DSQL role missing resource context".into())
        })?;
        let mut inline_policies = HashMap::new();
        inline_policies.insert(self.policy_name.clone(), self.dsql_policy(cluster_arn));
        let role = IamRole::new(
            self.role_name.clone(),
            IamRoleConfig {
                trust_policy: Self::assume_role_policy(),
                inline_policies,
                dependent_inline_policies: Vec::new(),
                managed_policy_arns: vec![],
                module: self.module.clone(),
            },
            rctx,
        );
        role.create(ctx).await
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
        if self.preexisting_role_arn.is_some() {
            return Ok(());
        }
        let rctx = self.rctx.as_ref().ok_or_else(|| {
            IacError::DependencyResolution("managed DSQL role missing resource context".into())
        })?;
        let role = IamRole::new(
            self.role_name.clone(),
            IamRoleConfig {
                trust_policy: Self::assume_role_policy(),
                inline_policies: HashMap::new(),
                dependent_inline_policies: Vec::new(),
                managed_policy_arns: vec![],
                module: self.module.clone(),
            },
            rctx,
        );
        role.delete(current, ctx).await
    }

    async fn describe(&self, _ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        // A preexisting (adopted) role describes from config; a managed role has
        // no provider query here, so its existence is Unsupported rather than a
        // confirmed Absent — the engine must not prune/skip on this.
        Ok(match self.preexisting_role_arn.as_deref() {
            Some(role_arn) => DescribeResult::Present(self.preexisting_state(role_arn)),
            None => DescribeResult::Unsupported,
        })
    }

    fn diff(&self, _current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

fn validate_preexisting(config: &DsqlConfig) -> Result<(), IacError> {
    if config.mode == DsqlClusterMode::Managed {
        return Ok(());
    }
    let missing = [
        ("dsql.endpoint", config.endpoint.as_ref()),
        (
            "dsql.management_endpoint_id",
            config.management_endpoint_id.as_ref(),
        ),
        (
            "dsql.connection_endpoint_id",
            config.connection_endpoint_id.as_ref(),
        ),
        ("dsql.runtime_role_arn", config.runtime_role_arn.as_ref()),
        ("dsql.admin_role_arn", config.admin_role_arn.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, value)| {
        value
            .filter(|value| !value.is_empty())
            .map(|_| ())
            .is_none()
            .then_some(name)
    })
    .collect::<Vec<_>>();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(IacError::DependencyResolution(format!(
            "preexisting DSQL mode is missing required fields: {}",
            missing.join(", ")
        )))
    }
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
        let extensions = Box::leak(Box::new(HashMap::new()));
        ModuleContext::new(state, extensions)
    }

    #[test]
    fn dsql_module_reports_name_and_networking_dependency() {
        let module = DsqlModule::new(EcsConfig::default());

        assert_eq!(module.name(), "dsql");
        assert_eq!(module.dependencies(), &["networking"]);
    }

    #[test]
    fn managed_mode_enumerates_authoritative_dsql_resources() {
        let module = DsqlModule::new(EcsConfig::default());
        let resources = module.resources(&module_context()).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert_eq!(ids.len(), 5);
        assert_eq!(
            ids,
            vec![
                DSQL_CLUSTER_ID,
                DSQL_MANAGEMENT_ENDPOINT_ID,
                DSQL_CONNECTION_ENDPOINT_ID,
                DSQL_RUNTIME_ROLE_ID,
                DSQL_ADMIN_ROLE_ID,
            ]
        );
    }

    #[test]
    fn managed_mode_declares_dsql_endpoint_categories() {
        let module = DsqlModule::new(EcsConfig::default());
        let resources = module.resources(&module_context()).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert!(ids.contains(&DSQL_MANAGEMENT_ENDPOINT_ID.to_owned()));
        assert!(ids.contains(&DSQL_CONNECTION_ENDPOINT_ID.to_owned()));
    }

    #[test]
    fn preexisting_mode_requires_all_hydration_fields() {
        let mut config = EcsConfig::default();
        config.dsql.mode = DsqlClusterMode::Preexisting;
        let module = DsqlModule::new(config);

        let err = match module.resources(&module_context()) {
            Ok(_) => panic!("missing preexisting fields should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("dsql.endpoint"));
    }

    #[test]
    fn preexisting_mode_enumerates_adopter_resources() {
        let mut config = EcsConfig::default();
        config.dsql.mode = DsqlClusterMode::Preexisting;
        config.dsql.endpoint = Some("cluster.example.dsql".into());
        config.dsql.management_endpoint_id = Some("vpce-management".into());
        config.dsql.connection_endpoint_id = Some("vpce-connection".into());
        config.dsql.runtime_role_arn = Some("arn:aws:iam::123456789012:role/runtime".into());
        config.dsql.admin_role_arn = Some("arn:aws:iam::123456789012:role/admin".into());
        let module = DsqlModule::new(config);

        let resources = module.resources(&module_context()).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert_eq!(ids.len(), 5);
        assert!(ids.contains(&DSQL_MANAGEMENT_ENDPOINT_ID.to_owned()));
        assert!(ids.contains(&DSQL_CONNECTION_ENDPOINT_ID.to_owned()));
    }

    #[test]
    fn preexisting_role_state_exposes_role_arn() {
        let resource = DsqlIamRoleResource::preexisting(
            ResourceId(DSQL_RUNTIME_ROLE_ID.to_owned()),
            "arn:aws:iam::123456789012:role/runtime".into(),
            "dsql".into(),
        );
        let state = resource.preexisting_state("arn:aws:iam::123456789012:role/runtime");

        assert_eq!(
            state.properties.get("role_arn").and_then(|v| v.as_str()),
            Some("arn:aws:iam::123456789012:role/runtime")
        );
    }
}
