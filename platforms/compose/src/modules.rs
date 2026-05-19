//! IaC modules for the compose platform.
//!
//! Two logical modules group compose services:
//! - `runtime` — the tokeirad service
//! - `observability` — mimir, loki, grafana, alloy

use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use tokeira_compose::ComposeService;
use tokeira_iac as iac;
use tokeira_orchestrator::StorageKind;

use crate::{
    compose::{MODULE_OBSERVABILITY, MODULE_RUNTIME, compose_services, module_for_service},
    config::{ComposeConfig, ComposeDsqlConfig, DsqlMode},
    observability_config::{ObservabilityConfigFilesResource, ObservabilityParams},
};

// ── Local state module ────────────────────────────────────────────

#[derive(Debug)]
pub struct LocalStateModule {
    pub state_dir: PathBuf,
}

impl iac::Module for LocalStateModule {
    fn name(&self) -> &str {
        "local-state"
    }

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        Ok(vec![Box::new(LocalStateResource {
            state_dir: self.state_dir.clone(),
        })])
    }
}

#[derive(Debug)]
struct LocalStateResource {
    state_dir: PathBuf,
}

#[async_trait]
impl iac::Resource for LocalStateResource {
    fn resource_type(&self) -> iac::ResourceType {
        iac::ResourceType::new("local_state_dir")
    }

    fn resource_id(&self) -> iac::ResourceId {
        iac::ResourceId("state-dir".into())
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        "local-state"
    }

    async fn create(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        std::fs::create_dir_all(&self.state_dir)
            .map_err(|error| iac::IacError::Other(error.into()))?;
        Ok(iac::ResourceState {
            resource_type: iac::ResourceType::new("local_state_dir"),
            physical_id: self.state_dir.display().to_string(),
            properties: serde_json::json!({ "path": self.state_dir }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: "local-state".into(),
        })
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<Option<iac::ResourceState>, iac::IacError> {
        if self.state_dir.exists() {
            Ok(Some(iac::ResourceState {
                resource_type: iac::ResourceType::new("local_state_dir"),
                physical_id: self.state_dir.display().to_string(),
                properties: serde_json::json!({ "path": self.state_dir }),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "local-state".into(),
            }))
        } else {
            Ok(None)
        }
    }

    fn diff(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        iac::InternalChange::NoChange {
            resource_id: self.resource_id(),
        }
    }
}

// ── Compose service modules ───────────────────────────────────────

/// Compose IaC module that provisions or adopts Aurora DSQL storage.
///
/// It depends on local state so cluster identity and resource state can be
/// recorded before observability/runtime modules need the endpoint writeback.
#[derive(Debug)]
pub struct DsqlModule {
    config: ComposeDsqlConfig,
    project_name: String,
}

impl DsqlModule {
    pub fn new(config: ComposeDsqlConfig, project_name: impl Into<String>) -> Self {
        Self {
            config,
            project_name: project_name.into(),
        }
    }
}

impl iac::Module for DsqlModule {
    fn name(&self) -> &str {
        "dsql"
    }

    fn dependencies(&self) -> &[&str] {
        &["local-state"]
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        if self.config.mode == DsqlMode::Preexisting
            && self.config.endpoint.as_deref().unwrap_or("").is_empty()
        {
            return Err(iac::IacError::Other(anyhow::anyhow!(
                "preexisting DSQL mode requires dsql.endpoint"
            )));
        }

        let cluster_identity = dsql_cluster_identity(&self.project_name);
        let rctx = tokeira_aws::ResourceContext {
            project: self.project_name.clone(),
            region: self.config.region.clone(),
            tags: HashMap::from([("ManagedBy".into(), "tkr".into())]),
        };
        let mode = match self.config.mode {
            DsqlMode::Managed => tokeira_aws::resources::dsql_cluster::DsqlClusterMode::Managed,
            DsqlMode::Preexisting => {
                tokeira_aws::resources::dsql_cluster::DsqlClusterMode::Preexisting
            }
        };
        let config = tokeira_aws::resources::dsql_cluster::DsqlClusterConfig {
            mode,
            preexisting_endpoint: self.config.endpoint.clone(),
            preexisting_arn: self.config.arn.clone(),
            fallback_identifier: None,
            resource_id: None,
            module: self.name().to_owned(),
        };
        let mut resources: Vec<Box<dyn iac::Resource>> = vec![
            Box::new(tokeira_aws::resources::dsql_cluster::DsqlCluster::new(
                cluster_identity,
                config,
                &rctx,
            )),
            Box::new(dsql_coordination_table(
                format!("{}-dsql-rate-limiter", self.project_name),
                self.name(),
                &rctx,
            )),
            Box::new(dsql_coordination_table(
                format!("{}-dsql-conn-lease", self.project_name),
                self.name(),
                &rctx,
            )),
        ];
        resources.shrink_to_fit();
        Ok(resources)
    }
}

fn dsql_coordination_table(
    table_name: String,
    module: &str,
    rctx: &tokeira_aws::ResourceContext,
) -> tokeira_aws::resources::dynamodb_table::DynamoDbTable {
    tokeira_aws::resources::dynamodb_table::DynamoDbTable::new(
        table_name,
        tokeira_aws::resources::dynamodb_table::DynamoDbTableConfig {
            key_schema: vec![tokeira_aws::resources::dynamodb_table::KeyAttribute {
                name: "pk".to_owned(),
                key_type: tokeira_aws::resources::dynamodb_table::KeyType::Hash,
                attribute_type: tokeira_aws::resources::dynamodb_table::AttributeType::String,
            }],
            billing_mode: tokeira_aws::resources::dynamodb_table::BillingMode::OnDemand,
            ttl_attribute: Some("ttl_epoch".to_owned()),
            module: module.to_owned(),
        },
        rctx,
    )
}

pub fn dsql_cluster_identity(project_name: &str) -> String {
    format!("{project_name}-compose")
}

pub fn dsql_resource_id(project_name: &str) -> iac::ResourceId {
    iac::ResourceId(format!("dsql-{}", dsql_cluster_identity(project_name)))
}

/// Groups compose services under a logical module name.
///
/// Each compose service resource reports its owning module via
/// [`module_for_service`], so `infra destroy --module observability`
/// correctly targets mimir, loki, grafana, and alloy.
#[derive(Debug)]
pub struct ComposeModule {
    module_name: String,
    storage: StorageKind,
    config_files: Option<ObservabilityConfigFilesResource>,
    services: Vec<ComposeService>,
}

impl ComposeModule {
    /// Build the runtime module (tokeirad).
    pub fn runtime(config: &ComposeConfig) -> Self {
        let services: Vec<ComposeService> = compose_services(config)
            .into_iter()
            .filter(|s| module_for_service(&s.name) == MODULE_RUNTIME)
            .collect();
        Self {
            module_name: MODULE_RUNTIME.into(),
            storage: config.storage,
            config_files: None,
            services,
        }
    }

    /// Build the observability module (mimir, loki, grafana, alloy).
    pub fn observability(config: &ComposeConfig) -> Self {
        let services: Vec<ComposeService> = compose_services(config)
            .into_iter()
            .filter(|s| module_for_service(&s.name) == MODULE_OBSERVABILITY)
            .collect();
        Self {
            module_name: MODULE_OBSERVABILITY.into(),
            storage: config.storage,
            config_files: Some(ObservabilityConfigFilesResource::new(
                config.deployment_dir.clone(),
                ObservabilityParams::from_config(config),
            )),
            services,
        }
    }
}

impl iac::Module for ComposeModule {
    fn name(&self) -> &str {
        &self.module_name
    }

    fn dependencies(&self) -> &[&str] {
        match (self.module_name.as_str(), self.storage) {
            (MODULE_RUNTIME, StorageKind::Dsql) => &["observability"],
            (MODULE_RUNTIME, StorageKind::InMemory) => &["local-state"],
            (MODULE_OBSERVABILITY, StorageKind::Dsql) => &["local-state", "dsql"],
            (MODULE_OBSERVABILITY, StorageKind::InMemory) => &["local-state", "runtime"],
            _ => &[],
        }
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        let config_resource_id = self
            .config_files
            .as_ref()
            .map(|_| ObservabilityConfigFilesResource::resource_id_value());
        if let Some(config_files) = &self.config_files {
            resources.push(Box::new(config_files.clone()));
        }
        resources.extend(self.services.iter().map(|service| {
            let config_resource_id = if module_for_service(&service.name) == MODULE_OBSERVABILITY {
                config_resource_id.clone()
            } else {
                None
            };
            Box::new(OwnedComposeResource {
                service: service.clone(),
                module_name: self.module_name.clone(),
                config_resource_id,
            }) as Box<dyn iac::Resource>
        }));
        Ok(resources)
    }
}

/// A compose service resource that reports the correct owning module.
#[derive(Debug)]
struct OwnedComposeResource {
    service: ComposeService,
    module_name: String,
    config_resource_id: Option<iac::ResourceId>,
}

#[async_trait]
impl iac::Resource for OwnedComposeResource {
    fn resource_type(&self) -> iac::ResourceType {
        self.service.resource_type()
    }

    fn resource_id(&self) -> iac::ResourceId {
        self.service.resource_id()
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        let mut dependencies = self.service.dependencies();
        if let Some(config_resource_id) = &self.config_resource_id {
            dependencies.push(config_resource_id.clone());
        }
        dependencies
    }

    fn module(&self) -> &str {
        &self.module_name
    }

    async fn create(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        let mut state = self.service.create(ctx).await?;
        state.module = self.module_name.clone();
        state.dependencies = self.dependencies();
        Ok(state)
    }

    async fn update(
        &self,
        current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        let mut state = self.service.update(current, ctx).await?;
        state.module = self.module_name.clone();
        state.dependencies = self.dependencies();
        Ok(state)
    }

    async fn delete(
        &self,
        current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        self.service.delete(current, ctx).await
    }

    async fn describe(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<Option<iac::ResourceState>, iac::IacError> {
        match self.service.describe(ctx).await? {
            Some(mut state) => {
                state.module = self.module_name.clone();
                state.dependencies = self.dependencies();
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    fn diff(
        &self,
        current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        self.service.diff(current, ctx)
    }
}
