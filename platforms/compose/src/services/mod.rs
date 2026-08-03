//! Compose service kinds and platform-local resources.

use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokeira_aws::kinds::{dsql_cluster::DsqlCluster, dynamodb_table::DynamoDbTable};
use tokeira_compose::ComposeService;
use tokeira_platform::{
    author::{LocatedValue, ValueShape, from_located_value},
    error::KindError,
    kind::{KindFunctions, PlacementContext, ProviderKind},
};

use crate::observability::ObservabilityConfiguration;

/// One environment entry without tuple-shaped author data.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    /// Environment variable name.
    pub name: String,
    /// Environment variable value.
    pub value: String,
}

/// Platform-owned logical volume vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum Volume {
    /// Persistent path beneath the deployment's local state root.
    State(StateVolume),
    /// Generated path beneath the deployment's configuration root.
    Config(ConfigVolume),
    /// Docker daemon socket.
    DockerSocket,
}

/// Persistent state mount.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateVolume {
    /// Logical state subpath.
    pub sub: String,
    /// Container mount target.
    pub at: String,
}

/// Generated configuration mount.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigVolume {
    /// Logical configuration subpath.
    pub sub: String,
    /// Container mount target.
    pub at: String,
}

/// Authored Compose service resource.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    /// Image reference.
    pub image: String,
    /// Desired replicas.
    pub replicas: u32,
    /// Published equal host/container ports.
    pub publish: Vec<u16>,
    /// Platform-resolved volumes.
    pub volumes: Vec<Volume>,
    /// Explicit environment entries.
    pub environment: Vec<Environment>,
    /// Container command.
    pub command: Vec<String>,
    /// Compose service start-order dependencies.
    pub depends_on: Vec<String>,
    /// Mount and couple the deployment's `tokeirad.toml`.
    pub server_config: bool,
    /// Add the non-secret AWS runtime selectors for this region.
    pub aws_region: Option<String>,
}

impl Service {
    fn validate(&self) -> Result<(), KindError> {
        if self.image.is_empty() {
            return Err(KindError::new("Compose service image cannot be empty"));
        }
        if self.replicas == 0 {
            return Err(KindError::new(
                "Compose service replicas must be greater than zero",
            ));
        }
        if self.publish.contains(&0) {
            return Err(KindError::new(
                "Compose service published ports must be greater than zero",
            ));
        }
        Ok(())
    }

    fn compose_service(&self, placement: &PlacementContext) -> ComposeService {
        let mut volumes = self
            .volumes
            .iter()
            .map(|volume| match volume {
                Volume::State(StateVolume { sub, at }) => format!(
                    "{}:{at}",
                    placement
                        .deployment_dir
                        .join(".tokeira-state")
                        .join(sub)
                        .display()
                ),
                Volume::Config(ConfigVolume { sub, at }) => format!(
                    "{}:{at}",
                    placement.deployment_dir.join("config").join(sub).display()
                ),
                Volume::DockerSocket => "/var/run/docker.sock:/var/run/docker.sock".to_string(),
            })
            .collect::<Vec<_>>();
        let mut environment = self
            .environment
            .iter()
            .map(|entry| (entry.name.clone(), entry.value.clone()))
            .collect::<HashMap<_, _>>();

        if self.server_config {
            let path = placement.deployment_dir.join("tokeirad.toml");
            if let Ok(bytes) = std::fs::read(&path) {
                volumes.push(format!("{}:/etc/tokeira/tokeirad.toml:ro", path.display()));
                environment.insert(
                    "TOKEIRA_CONFIG".to_string(),
                    "/etc/tokeira/tokeirad.toml".to_string(),
                );
                environment.insert(
                    "TOKEIRA_SERVER_CONFIG_DIGEST".to_string(),
                    tokeira_platform::content::ContentIdentity::new(
                        "compose/server-config",
                        &bytes,
                    )
                    .prefixed_sha256(),
                );
            }
        }

        if let Some(region) = &self.aws_region {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
            volumes.push(format!("{home}/.aws:/home/nonroot/.aws:ro"));
            environment.insert("HOME".to_string(), "/home/nonroot".to_string());
            environment.insert("AWS_REGION".to_string(), region.clone());
            if let Ok(profile) = std::env::var("AWS_PROFILE") {
                environment.insert("AWS_PROFILE".to_string(), profile);
            }
        }

        let config_id = crate::observability::configuration_resource_id();
        let mut resource_dependencies = Vec::new();
        if self
            .volumes
            .iter()
            .any(|volume| matches!(volume, Volume::Config(_)))
            && let Some(identity) = placement.dependency_content.get(&config_id)
        {
            resource_dependencies.push(config_id.0);
            environment.insert(
                "TOKEIRA_CONFIG_DIGEST".to_string(),
                identity.prefixed_sha256(),
            );
        }

        ComposeService {
            name: placement.logical_id.clone(),
            image: self.image.clone(),
            ports: self
                .publish
                .iter()
                .map(|port| format!("{port}:{port}"))
                .collect(),
            volumes,
            environment,
            depends_on: self.depends_on.clone(),
            healthcheck: None,
            command: self.command.clone(),
            resource_dependencies,
        }
    }
}

impl ProviderKind for Service {
    fn kind_name(&self) -> &'static str {
        "Service"
    }

    fn validate_input(&self) -> Result<(), KindError> {
        self.validate()
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &[]
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        tokeira_compose::canonicalize_manifest(self.compose_service(placement).to_manifest())
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        self.validate()?;
        Ok(Box::new(PlacedService {
            service: self.compose_service(placement),
            module: placement.module.clone(),
        }))
    }
}

#[derive(Debug)]
struct PlacedService {
    service: ComposeService,
    module: String,
}

#[async_trait]
impl tokeira_iac::Resource for PlacedService {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::Resource::resource_type(&self.service)
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        tokeira_iac::Resource::resource_id(&self.service)
    }

    fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
        tokeira_iac::Resource::dependencies(&self.service)
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(
        &self,
        context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        tokeira_iac::Resource::create(&self.service, context).await
    }

    async fn update(
        &self,
        current: &tokeira_iac::ResourceState,
        context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        tokeira_iac::Resource::update(&self.service, current, context).await
    }

    async fn delete(
        &self,
        current: &tokeira_iac::ResourceState,
        context: &tokeira_iac::ProvisionContext,
    ) -> Result<(), tokeira_iac::IacError> {
        tokeira_iac::Resource::delete(&self.service, current, context).await
    }

    async fn describe(
        &self,
        context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::DescribeResult, tokeira_iac::IacError> {
        tokeira_iac::Resource::describe(&self.service, context).await
    }

    fn diff(
        &self,
        current: &tokeira_iac::ResourceState,
        context: &tokeira_iac::ProvisionContext,
    ) -> tokeira_iac::InternalChange {
        tokeira_iac::Resource::diff(&self.service, current, context)
    }

    fn change_semantics(
        &self,
        context: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        tokeira_iac::Resource::change_semantics(&self.service, context)
    }

    fn display_kind(&self) -> Option<&'static str> {
        tokeira_iac::Resource::display_kind(&self.service)
    }
}

/// Marker for the deployment-local infrastructure state root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStateDir {}

impl ProviderKind for LocalStateDir {
    fn kind_name(&self) -> &'static str {
        "LocalStateDir"
    }

    fn validate_input(&self) -> Result<(), KindError> {
        Ok(())
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &[]
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        serde_json::json!({ "path": placement.deployment_dir.join("state") })
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        Ok(Box::new(LocalStateResource {
            state_dir: placement.deployment_dir.join("state"),
            module: placement.module.clone(),
        }))
    }
}

#[derive(Debug)]
struct LocalStateResource {
    state_dir: PathBuf,
    module: String,
}

impl LocalStateResource {
    fn state(&self) -> tokeira_iac::ResourceState {
        tokeira_iac::ResourceState {
            resource_type: tokeira_iac::Resource::resource_type(self),
            physical_id: self.state_dir.display().to_string(),
            properties: serde_json::json!({ "path": self.state_dir }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.module.clone(),
        }
    }
}

#[async_trait]
impl tokeira_iac::Resource for LocalStateResource {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new("local_state_dir")
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        tokeira_iac::ResourceId("state-dir".to_string())
    }

    fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(
        &self,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        std::fs::create_dir_all(&self.state_dir)
            .map_err(|error| tokeira_iac::IacError::Other(error.into()))?;
        Ok(self.state())
    }

    async fn update(
        &self,
        current: &tokeira_iac::ResourceState,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &tokeira_iac::ResourceState,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<(), tokeira_iac::IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::DescribeResult, tokeira_iac::IacError> {
        Ok(if self.state_dir.exists() {
            tokeira_iac::DescribeResult::Present(self.state())
        } else {
            tokeira_iac::DescribeResult::Absent
        })
    }

    fn diff(
        &self,
        _current: &tokeira_iac::ResourceState,
        _context: &tokeira_iac::ProvisionContext,
    ) -> tokeira_iac::InternalChange {
        tokeira_iac::InternalChange::NoChange {
            resource_id: tokeira_iac::Resource::resource_id(self),
        }
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("state directory")
    }
}

/// Closed first-party Compose kind set selected at compile time.
#[derive(Debug)]
pub enum ComposeKind {
    /// Aurora DSQL cluster.
    DsqlCluster(DsqlCluster),
    /// DynamoDB coordination table.
    DynamoDbTable(DynamoDbTable),
    /// Deployment-local state root.
    LocalStateDir(LocalStateDir),
    /// Generated observability configuration.
    ObservabilityConfiguration(ObservabilityConfiguration),
    /// Docker Compose service.
    Service(Service),
}

macro_rules! delegate_kind {
    ($self:ident, $method:ident $(, $argument:expr)?) => {
        match $self {
            Self::DsqlCluster(kind) => kind.$method($($argument)?),
            Self::DynamoDbTable(kind) => kind.$method($($argument)?),
            Self::LocalStateDir(kind) => kind.$method($($argument)?),
            Self::ObservabilityConfiguration(kind) => kind.$method($($argument)?),
            Self::Service(kind) => kind.$method($($argument)?),
        }
    };
}

impl ProviderKind for ComposeKind {
    fn kind_name(&self) -> &'static str {
        delegate_kind!(self, kind_name)
    }

    fn validate_input(&self) -> Result<(), KindError> {
        delegate_kind!(self, validate_input)
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        delegate_kind!(self, declared_outputs)
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        delegate_kind!(self, desired_manifest, placement)
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        delegate_kind!(self, realize, placement)
    }
}

/// Compile-time constructor functions for the Compose first-party set.
pub fn kind_functions() -> KindFunctions<ComposeKind> {
    KindFunctions {
        contains: |name| {
            matches!(
                name,
                "DsqlCluster"
                    | "DynamoDbTable"
                    | "LocalStateDir"
                    | "ObservabilityConfiguration"
                    | "Service"
            )
        },
        defaults: |name| (name == "Service").then(service_defaults),
        decode,
    }
}

fn decode(name: &str, value: LocatedValue) -> Result<ComposeKind, KindError> {
    let range = value.range;
    macro_rules! decode_as {
        ($variant:ident, $type:ty) => {
            from_located_value::<$type>(value)
                .map(ComposeKind::$variant)
                .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
        };
    }
    match name {
        "DsqlCluster" => decode_as!(DsqlCluster, DsqlCluster),
        "DynamoDbTable" => decode_as!(DynamoDbTable, DynamoDbTable),
        "LocalStateDir" => decode_as!(LocalStateDir, LocalStateDir),
        "ObservabilityConfiguration" => {
            decode_as!(ObservabilityConfiguration, ObservabilityConfiguration)
        }
        "Service" => decode_as!(Service, Service),
        _ => Err(KindError::new(format!("unknown Compose kind `{name}`"))),
    }
}

fn service_defaults() -> LocatedValue {
    LocatedValue::new(ValueShape::Struct {
        name: "Service".to_string(),
        fields: vec![
            ("image".to_string(), LocatedValue::string("")),
            (
                "replicas".to_string(),
                LocatedValue::new(ValueShape::Integer(0)),
            ),
            (
                "publish".to_string(),
                LocatedValue::new(ValueShape::Sequence(Vec::new())),
            ),
            (
                "volumes".to_string(),
                LocatedValue::new(ValueShape::Sequence(Vec::new())),
            ),
            (
                "environment".to_string(),
                LocatedValue::new(ValueShape::Sequence(Vec::new())),
            ),
            (
                "command".to_string(),
                LocatedValue::new(ValueShape::Sequence(Vec::new())),
            ),
            (
                "depends_on".to_string(),
                LocatedValue::new(ValueShape::Sequence(Vec::new())),
            ),
            (
                "server_config".to_string(),
                LocatedValue::new(ValueShape::Bool(false)),
            ),
            (
                "aws_region".to_string(),
                LocatedValue::new(ValueShape::Option(None)),
            ),
        ],
    })
}

#[derive(Debug, Serialize)]
struct InspectionDocument {
    services: std::collections::BTreeMap<String, InspectionService>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InspectionService {
    image: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ports: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    environment: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    command: Vec<String>,
}

/// Render the deterministic operator-facing Compose inspection projection.
pub(crate) fn inspection_bytes(
    manifests: &std::collections::BTreeMap<tokeira_iac::ResourceId, serde_json::Value>,
) -> Result<Vec<u8>, serde_yaml::Error> {
    let services = manifests
        .iter()
        .filter(|(id, manifest)| id.0.starts_with("compose/") && manifest.get("image").is_some())
        .map(|(id, manifest)| {
            serde_json::from_value::<InspectionService>(manifest.clone()).map(|service| {
                (
                    id.0.strip_prefix("compose/")
                        .expect("the filtered id has a Compose prefix")
                        .to_string(),
                    service,
                )
            })
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, _>>()
        .map_err(|error| <serde_yaml::Error as serde::de::Error>::custom(error.to_string()))?;
    serde_yaml::to_string(&InspectionDocument { services }).map(String::into_bytes)
}
