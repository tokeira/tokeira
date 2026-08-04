//! The Compose provider's complete kind export.
//!
//! Every authorable Docker/local capability of this provider appears in
//! [`KIND_NAMES`] and decodes through [`decode`]. The engine kind library
//! aggregates provider exports verbatim — no platform curates below this
//! set, so a definition edited within one engine version can adopt any kind
//! the provider ships.

use std::{collections::HashMap, path::PathBuf};

use crate::ComposeService;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokeira_platform::{
    author::{LocatedValue, ValueShape, from_located_value},
    error::KindError,
    kind::{PlacementContext, ProviderKind},
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

        // Declared dependency on the server-config node ⇒ mount the live
        // file and couple to the node's desired-content identity — the same
        // framework-native coupling the configuration resource uses below.
        // The identity digests the node's manifest (which digests the source
        // set's bytes), so a `tokeirad.toml` edit is a manifest diff on this
        // service: the plan states the update and the apply recreates the
        // container onto the new content.
        let server_config_id = server_config_resource_id();
        let mut resource_dependencies = Vec::new();
        if let Some(identity) = placement.dependency_content.get(&server_config_id) {
            volumes.push(format!(
                "{}:/etc/tokeira/tokeirad.toml:ro",
                placement.deployment_dir.join("tokeirad.toml").display()
            ));
            environment.insert(
                "TOKEIRA_CONFIG".to_string(),
                "/etc/tokeira/tokeirad.toml".to_string(),
            );
            environment.insert(
                "TOKEIRA_SERVER_CONFIG_DIGEST".to_string(),
                identity.prefixed_sha256(),
            );
            resource_dependencies.push(server_config_id.0);
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
        crate::canonicalize_manifest(self.compose_service(placement).to_manifest())
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

    /// What a state-dir change does, read from this file's own lifecycle
    /// paths. The headline is the delete: it is deliberately a no-op — the
    /// record retires, `<deployment_dir>/state` and everything in it survive
    /// — so a deletion declares its data **preserved**, the opposite of what
    /// the kind's name suggests.
    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        // Cited by module identity, never repo layout; every name is a real
        // identifier in this module.
        const CREATE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::LocalStateResource::create — std::fs::create_dir_all; an existing \
             tree is left as-is"
        ));
        const DELETE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::LocalStateResource::delete — deliberate no-op (returns Ok(())): \
             the record retires; the directory and its contents survive"
        ));
        local_marker_semantics(ctx.kind, CREATE, DELETE)
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("state directory")
    }
}

/// The engine identity of the deployment's server-config node.
pub fn server_config_resource_id() -> tokeira_iac::ResourceId {
    tokeira_iac::ResourceId("server-config".to_string())
}

/// The deployment's server configuration (`tokeirad.toml`) as an authored
/// graph node. The file is operator-authored: the node's desired manifest
/// digests the interpreted source set's copy (`definition_dir`, so a
/// baseline realization digests the retained bytes), and consumers couple
/// through the framework's dependency-content identity — a `tokeirad.toml`
/// edit diffs every declared consumer, and the graph names the server
/// configuration for ordering, dependants, and dependency loss. The node
/// itself never diffs: its live truth is the same file its desired state
/// reads, so content movement is the consumers' manifests' business.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {}

impl ProviderKind for ServerConfig {
    fn kind_name(&self) -> &'static str {
        "ServerConfig"
    }

    fn validate_input(&self) -> Result<(), KindError> {
        Ok(())
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &[]
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        // The definition-source copy first: a retained revision folder holds
        // the whole desired-source set, so a baseline digests what that
        // revision applied. Retained history from before server-config
        // retention falls back to the live file rather than inventing an
        // edit; an absent file is stated in the manifest and refused at
        // create, never silently skipped.
        let content = [&placement.definition_dir, &placement.deployment_dir]
            .into_iter()
            .map(|dir| dir.join("tokeirad.toml"))
            .find_map(|path| std::fs::read(path).ok())
            .map(|bytes| {
                tokeira_platform::content::ContentIdentity::new("compose/server-config", &bytes)
                    .prefixed_sha256()
            });
        serde_json::json!({ "path": "tokeirad.toml", "content_digest": content })
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        Ok(Box::new(ServerConfigResource {
            path: placement.deployment_dir.join("tokeirad.toml"),
            module: placement.module.clone(),
        }))
    }
}

#[derive(Debug)]
struct ServerConfigResource {
    /// The live file the containers bind-mount.
    path: PathBuf,
    module: String,
}

impl ServerConfigResource {
    fn state(&self) -> tokeira_iac::ResourceState {
        tokeira_iac::ResourceState {
            resource_type: tokeira_iac::Resource::resource_type(self),
            physical_id: self.path.display().to_string(),
            // Constant properties, equal in `create` and `describe`: the
            // node's record must never read as departed — content movement
            // is the consumers' manifests' business.
            properties: serde_json::json!({ "path": "tokeirad.toml" }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.module.clone(),
        }
    }
}

#[async_trait]
impl tokeira_iac::Resource for ServerConfigResource {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new("server_config")
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        server_config_resource_id()
    }

    fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("server configuration")
    }

    async fn create(
        &self,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        // The file is operator-authored; creation records the node, writes
        // nothing — and a definition that declares the node while the file
        // is missing is refused here with the fact, not mounted as a Docker
        // directory stub.
        if !self.path.is_file() {
            return Err(tokeira_iac::IacError::Other(anyhow::anyhow!(
                "the definition declares ServerConfig but {} does not exist",
                self.path.display()
            )));
        }
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
        // Deliberate no-op: the record retires; the operator's file survives.
        Ok(())
    }

    async fn describe(
        &self,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::DescribeResult, tokeira_iac::IacError> {
        Ok(if self.path.is_file() {
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

    /// What a server-config-node change does, read from this file's own
    /// lifecycle paths. Everything mirrors [`LocalStateResource`]: the node
    /// records and retires; the operator's `tokeirad.toml` is never written
    /// or removed by any path here.
    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        // Cited by module identity, never repo layout; every name is a real
        // identifier in this module.
        const CREATE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::ServerConfigResource::create — records the node; writes nothing"
        ));
        const DELETE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::ServerConfigResource::delete — deliberate no-op (returns Ok(())): \
             the record retires; the operator's tokeirad.toml survives"
        ));
        local_marker_semantics(ctx.kind, CREATE, DELETE)
    }
}

/// Shared declaration shape for the two local marker nodes (state dir,
/// server config): every lifecycle path records or retires without touching
/// the operator's filesystem contents, so every field is an engine fact from
/// the cited no-op paths. The diff of both kinds only ever answers NoChange;
/// Update/Replace are declared anyway — totality — from the no-op update.
fn local_marker_semantics(
    kind: tokeira_iac::ChangeKind,
    create: tokeira_iac::Citation,
    delete: tokeira_iac::Citation,
) -> tokeira_iac::ChangeSemantics {
    use tokeira_iac::{
        ChangeKind, ChangeSemantics, Confidence, DataEffect, Disruption, LifecycleOperation,
        ReplacementPolicy, Reversibility,
    };
    let declared = |operation: LifecycleOperation,
                    data_effect: DataEffect,
                    citation: &tokeira_iac::Citation,
                    reversal: &tokeira_iac::Citation| ChangeSemantics {
        operation: Confidence::EngineFact {
            value: operation,
            citation: citation.clone(),
        },
        replacement: Confidence::EngineFact {
            value: ReplacementPolicy::NotRequired,
            citation: citation.clone(),
        },
        disruption: Confidence::EngineFact {
            value: Disruption::None,
            citation: citation.clone(),
        },
        data_effect: Confidence::EngineFact {
            value: data_effect,
            citation: citation.clone(),
        },
        reversibility: Confidence::EngineFact {
            value: Reversibility::Reversible,
            citation: reversal.clone(),
        },
        statement: None,
        provider_assigned: Vec::new(),
    };
    match kind {
        ChangeKind::Create => declared(
            LifecycleOperation::Created,
            DataEffect::NoDataHeld,
            &create,
            &delete,
        ),
        ChangeKind::Update | ChangeKind::Replace => declared(
            LifecycleOperation::UpdatedInPlace,
            DataEffect::Preserved,
            &create,
            &create,
        ),
        ChangeKind::Delete => declared(
            LifecycleOperation::Deleted,
            DataEffect::Preserved,
            &delete,
            &create,
        ),
        ChangeKind::NoChange => ChangeSemantics::default(),
    }
}

/// The Compose provider's closed kind set.
#[derive(Debug)]
pub enum ComposeKind {
    /// Deployment-local state root.
    LocalStateDir(LocalStateDir),
    /// Generated observability configuration.
    ObservabilityConfiguration(ObservabilityConfiguration),
    /// The deployment's server configuration (`tokeirad.toml`).
    ServerConfig(ServerConfig),
    /// Docker Compose service.
    Service(Service),
}

macro_rules! delegate_kind {
    ($self:ident, $method:ident $(, $argument:expr)?) => {
        match $self {
            Self::LocalStateDir(kind) => kind.$method($($argument)?),
            Self::ObservabilityConfiguration(kind) => kind.$method($($argument)?),
            Self::ServerConfig(kind) => kind.$method($($argument)?),
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

/// Complete author-visible Compose-provider kind names, in stable order.
pub const KIND_NAMES: &[&str] = &[
    "LocalStateDir",
    "ObservabilityConfiguration",
    "ServerConfig",
    "Service",
];

/// Provider-owned `<Kind>::EMPTY` defaults.
pub fn defaults(name: &str) -> Option<LocatedValue> {
    (name == "Service").then(service_defaults)
}

/// Decode one named Compose-provider kind from a host-free author value.
pub fn decode(name: &str, value: LocatedValue) -> Result<ComposeKind, KindError> {
    let range = value.range;
    macro_rules! decode_as {
        ($variant:ident, $type:ty) => {
            from_located_value::<$type>(value)
                .map(ComposeKind::$variant)
                .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
        };
    }
    match name {
        "LocalStateDir" => decode_as!(LocalStateDir, LocalStateDir),
        "ObservabilityConfiguration" => {
            decode_as!(ObservabilityConfiguration, ObservabilityConfiguration)
        }
        "ServerConfig" => decode_as!(ServerConfig, ServerConfig),
        "Service" => decode_as!(Service, Service),
        _ => Err(KindError::new(format!(
            "unknown Compose-provider kind `{name}`"
        ))),
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
/// Deterministic operator-facing docker-compose projection of realized
/// service manifests. Tokeira never reads it back.
pub fn inspection_bytes(
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

#[cfg(test)]
mod kind_inventory_tests {
    use super::*;

    // The inventory is the provider's single kind authority: every listed
    // name reaches a decode arm (a listed name may fail decode on missing
    // fields, never as unknown), and unlisted names never decode.
    #[test]
    fn inventory_matches_decode_arms_exactly() {
        for name in KIND_NAMES {
            let probe = decode(
                name,
                LocatedValue::new(ValueShape::Struct {
                    name: (*name).to_string(),
                    fields: Vec::new(),
                }),
            );
            if let Err(error) = probe {
                assert!(
                    !error.message.contains("unknown"),
                    "inventory name `{name}` hit the unknown-kind arm: {}",
                    error.message
                );
            }
        }
        let unknown = decode(
            "NotAComposeKind",
            LocatedValue::new(ValueShape::Struct {
                name: "NotAComposeKind".to_string(),
                fields: Vec::new(),
            }),
        )
        .expect_err("unknown kind must not decode");
        assert!(unknown.message.contains("unknown"));
    }
}
