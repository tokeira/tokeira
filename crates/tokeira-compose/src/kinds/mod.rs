//! The Compose provider's kinds and namespace facts.
//!
//! Kinds and resources are distinct: each kind here is the authored face of
//! one resource and realizes it directly — [`Service`] realizes the
//! [`ComposeService`] model in the crate root; the two local marker kinds
//! realize their node resources beside them. Each resource owns its one
//! word as a `TYPE` const; the kinds recover it. The namespace facts at the
//! bottom — `NAMESPACE`, `KINDS`, `decode` — are what the assembled
//! binary lists for the frontend; nothing is registered anywhere else.

use std::path::PathBuf;

use async_trait::async_trait;
use serde::Deserialize;
use tokeira_platform::{
    author::LocatedValue,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

use crate::{
    ComposeService, Environment, Healthcheck, PullPolicy, Volume, config_content_resource_id,
};

/// The authored face of a compose service. Realizes the [`ComposeService`]
/// model directly: the service name comes from the graph's logical id, and
/// the content couplings come from declared dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    /// Image reference.
    pub(crate) image: String,
    /// Registry resolution policy carried into the service manifest.
    #[serde(default)]
    pub(crate) pull_policy: PullPolicy,
    /// Desired replicas.
    #[serde(default = "crate::kinds::default_replicas")]
    pub(crate) replicas: u32,
    /// Published equal host/container ports.
    #[serde(default)]
    pub(crate) publish: Vec<u16>,
    /// Platform-resolved logical volumes.
    #[serde(default)]
    pub(crate) volumes: Vec<Volume>,
    /// Explicit environment entries.
    #[serde(default)]
    pub(crate) environment: Vec<Environment>,
    /// Container command.
    #[serde(default)]
    pub(crate) command: Vec<String>,
    /// Compose service start-order dependencies.
    #[serde(default)]
    pub(crate) depends_on: Vec<String>,
    /// Optional Docker healthcheck definition.
    #[serde(default)]
    pub(crate) healthcheck: Option<Healthcheck>,
    /// Add the non-secret AWS runtime selectors for this region.
    #[serde(default)]
    pub(crate) aws_region: Option<String>,
}

fn default_replicas() -> u32 {
    1
}

impl Service {
    /// Realize the model: authored fields carry over, the name comes from
    /// the logical id, and declared dependencies couple content identities.
    /// A dependency on the server-config node couples its desired-content
    /// identity, so a `tokeirad.toml` edit is a manifest diff on this
    /// service; a `Config` volume with the rendered-content dependency
    /// couples the same way through [`config_content_resource_id`].
    fn realized(&self, placement: &PlacementContext) -> ComposeService {
        let mut service = ComposeService {
            name: placement.logical_id.clone(),
            image: self.image.clone(),
            pull_policy: self.pull_policy.clone(),
            replicas: self.replicas,
            publish: self.publish.clone(),
            volumes: self.volumes.clone(),
            environment: self.environment.clone(),
            depends_on: self.depends_on.clone(),
            healthcheck: self.healthcheck.clone(),
            command: self.command.clone(),
            aws_region: self.aws_region.clone(),
            server_config_digest: None,
            config_digest: None,
            module: placement.module.clone(),
        };
        let server_config_id = server_config_resource_id();
        if let Some(identity) = placement.dependency_content.get(&server_config_id) {
            service.server_config_digest = Some(identity.prefixed_sha256());
        }
        let config_id = config_content_resource_id();
        if self
            .volumes
            .iter()
            .any(|volume| matches!(volume, Volume::Config(_)))
            && let Some(identity) = placement.dependency_content.get(&config_id)
        {
            service.config_digest = Some(identity.prefixed_sha256());
        }
        service
    }
}

impl Kind<ComposeService> for Service {
    fn realize(&self, placement: &PlacementContext) -> Result<ComposeService, KindError> {
        Ok(self.realized(placement))
    }
}

/// Marker for the deployment-local infrastructure state root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStateDir {}

impl Kind<LocalStateResource> for LocalStateDir {
    fn realize(&self, placement: &PlacementContext) -> Result<LocalStateResource, KindError> {
        Ok(LocalStateResource {
            state_dir: placement.deployment_dir.join("state"),
            module: placement.module.clone(),
        })
    }
}

/// The state-dir resource the [`LocalStateDir`] kind realizes.
#[derive(Debug)]
struct LocalStateResource {
    state_dir: PathBuf,
    module: String,
}

impl LocalStateResource {
    /// The resource's one word: engine resource type and author-visible
    /// name, stated once here.
    const TYPE: &'static str = "LocalStateDir";

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
        tokeira_iac::ResourceType::new(Self::TYPE)
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({ "path": self.state_dir })
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
    /// the resource's name suggests.
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
pub(crate) fn server_config_resource_id() -> tokeira_iac::ResourceId {
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

impl Kind<ServerConfigResource> for ServerConfig {
    fn realize(&self, placement: &PlacementContext) -> Result<ServerConfigResource, KindError> {
        Ok(ServerConfigResource {
            path: placement.deployment_dir.join("tokeirad.toml"),
            definition_path: placement.definition_dir.join("tokeirad.toml"),
            module: placement.module.clone(),
        })
    }
}

/// The server-config node resource the [`ServerConfig`] kind realizes.
#[derive(Debug)]
struct ServerConfigResource {
    /// The live file the containers bind-mount.
    path: PathBuf,
    /// The interpreted source-set copy used for retained-revision evidence.
    definition_path: PathBuf,
    module: String,
}

impl ServerConfigResource {
    /// The resource's one word: engine resource type and author-visible
    /// name, stated once here.
    const TYPE: &'static str = "ServerConfig";

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
        tokeira_iac::ResourceType::new(Self::TYPE)
    }

    fn desired_manifest(&self) -> serde_json::Value {
        // A retained source-set copy wins; history predating companion-file
        // retention falls back to the live file instead of inventing an edit.
        let content = [&self.definition_path, &self.path]
            .into_iter()
            .find_map(|path| std::fs::read(path).ok())
            .map(|bytes| {
                tokeira_platform::content::ContentIdentity::new("compose/server-config", &bytes)
                    .prefixed_sha256()
            });
        serde_json::json!({ "path": "tokeirad.toml", "content_digest": content })
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
    /// lifecycle paths. Everything mirrors [`LocalStateDir`]: the node
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

/// The namespace word: the normalized crate name definitions import from.
pub(crate) const NAMESPACE: &str = "tokeira_compose";

/// The provider's author-visible kind names, each the word its resource
/// owns.
pub(crate) const KINDS: &[&str] = &[
    LocalStateResource::TYPE,
    ServerConfigResource::TYPE,
    ComposeService::TYPE,
];

/// Authoring-only empty shapes for frontends with explicit struct-update
/// syntax. The values mirror this kind's Serde defaults; they do not carry
/// validation, outputs, realization, or lifecycle behaviour.
pub(crate) fn defaults(name: &str) -> Option<LocatedValue> {
    (name == ComposeService::TYPE).then(|| {
        LocatedValue::new(tokeira_platform::author::ValueShape::Struct {
            name: ComposeService::TYPE.to_string(),
            fields: vec![
                ("image".to_string(), LocatedValue::string("")),
                ("pull_policy".to_string(), LocatedValue::string("missing")),
                (
                    "replicas".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Integer(1)),
                ),
                (
                    "publish".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Sequence(Vec::new())),
                ),
                (
                    "volumes".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Sequence(Vec::new())),
                ),
                (
                    "environment".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Sequence(Vec::new())),
                ),
                (
                    "command".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Sequence(Vec::new())),
                ),
                (
                    "depends_on".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Sequence(Vec::new())),
                ),
                (
                    "healthcheck".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Option(None)),
                ),
                (
                    "aws_region".to_string(),
                    LocatedValue::new(tokeira_platform::author::ValueShape::Option(None)),
                ),
            ],
        })
    })
}

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub(crate) fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    Some(match name {
        LocalStateResource::TYPE => kind::decode_resource::<LocalStateDir, LocalStateResource>(
            LocalStateResource::TYPE,
            value,
        ),
        ServerConfigResource::TYPE => kind::decode_resource::<ServerConfig, ServerConfigResource>(
            ServerConfigResource::TYPE,
            value,
        ),
        ComposeService::TYPE => {
            kind::decode_service::<Service, ComposeService>(ComposeService::TYPE, value)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod kind_inventory_tests {
    use super::*;
    use tokeira_platform::author::ValueShape;

    fn placement() -> PlacementContext {
        PlacementContext {
            deployment_id: "demo".to_string(),
            deployment_dir: PathBuf::from("/deployments/demo"),
            definition_dir: PathBuf::from("/deployments/demo"),
            module: "runtime".to_string(),
            logical_id: "tokeirad".to_string(),
            dependencies: Vec::new(),
            dependency_content: Default::default(),
            tags: Default::default(),
        }
    }

    // The namespace facts hold together: every listed name decodes here,
    // and an unknown name is refused as not-ours rather than an error.
    #[test]
    fn every_listed_kind_decodes_and_unknown_names_refuse() {
        for name in KINDS {
            let probe = decode(
                name,
                LocatedValue::new(ValueShape::Struct {
                    name: (*name).to_string(),
                    fields: Vec::new(),
                }),
            );
            assert!(probe.is_some(), "kind `{name}` must belong to {NAMESPACE}");
        }
        assert!(decode("Unknown", LocatedValue::new(ValueShape::Unit)).is_none());
    }

    // The service decodes partially: omission is the default — an authored
    // Service stating only its image admits, with replicas defaulting to 1.
    #[test]
    fn service_decodes_with_field_defaults() {
        let decoded = decode(
            ComposeService::TYPE,
            LocatedValue::new(ValueShape::Struct {
                name: ComposeService::TYPE.to_string(),
                fields: vec![("image".to_string(), LocatedValue::string("tokeirad:latest"))],
            }),
        )
        .expect("Service belongs to this namespace")
        .expect("partial authoring decodes");
        assert_eq!(decoded.name(), ComposeService::TYPE);
    }

    #[test]
    fn kinds_realize_their_concrete_resources() {
        let service = Service {
            image: "tokeirad:latest".to_string(),
            pull_policy: PullPolicy::Never,
            replicas: 1,
            publish: Vec::new(),
            volumes: Vec::new(),
            environment: Vec::new(),
            command: Vec::new(),
            depends_on: Vec::new(),
            healthcheck: None,
            aws_region: None,
        };
        let realized: ComposeService = service.realize(&placement()).expect("service realizes");
        assert_eq!(realized.name, "tokeirad");
        assert_eq!(realized.pull_policy, PullPolicy::Never);

        let realized: LocalStateResource = LocalStateDir {}
            .realize(&placement())
            .expect("state directory realizes");
        assert_eq!(realized.state_dir, PathBuf::from("/deployments/demo/state"));
    }
}
