//! Deployment-owned server-configuration definition kind.
//!
//! [`ServerConfig`] gives every platform the same authored graph node for the
//! deployment's `tokeirad.toml`. The node establishes ordering and a stable
//! content identity without taking ownership of the operator-authored file:
//! create and describe verify its presence, while update and delete never
//! rewrite it. Platform service kinds consume the dependency identity and own
//! their substrate-specific delivery mechanism.

use std::path::PathBuf;

use serde::Deserialize;
use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

use crate::config_history::SERVER_CONFIG;

/// Namespace through which definition frontends admit deployment-owned kinds.
pub const NAMESPACE: &str = "tokeira_deployment";

/// Author-visible name and realized resource type of the server-config node.
pub const TYPE: &str = "ServerConfig";

/// The stable engine identity of the deployment's server-config node.
pub fn resource_id() -> tokeira_iac::ResourceId {
    tokeira_iac::ResourceId("server-config".to_string())
}

/// The deployment's server configuration as an authored graph node.
///
/// The desired manifest digests the interpreted source set's retained copy
/// when available, falling back to the live deployment document for histories
/// created before companion-file retention. A dependent service therefore
/// changes whenever its declared configuration content changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {}

impl Kind<ServerConfigResource> for ServerConfig {
    fn realize(&self, placement: &PlacementContext) -> Result<ServerConfigResource, KindError> {
        Ok(ServerConfigResource {
            path: placement.deployment_dir.join(SERVER_CONFIG),
            definition_path: placement.definition_dir.join(SERVER_CONFIG),
            module: placement.module.clone(),
        })
    }
}

/// Assemble the deployment-owned authoring namespace.
pub fn namespace() -> Namespace {
    Namespace {
        name: NAMESPACE,
        kinds: &[TYPE],
        defaults: None,
        decode,
    }
}

fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    (name == TYPE).then(|| kind::decode_resource::<ServerConfig, ServerConfigResource>(TYPE, value))
}

#[derive(Debug)]
struct ServerConfigResource {
    path: PathBuf,
    definition_path: PathBuf,
    module: String,
}

impl ServerConfigResource {
    fn state(&self) -> tokeira_iac::ResourceState {
        tokeira_iac::ResourceState {
            resource_type: tokeira_iac::Resource::resource_type(self),
            physical_id: self.path.display().to_string(),
            // This constant property keeps create and describe equal. Content
            // movement belongs to dependent manifests, not the node's live
            // state, because both desired and live read the same document.
            properties: serde_json::json!({ "path": SERVER_CONFIG }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.module.clone(),
        }
    }
}

#[async_trait::async_trait]
impl tokeira_iac::Resource for ServerConfigResource {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new(TYPE)
    }

    fn desired_manifest(&self) -> serde_json::Value {
        let content = [&self.definition_path, &self.path]
            .into_iter()
            .find_map(|path| std::fs::read(path).ok())
            .map(|bytes| {
                tokeira_platform::content::ContentIdentity::new("deployment/server-config", &bytes)
                    .prefixed_sha256()
            });
        serde_json::json!({ "path": SERVER_CONFIG, "content_digest": content })
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        resource_id()
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
        // The record retires while the operator's configuration survives.
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
            resource_id: resource_id(),
        }
    }

    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        use tokeira_iac::{
            ChangeKind, ChangeSemantics, Citation, Confidence, DataEffect, Disruption,
            LifecycleOperation, ReplacementPolicy, Reversibility,
        };

        const CREATE: Citation = Citation::code(concat!(
            module_path!(),
            "::ServerConfigResource::create — records the existing operator-owned document"
        ));
        const DELETE: Citation = Citation::code(concat!(
            module_path!(),
            "::ServerConfigResource::delete — retires only the record; the document survives"
        ));
        let declared =
            |operation, data_effect, citation: &Citation, reversal: &Citation| ChangeSemantics {
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
        match ctx.kind {
            ChangeKind::Create => declared(
                LifecycleOperation::Created,
                DataEffect::NoDataHeld,
                &CREATE,
                &DELETE,
            ),
            ChangeKind::Update | ChangeKind::Replace => declared(
                LifecycleOperation::UpdatedInPlace,
                DataEffect::Preserved,
                &CREATE,
                &CREATE,
            ),
            ChangeKind::Delete => declared(
                LifecycleOperation::Deleted,
                DataEffect::Preserved,
                &DELETE,
                &CREATE,
            ),
            ChangeKind::NoChange => ChangeSemantics::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokeira_iac::Resource as _;
    use tokeira_platform::{author::ValueShape, kind::Kind as _};

    use super::*;

    fn placement(root: &std::path::Path) -> PlacementContext {
        PlacementContext {
            deployment_id: "demo".to_string(),
            deployment_dir: root.to_path_buf(),
            definition_dir: root.to_path_buf(),
            module: "runtime".to_string(),
            logical_id: "server_config".to_string(),
            dependencies: Vec::new(),
            dependency_content: Default::default(),
            tags: Default::default(),
        }
    }

    #[test]
    fn namespace_admits_only_server_config() {
        let declaration = namespace();
        assert_eq!(declaration.name, NAMESPACE);
        assert_eq!(declaration.kinds, [TYPE]);
        let value = LocatedValue::new(ValueShape::Struct {
            name: TYPE.to_string(),
            fields: Vec::new(),
        });
        assert!(decode(TYPE, value.clone()).is_some());
        assert!(decode("Unknown", value).is_none());
    }

    #[test]
    fn desired_manifest_uses_the_shared_document_identity() {
        let root = tempfile::tempdir().expect("temporary deployment");
        std::fs::write(root.path().join(SERVER_CONFIG), "[infrastructure]\n")
            .expect("server config");
        let resource = ServerConfig {}
            .realize(&placement(root.path()))
            .expect("server config realizes");

        assert_eq!(resource.resource_id(), resource_id());
        assert_eq!(resource.resource_type().0, TYPE);
        assert_eq!(resource.desired_manifest()["path"], SERVER_CONFIG);
        assert!(
            resource.desired_manifest()["content_digest"]
                .as_str()
                .is_some_and(|digest| digest.starts_with("sha256:"))
        );
    }
}
