//! Definition source admission, frontend evaluation, and invocation-bound realization.

use std::{collections::BTreeMap, fmt, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::{
    author::LocatedValue,
    config::admit_config,
    error::{
        ConfigError, DefinitionError, FrontendDiagnostic, ProjectionError, VerificationFinding,
        VerificationReport,
    },
    graph::{VerifiedGraph, WritebackValue},
    kind::{KindFunctions, PlacementContext, ProviderKind},
};

/// Source identity safe to render in frontend diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionSourceName {
    /// Persistable deployment-root relative identity.
    DeploymentRelative(RelativeDefinitionPath),
    /// Explicit standalone authoring path, never valid as deployment metadata.
    AuthoringPath(PathBuf),
}

impl fmt::Display for DefinitionSourceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeploymentRelative(path) => f.write_str(path.as_str()),
            Self::AuthoringPath(path) => write!(f, "{}", path.display()),
        }
    }
}

/// Exact owned source and independently selected format.
#[derive(Debug, Clone)]
pub struct DefinitionSource {
    /// Recorded or explicitly selected frontend format.
    pub format: DefinitionFormatId,
    /// Display/persistence-safe source identity.
    pub source_name: DefinitionSourceName,
    /// Exact bytes evaluated and hashed for configuration identity.
    pub bytes: Arc<[u8]>,
}

/// One borrow of an admitted definition source.
#[derive(Debug, Clone, Copy)]
pub struct FrontendSource<'a> {
    /// Display-safe source identity.
    pub source_name: &'a DefinitionSourceName,
    /// Exact source bytes.
    pub bytes: &'a [u8],
}

/// Completed transient structure returned by a definition frontend.
#[derive(Debug)]
pub struct FrontendOutput<K> {
    /// Host-free platform configuration value.
    pub config: LocatedValue,
    /// Completed structural graph built inside the frontend evaluator.
    pub graph: VerifiedGraph<K>,
}

/// Statically assembled evaluator for one definition format.
pub trait DefinitionFrontend: Clone + Send + Sync + 'static {
    /// Open validated format identity embedded in the assembled provisioner.
    fn format(&self) -> &DefinitionFormatId;

    /// Evaluate typed context into one completed transient structure.
    fn evaluate<C, K>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        kinds: KindFunctions<K>,
    ) -> Result<FrontendOutput<K>, FrontendDiagnostic>
    where
        C: Serialize,
        K: ProviderKind + 'static;
}

/// Versioned content identity of format plus exact definition bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationIdentity {
    algorithm: ConfigurationIdentityAlgorithm,
    /// Lowercase SHA-256 digest.
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum ConfigurationIdentityAlgorithm {
    #[serde(rename = "sha256-v1")]
    Sha256V1,
}

impl ConfigurationIdentity {
    /// Compute a path-, state-, and context-independent configuration identity.
    pub fn compute(format: &DefinitionFormatId, bytes: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"tokeira.configuration.v1\0");
        digest.update((format.as_str().len() as u64).to_be_bytes());
        digest.update(format.as_str().as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
        Self {
            algorithm: ConfigurationIdentityAlgorithm::Sha256V1,
            digest: hex::encode(digest.finalize()),
        }
    }

    /// Stable serialized algorithm/version label.
    pub fn algorithm(&self) -> &'static str {
        match self.algorithm {
            ConfigurationIdentityAlgorithm::Sha256V1 => "sha256-v1",
        }
    }
}

/// Typed config, immutable graph, and source identity admitted in memory.
#[derive(Debug)]
pub struct EvaluatedDefinition<C, K> {
    /// Typed platform config.
    pub config: C,
    /// Completed structural graph.
    pub graph: VerifiedGraph<K>,
    /// Format-plus-source configuration identity.
    pub configuration_identity: ConfigurationIdentity,
}

/// Evaluate a source, admit its typed config, and retain no frontend runtime state.
pub fn evaluate_definition<Cx, C, K, F>(
    frontend: &F,
    source: DefinitionSource,
    context: &Cx,
    kinds: KindFunctions<K>,
    validate_config: fn(&C) -> Result<(), ConfigError>,
) -> Result<EvaluatedDefinition<C, K>, DefinitionError>
where
    Cx: Serialize,
    C: DeserializeOwned,
    K: ProviderKind + 'static,
    F: DefinitionFrontend,
{
    if &source.format != frontend.format() {
        return Err(DefinitionError::FormatMismatch {
            source_format: source.format,
            frontend_format: frontend.format().clone(),
        });
    }
    let identity = ConfigurationIdentity::compute(&source.format, source.bytes.as_ref());
    let format = source.format.clone();
    let source_name = source.source_name.clone();
    let output = frontend.evaluate(
        FrontendSource {
            source_name: &source.source_name,
            bytes: source.bytes.as_ref(),
        },
        context,
        kinds,
    )?;
    let config =
        admit_config(output.config, validate_config).map_err(|error| DefinitionError::Config {
            format,
            source_name,
            error,
        })?;
    Ok(EvaluatedDefinition {
        config,
        graph: output.graph,
        configuration_identity: identity,
    })
}

/// Definition whose complete concrete kind set passed pure input validation.
pub struct VerifiedDefinition<'a, C, K> {
    definition: &'a EvaluatedDefinition<C, K>,
}

impl<C, K> fmt::Debug for VerifiedDefinition<'_, C, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedDefinition")
            .field("resource_count", &self.definition.graph.resources().len())
            .finish()
    }
}

impl<'a, C, K> VerifiedDefinition<'a, C, K> {
    /// Borrow the exact evaluated definition that was validated.
    pub fn definition(&self) -> &'a EvaluatedDefinition<C, K> {
        self.definition
    }
}

/// Validate every concrete kind input without fabricating invocation facts.
pub fn verify_definition<C, K: ProviderKind>(
    definition: &EvaluatedDefinition<C, K>,
) -> Result<VerifiedDefinition<'_, C, K>, VerificationReport> {
    let findings =
        definition
            .graph
            .resources()
            .iter()
            .filter_map(|resource| {
                resource.kind().validate_input().err().map(|error| {
                    VerificationFinding::InvalidInput {
                        resource: format!("{}/{}", resource.module(), resource.logical_id()),
                        provider_kind: resource.kind().kind_name().to_string(),
                        message: error.message,
                    }
                })
            })
            .collect::<Vec<_>>();
    if findings.is_empty() {
        Ok(VerifiedDefinition { definition })
    } else {
        Err(VerificationReport { findings })
    }
}

/// Logical-to-engine identity produced by the one execution realization.
#[derive(Debug, Clone, Default)]
pub struct RealizedResourceIndex {
    ids: BTreeMap<(String, String), tokeira_iac::ResourceId>,
}

impl RealizedResourceIndex {
    /// Resolve one logical resource to its engine identity.
    pub fn get(&self, module: &str, resource: &str) -> Option<&tokeira_iac::ResourceId> {
        self.ids.get(&(module.to_string(), resource.to_string()))
    }
}

/// Complete invocation-bound realization in declaration order.
pub struct RealizedResources {
    index: RealizedResourceIndex,
    resources: Vec<Box<dyn tokeira_iac::Resource>>,
}

impl fmt::Debug for RealizedResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealizedResources")
            .field("index", &self.index)
            .field("resource_count", &self.resources.len())
            .finish()
    }
}

impl RealizedResources {
    /// Borrow the logical-to-engine identity index.
    pub fn index(&self) -> &RealizedResourceIndex {
        &self.index
    }

    /// Borrow resources in source declaration order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &dyn tokeira_iac::Resource> {
        self.resources.iter().map(Box::as_ref)
    }

    /// Transfer resources to the infrastructure engine.
    pub fn into_resources(self) -> Vec<Box<dyn tokeira_iac::Resource>> {
        self.resources
    }
}

impl<C, K: ProviderKind> VerifiedDefinition<'_, C, K> {
    /// Realize the verified set once with real invocation identity and placement.
    pub fn realize(
        &self,
        deployment_id: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<RealizedResources, ProjectionError> {
        let nodes = self.definition.graph.resources();
        let mut index = RealizedResourceIndex::default();
        let mut pending = (0..nodes.len()).collect::<Vec<_>>();
        let mut realized = std::iter::repeat_with(|| None)
            .take(nodes.len())
            .collect::<Vec<Option<Box<dyn tokeira_iac::Resource>>>>();
        while !pending.is_empty() {
            let Some(position) = pending.iter().position(|node_index| {
                nodes[*node_index].dependencies().iter().all(|dependency| {
                    index
                        .get(dependency.module(), dependency.logical_id())
                        .is_some()
                })
            }) else {
                return Err(ProjectionError {
                    resource: "structural-graph".to_string(),
                    provider_kind: "dependency-order".to_string(),
                    message: "verified resource dependencies could not be ordered".to_string(),
                });
            };
            let node_index = pending.remove(position);
            let node = &nodes[node_index];
            let dependencies = node
                .dependencies()
                .iter()
                .map(|dependency| {
                    index
                        .get(dependency.module(), dependency.logical_id())
                        .cloned()
                        .expect("the selected resource has realized dependencies")
                })
                .collect();
            let placement = PlacementContext {
                deployment_id: deployment_id.to_string(),
                module: node.module().to_string(),
                logical_id: node.logical_id().to_string(),
                dependencies,
                tags: tags.clone(),
            };
            let resource = node
                .kind()
                .realize(&placement)
                .map_err(|error| ProjectionError {
                    resource: format!("{}/{}", node.module(), node.logical_id()),
                    provider_kind: node.kind().kind_name().to_string(),
                    message: error.message,
                })?;
            index.ids.insert(
                (node.module().to_string(), node.logical_id().to_string()),
                resource.resource_id(),
            );
            realized[node_index] = Some(resource);
        }

        let mut resources = Vec::with_capacity(realized.len());
        for entry in realized {
            resources.push(entry.expect("every verified resource was realized"));
        }
        Ok(RealizedResources { index, resources })
    }

    /// Resolve only explicitly declared writebacks from applied state.
    pub fn resolve_writeback(
        &self,
        realized: &RealizedResources,
        state: &tokeira_iac::InfraState,
    ) -> Vec<(String, String)> {
        self.definition
            .graph
            .writeback()
            .iter()
            .filter_map(|entry| {
                let value = match entry.value() {
                    WritebackValue::Literal(value) => Some(value.clone()),
                    WritebackValue::Output(output) => {
                        let reference = output.resource();
                        let resource_id = realized
                            .index()
                            .get(reference.module(), reference.logical_id())?;
                        state
                            .resources
                            .get(resource_id)?
                            .properties
                            .get(output.output())?
                            .as_str()
                            .map(str::to_string)
                    }
                }?;
                Some((entry.key().to_string(), value))
            })
            .collect()
    }
}
