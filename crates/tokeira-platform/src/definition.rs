//! Definition source admission, frontend evaluation, and invocation-bound realization.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::{
    author::LocatedValue,
    error::{DefinitionError, FrontendDiagnostic, KindError, ProjectionError},
    graph::{VerifiedGraph, WritebackValue},
    kind::{DecodedKind, PlacementContext, RealizedResource},
};

/// One provider namespace: the runtime shadow of a crate dependency.
///
/// The name is the normalized crate name a definition imports from
/// (`tokeira_compose`, `tokeira_aws`); the kind names and the decoder are
/// the crate's own facts, stated next to its resources. A frontend resolves
/// `use tokeira_aws::DsqlCluster` against the assembled binary's list of
/// these — there is no composed vocabulary and no registration.
#[derive(Debug, Clone, Copy)]
pub struct Namespace {
    /// The normalized crate name definitions import from.
    pub name: &'static str,
    /// Author-visible kind names, for module synthesis and located
    /// unknown-name errors.
    pub kinds: &'static [&'static str],
    /// Authoring-only empty shapes used by frontends that support explicit
    /// struct-update defaults such as `.tkd`'s `..Kind::EMPTY`.
    pub defaults: Option<fn(&str) -> Option<LocatedValue>>,
    /// Decode one authored kind; `None` when the name is not this
    /// namespace's.
    pub decode: fn(&str, LocatedValue) -> Option<Result<DecodedKind, KindError>>,
}

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

/// Serves a definition's part sources by name: `mod networking;` in a root
/// asks for `networking`, and the resolver answers the sibling document's
/// exact bytes. The caller anchors resolution at the interpreted source's
/// own directory — a retained revision's root loads that revision's parts,
/// never the live ones — and names are bare identifiers: the resolver owns
/// the `<name>.<extension>` mapping and admits no path separators.
pub trait SourceResolver {
    /// The named part's exact bytes.
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError>;
}

/// A part source that could not be served.
#[derive(Debug, thiserror::Error)]
#[error("part `{name}` cannot be served: {reason}")]
pub struct PartResolveError {
    /// The requested part name.
    pub name: String,
    /// What went wrong, in the resolver's own words (the attempted path,
    /// the refusal class).
    pub reason: String,
}

/// The canonical resolver for contexts with no part sources: evaluation
/// paths that predate set retention answer every request with a typed
/// refusal instead of guessing at a directory.
#[derive(Debug, Clone, Copy)]
pub struct NoPartSources;

impl SourceResolver for NoPartSources {
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
        Err(PartResolveError {
            name: name.to_string(),
            reason: "no part sources are available on this evaluation path".to_string(),
        })
    }
}

/// Serves parts from one directory: `name` maps to `<dir>/<name>.<ext>`.
/// Names are admitted as bare identifiers only — separators, dots, and
/// empty names refuse — so a definition can never reach outside the
/// directory its root was interpreted from.
#[derive(Debug, Clone)]
pub struct DirectoryPartSources {
    dir: std::path::PathBuf,
    extension: String,
}

impl DirectoryPartSources {
    /// Resolve parts beside the interpreted source, with the format's
    /// source extension.
    pub fn new(dir: impl Into<std::path::PathBuf>, extension: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            extension: extension.into(),
        }
    }
}

impl SourceResolver for DirectoryPartSources {
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
        let admissible =
            !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !admissible {
            return Err(PartResolveError {
                name: name.to_string(),
                reason: "part names are bare identifiers".to_string(),
            });
        }
        let path = self.dir.join(format!("{name}.{}", self.extension));
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Arc::from(bytes)),
            Err(error) => Err(PartResolveError {
                name: name.to_string(),
                reason: format!("failed to read {}: {error}", path.display()),
            }),
        }
    }
}

/// Records which parts a frontend loaded, in first-request order, so the
/// configuration identity can cover the complete evaluated set. Repeat
/// requests serve the recorded bytes, keeping one identity entry per part.
struct RecordingResolver<'a> {
    inner: &'a dyn SourceResolver,
    served: std::cell::RefCell<Vec<(String, Arc<[u8]>)>>,
}

impl SourceResolver for RecordingResolver<'_> {
    fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
        if let Some((_, bytes)) = self
            .served
            .borrow()
            .iter()
            .find(|(served, _)| served == name)
        {
            return Ok(bytes.clone());
        }
        let bytes = self.inner.resolve(name)?;
        self.served
            .borrow_mut()
            .push((name.to_string(), bytes.clone()));
        Ok(bytes)
    }
}

/// Completed transient structure returned by a definition frontend.
#[derive(Debug)]
pub struct FrontendOutput {
    /// Host-free platform configuration value.
    pub config: LocatedValue,
    /// Completed structural graph built inside the frontend evaluator.
    pub graph: VerifiedGraph<DecodedKind>,
}

/// Statically assembled evaluator for one definition format.
///
/// The frontend receives the assembled binary's provider [`Namespace`] list
/// by reference: the kinds a definition may import are exactly those crates'
/// facts, and the frontend needs nothing else — names for module synthesis
/// and unknown-name refusals, decoding into realizable resources.
pub trait DefinitionFrontend: Clone + Send + Sync + 'static {
    /// Open validated format identity embedded in the assembled provisioner.
    fn format(&self) -> &DefinitionFormatId;

    /// Evaluate typed context into one completed transient structure.
    fn evaluate<C>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        namespaces: &[Namespace],
        parts: &dyn SourceResolver,
    ) -> Result<FrontendOutput, FrontendDiagnostic>
    where
        C: Serialize;

    /// Refuse a create-time-immutable change between two sources: the prior
    /// admitted configuration and the one about to apply. Each refusal
    /// message names a changed field. Each side resolves its definition
    /// parts through its own resolver — the prior side against the retained
    /// revision's set, the current side against the live one — so a
    /// multi-document definition compares as the set it was, not just its
    /// root. The default gates nothing — a format with no create-time
    /// admission surface admits every edit; the `.tkd` frontend overrides
    /// this, and `.tkdp` adopts when its admission surface lands.
    fn retarget_check<C>(
        &self,
        _prior: FrontendSource<'_>,
        _current: FrontendSource<'_>,
        _context: &C,
        _namespaces: &[Namespace],
        _prior_parts: &dyn SourceResolver,
        _current_parts: &dyn SourceResolver,
    ) -> Result<(), Vec<String>>
    where
        C: Serialize,
    {
        Ok(())
    }
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
    #[serde(rename = "sha256-set-v1")]
    Sha256SetV1,
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

    /// Compute the identity of a multi-document definition: the root plus
    /// every companion the evaluation loaded, in first-request order. A
    /// single-document definition keeps [`Self::compute`]'s `sha256-v1`
    /// digest byte-for-byte, so recorded identities never move when this
    /// variant is introduced.
    ///
    /// Public so a verifier can recompute the identity over fetched bytes in
    /// a claimed order and compare it with a recorded one (this is the sole
    /// implementation — the deployment repository never mirrors it). The
    /// byte layout is a compatibility surface: length-prefixed,
    /// domain-separated, order-sensitive; goldens pin it in the tests.
    pub fn compute_set(
        format: &DefinitionFormatId,
        root: &[u8],
        parts: &[(String, Arc<[u8]>)],
    ) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"tokeira.configuration.set.v1\0");
        digest.update((format.as_str().len() as u64).to_be_bytes());
        digest.update(format.as_str().as_bytes());
        digest.update((root.len() as u64).to_be_bytes());
        digest.update(root);
        for (name, bytes) in parts {
            digest.update((name.len() as u64).to_be_bytes());
            digest.update(name.as_bytes());
            digest.update((bytes.len() as u64).to_be_bytes());
            digest.update(bytes.as_ref());
        }
        Self {
            algorithm: ConfigurationIdentityAlgorithm::Sha256SetV1,
            digest: hex::encode(digest.finalize()),
        }
    }

    /// Stable serialized algorithm/version label.
    pub fn algorithm(&self) -> &'static str {
        match self.algorithm {
            ConfigurationIdentityAlgorithm::Sha256V1 => "sha256-v1",
            ConfigurationIdentityAlgorithm::Sha256SetV1 => "sha256-set-v1",
        }
    }
}

/// Evaluated configuration value, immutable graph, and source identity
/// admitted in memory.
///
/// The configuration is the frontend's evaluated value, held as data: the
/// definition authors the shape, the kinds validate their own inputs, and no
/// platform-side configuration type exists to decode into.
#[derive(Debug)]
pub struct EvaluatedDefinition<K> {
    /// The evaluated configuration value.
    pub config: LocatedValue,
    /// Completed structural graph.
    pub graph: VerifiedGraph<K>,
    /// Format-plus-source configuration identity.
    pub configuration_identity: ConfigurationIdentity,
    /// The companion documents evaluation actually served, in first-request
    /// order — exactly the set and order `configuration_identity` was
    /// computed over (empty for a single-document definition). Surfaced so a
    /// publisher can claim the served order without re-deriving it from
    /// declarations or the filesystem (deployment-repository spec, Req 1/2).
    pub served_companions: Vec<(String, Arc<[u8]>)>,
}

/// Evaluate a source against the declared provider namespaces; retain no
/// frontend runtime state.
pub fn evaluate_definition<Cx, F>(
    frontend: &F,
    source: DefinitionSource,
    context: &Cx,
    namespaces: &[Namespace],
    parts: &dyn SourceResolver,
) -> Result<EvaluatedDefinition<DecodedKind>, DefinitionError>
where
    Cx: Serialize,
    F: DefinitionFrontend,
{
    if &source.format != frontend.format() {
        return Err(DefinitionError::FormatMismatch {
            source_format: source.format,
            frontend_format: frontend.format().clone(),
        });
    }
    // The identity must cover exactly what evaluation read, so the parts
    // the frontend loads are recorded here rather than re-derived: a root
    // with no `mod` declarations keeps the single-document identity
    // unchanged.
    let recorder = RecordingResolver {
        inner: parts,
        served: std::cell::RefCell::new(Vec::new()),
    };
    let output = frontend.evaluate(
        FrontendSource {
            source_name: &source.source_name,
            bytes: source.bytes.as_ref(),
        },
        context,
        namespaces,
        &recorder,
    )?;
    let served = recorder.served.into_inner();
    let identity = if served.is_empty() {
        ConfigurationIdentity::compute(&source.format, source.bytes.as_ref())
    } else {
        ConfigurationIdentity::compute_set(&source.format, source.bytes.as_ref(), &served)
    };
    Ok(EvaluatedDefinition {
        config: output.config,
        graph: output.graph,
        configuration_identity: identity,
        served_companions: served,
    })
}

/// Structurally admitted definition waiting for invocation-bound resources.
pub struct VerifiedDefinition<'a, K> {
    definition: &'a EvaluatedDefinition<K>,
}

impl<K> fmt::Debug for VerifiedDefinition<'_, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedDefinition")
            .field("resource_count", &self.definition.graph.resources().len())
            .finish()
    }
}

impl<'a, K> VerifiedDefinition<'a, K> {
    /// Borrow the exact evaluated definition that was admitted.
    pub fn definition(&self) -> &'a EvaluatedDefinition<K> {
        self.definition
    }
}

/// Retain the exact structurally verified definition for realization.
///
/// Resource validation occurs immediately after each [`Kind`](crate::kind::Kind)
/// constructs its complete resource. That keeps provider facts on the
/// resource without fabricating placement here.
pub fn verify_definition<K>(definition: &EvaluatedDefinition<K>) -> VerifiedDefinition<'_, K> {
    VerifiedDefinition { definition }
}

/// Logical-to-infrastructure identity produced by one execution realization.
///
/// Only infrastructure resources have identities in [`tokeira_iac::InfraState`]
/// and may supply writeback outputs. Service realization is intentionally not
/// projected into this index.
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
    services: Vec<Box<dyn tokeira_deploy_engine::Service>>,
    manifests: BTreeMap<tokeira_iac::ResourceId, serde_json::Value>,
}

impl fmt::Debug for RealizedResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealizedResources")
            .field("index", &self.index)
            .field("resource_count", &self.resources.len())
            .field("service_count", &self.services.len())
            .field("manifest_count", &self.manifests.len())
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

    /// Transfer resources to their sole owning engines.
    // The pair is the whole ownership hand-off; naming aliases for two
    // one-use vectors would add vocabulary without clarifying the boundary.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Vec<Box<dyn tokeira_iac::Resource>>,
        Vec<Box<dyn tokeira_deploy_engine::Service>>,
    ) {
        (self.resources, self.services)
    }

    /// Provider-owned desired manifests keyed by executed resource identity.
    pub fn manifests(&self) -> &BTreeMap<tokeira_iac::ResourceId, serde_json::Value> {
        &self.manifests
    }
}

impl VerifiedDefinition<'_, DecodedKind> {
    /// Realize the verified set once with real invocation identity and placement.
    /// `definition_dir` names where the interpreted source was read from —
    /// the deployment root for a working realization, a retained revision
    /// folder for a baseline — so kinds resolve desired-source companions
    /// against the source set actually being realized.
    pub fn realize(
        &self,
        deployment_id: &str,
        deployment_dir: &std::path::Path,
        definition_dir: &std::path::Path,
        tags: &BTreeMap<String, String>,
    ) -> Result<RealizedResources, ProjectionError> {
        let nodes = self.definition.graph.resources();
        let mut index = RealizedResourceIndex::default();
        let mut content = BTreeMap::new();
        let mut manifests = BTreeMap::new();
        let mut completed = BTreeSet::new();
        let mut pending = (0..nodes.len()).collect::<Vec<_>>();
        let mut realized_nodes = std::iter::repeat_with(|| None)
            .take(nodes.len())
            .collect::<Vec<Option<RealizedResource>>>();
        while !pending.is_empty() {
            let Some(position) = pending.iter().position(|node_index| {
                nodes[*node_index].dependencies().iter().all(|dependency| {
                    completed.contains(&(
                        dependency.module().to_string(),
                        dependency.logical_id().to_string(),
                    ))
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
            let dependencies: Vec<tokeira_iac::ResourceId> = node
                .dependencies()
                .iter()
                .filter_map(|dependency| {
                    index
                        .get(dependency.module(), dependency.logical_id())
                        .cloned()
                })
                .collect();
            let dependency_count = dependencies.len();
            let dependency_content = dependencies
                .iter()
                .filter_map(|id| {
                    content
                        .get(id)
                        .cloned()
                        .map(|identity| (id.clone(), identity))
                })
                .collect();
            let placement = PlacementContext {
                deployment_id: deployment_id.to_string(),
                deployment_dir: deployment_dir.to_path_buf(),
                definition_dir: definition_dir.to_path_buf(),
                module: node.module().to_string(),
                logical_id: node.logical_id().to_string(),
                dependencies,
                dependency_content,
                tags: tags.clone(),
            };
            let realized = node
                .kind()
                .realize(&placement)
                .map_err(|error| ProjectionError {
                    resource: format!("{}/{}", node.module(), node.logical_id()),
                    provider_kind: node.kind().name().to_string(),
                    message: error.message,
                })?;
            for entry in self.definition.graph.writeback() {
                let WritebackValue::Output(output) = entry.value() else {
                    continue;
                };
                if output.resource() == node.reference()
                    && !realized.declared_outputs().contains(&output.output())
                {
                    return Err(ProjectionError {
                        resource: format!("{}/{}", node.module(), node.logical_id()),
                        provider_kind: node.kind().name().to_string(),
                        message: format!(
                            "unknown output `{}`; supported outputs: {}",
                            output.output(),
                            realized.declared_outputs().join(", ")
                        ),
                    });
                }
            }
            match &realized {
                RealizedResource::Infra(resource) => {
                    if dependency_count != node.dependencies().len() {
                        return Err(ProjectionError {
                            resource: format!("{}/{}", node.module(), node.logical_id()),
                            provider_kind: node.kind().name().to_string(),
                            message:
                                "an infrastructure resource cannot depend on a service resource"
                                    .to_string(),
                        });
                    }
                    let resource_id = resource.resource_id();
                    let manifest = resource.desired_manifest();
                    content.insert(
                        resource_id.clone(),
                        crate::content::ContentIdentity::new(
                            &format!("provider-resource/{}", node.kind().name()),
                            manifest.to_string().as_bytes(),
                        ),
                    );
                    manifests.insert(resource_id.clone(), manifest);
                    index.ids.insert(
                        (node.module().to_string(), node.logical_id().to_string()),
                        resource_id,
                    );
                }
                RealizedResource::Service(_) => {
                    if self.definition.graph.writeback().iter().any(|entry| {
                        matches!(
                            entry.value(),
                            WritebackValue::Output(output) if output.resource() == node.reference()
                        )
                    }) {
                        return Err(ProjectionError {
                            resource: format!("{}/{}", node.module(), node.logical_id()),
                            provider_kind: node.kind().name().to_string(),
                            message: "service outputs cannot be written to infrastructure-backed configuration"
                                .to_string(),
                        });
                    }
                }
            }
            realized_nodes[node_index] = Some(realized);
            completed.insert((node.module().to_string(), node.logical_id().to_string()));
        }

        let mut resources = Vec::new();
        let mut services = Vec::new();
        for resource in realized_nodes {
            match resource.expect("every verified resource was realized") {
                RealizedResource::Infra(resource) => resources.push(resource),
                RealizedResource::Service(service) => services.push(service),
            }
        }
        Ok(RealizedResources {
            index,
            resources,
            services,
            manifests,
        })
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

#[cfg(test)]
mod part_tests {
    use std::sync::Arc;

    use serde::Serialize;

    use super::*;
    use crate::{author::ValueShape, graph::StructuralGraphBuilder};

    /// The smallest frontend that exercises the resolver: unit config, one
    /// module, and the configured part requests in order.
    #[derive(Clone)]
    struct PartRequestingFrontend {
        format: DefinitionFormatId,
        requests: Vec<&'static str>,
    }

    impl PartRequestingFrontend {
        fn new(requests: Vec<&'static str>) -> Self {
            Self {
                format: DefinitionFormatId::new("tkd").expect("static id"),
                requests,
            }
        }
    }

    impl DefinitionFrontend for PartRequestingFrontend {
        fn format(&self) -> &DefinitionFormatId {
            &self.format
        }

        fn evaluate<C: Serialize>(
            &self,
            source: FrontendSource<'_>,
            _context: &C,
            _namespaces: &[Namespace],
            parts: &dyn SourceResolver,
        ) -> Result<FrontendOutput, FrontendDiagnostic> {
            for name in &self.requests {
                parts.resolve(name).map_err(|error| FrontendDiagnostic {
                    format: self.format.clone(),
                    source_name: source.source_name.clone(),
                    range: None,
                    category: crate::error::DiagnosticCategory::Frontend,
                    message: error.to_string(),
                })?;
            }
            let mut graph = StructuralGraphBuilder::<DecodedKind>::new();
            graph.add_module("state", Vec::new());
            Ok(FrontendOutput {
                config: LocatedValue {
                    value: ValueShape::Unit,
                    range: None,
                },
                graph: graph.finish().expect("the one-module graph verifies"),
            })
        }
    }

    struct MapParts(std::collections::BTreeMap<&'static str, &'static [u8]>);

    impl SourceResolver for MapParts {
        fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
            self.0
                .get(name)
                .map(|bytes| Arc::from(*bytes))
                .ok_or_else(|| PartResolveError {
                    name: name.to_string(),
                    reason: "absent from the fixture".to_string(),
                })
        }
    }

    #[derive(Serialize)]
    struct Ctx {
        project_name: String,
    }

    fn source(bytes: &[u8]) -> DefinitionSource {
        DefinitionSource {
            format: DefinitionFormatId::new("tkd").expect("static id"),
            source_name: DefinitionSourceName::AuthoringPath("definition.tkd".into()),
            bytes: Arc::from(bytes.to_vec()),
        }
    }

    fn ctx() -> Ctx {
        Ctx {
            project_name: "demo".to_string(),
        }
    }

    fn namespaces() -> Vec<Namespace> {
        Vec::new()
    }

    // The regression pin: a root that loads no parts keeps the
    // single-document identity byte-for-byte — recorded identities of
    // every existing deployment never move.
    #[test]
    fn single_document_identity_is_byte_stable() {
        let frontend = PartRequestingFrontend::new(Vec::new());
        let evaluated = evaluate_definition(
            &frontend,
            source(b"root"),
            &ctx(),
            &namespaces(),
            &NoPartSources,
        )
        .expect("evaluates");
        let direct =
            ConfigurationIdentity::compute(&DefinitionFormatId::new("tkd").unwrap(), b"root");
        assert_eq!(evaluated.configuration_identity.algorithm(), "sha256-v1");
        assert_eq!(evaluated.configuration_identity.digest, direct.digest);
    }

    #[test]
    fn set_identity_covers_parts_and_dedupes_repeat_requests() {
        let parts = MapParts(std::collections::BTreeMap::from([
            ("a", b"alpha".as_slice()),
            ("b", b"beta".as_slice()),
        ]));

        let repeat = evaluate_definition(
            &PartRequestingFrontend::new(vec!["b", "a", "b"]),
            source(b"root"),
            &ctx(),
            &namespaces(),
            &parts,
        )
        .expect("evaluates");
        let once = evaluate_definition(
            &PartRequestingFrontend::new(vec!["b", "a"]),
            source(b"root"),
            &ctx(),
            &namespaces(),
            &parts,
        )
        .expect("evaluates");

        assert_eq!(repeat.configuration_identity.algorithm(), "sha256-set-v1");
        assert_eq!(
            repeat.configuration_identity.digest, once.configuration_identity.digest,
            "a repeat request adds nothing to the identity"
        );

        // A changed part byte moves the identity.
        let changed = MapParts(std::collections::BTreeMap::from([
            ("a", b"ALPHA".as_slice()),
            ("b", b"beta".as_slice()),
        ]));
        let moved = evaluate_definition(
            &PartRequestingFrontend::new(vec!["b", "a"]),
            source(b"root"),
            &ctx(),
            &namespaces(),
            &changed,
        )
        .expect("evaluates");
        assert_ne!(
            moved.configuration_identity.digest,
            once.configuration_identity.digest
        );
    }

    // The digest layouts are a compatibility surface shared with the
    // deployment repository's verifier; these vectors were computed
    // independently of this implementation (python hashlib, TUF spike),
    // so the layout cannot drift together with its own test.
    #[test]
    fn identity_layouts_match_independent_golden_vectors() {
        let format = DefinitionFormatId::new("tkd").unwrap();
        let single = ConfigurationIdentity::compute(&format, b"x");
        assert_eq!(single.algorithm(), "sha256-v1");
        assert_eq!(
            single.digest,
            "bb59fa39fd235403f766d2fe91db753b3033fc3df27b6c20146e41d62ac978a1"
        );

        let set = ConfigurationIdentity::compute_set(
            &format,
            b"mod a;\n",
            &[("a".to_string(), Arc::from(&b"A"[..]))],
        );
        assert_eq!(set.algorithm(), "sha256-set-v1");
        assert_eq!(
            set.digest,
            "be9dadaa31447ee1b988535f5add4a582710914db105ed08ebe6b314060ce8ba"
        );
    }

    proptest::proptest! {
        // Feature: deployment-repository, Property P2 (identity layer):
        // for any definition set and request order, the public
        // `compute_set` over the recorded served companions equals the
        // identity `evaluate_definition` computed, and the recorded order
        // is first-request order with repeats deduplicated.
        #[test]
        fn served_companions_recompute_to_the_evaluated_identity(
            root in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
            parts in proptest::collection::btree_map("[a-z]{1,6}", proptest::collection::vec(proptest::prelude::any::<u8>(), 0..32), 0..4),
            order_seed in proptest::collection::vec(proptest::prelude::any::<proptest::sample::Index>(), 0..8),
        ) {
            let names: Vec<String> = parts.keys().cloned().collect();
            let requests: Vec<&'static str> = order_seed
                .iter()
                .filter(|_| !names.is_empty())
                .map(|index| {
                    let name = &names[index.index(names.len())];
                    // Test-only leak: proptest cases are few and names tiny.
                    Box::leak(name.clone().into_boxed_str()) as &'static str
                })
                .collect();
            let resolver = OwnedMapParts(
                parts
                    .iter()
                    .map(|(name, bytes)| (name.clone(), Arc::from(bytes.as_slice())))
                    .collect(),
            );
            let evaluated = evaluate_definition(
                &PartRequestingFrontend {
                    format: DefinitionFormatId::new("tkd").unwrap(),
                    requests: requests.clone(),
                },
                source(&root),
                &ctx(),
                &namespaces(),
                &resolver,
            )
            .expect("fixture evaluation cannot fail");

            // First-request order, deduplicated.
            let mut expected_order = Vec::new();
            for name in &requests {
                if !expected_order.contains(&name.to_string()) {
                    expected_order.push(name.to_string());
                }
            }
            let served_names: Vec<String> = evaluated
                .served_companions
                .iter()
                .map(|(name, _)| name.clone())
                .collect();
            proptest::prop_assert_eq!(&served_names, &expected_order);

            // The public recomputation is the identity's sole implementation.
            let format = DefinitionFormatId::new("tkd").unwrap();
            let recomputed = if evaluated.served_companions.is_empty() {
                ConfigurationIdentity::compute(&format, &root)
            } else {
                ConfigurationIdentity::compute_set(&format, &root, &evaluated.served_companions)
            };
            proptest::prop_assert_eq!(
                &recomputed.digest,
                &evaluated.configuration_identity.digest
            );
            proptest::prop_assert_eq!(
                recomputed.algorithm(),
                evaluated.configuration_identity.algorithm()
            );
        }
    }

    struct OwnedMapParts(std::collections::BTreeMap<String, Arc<[u8]>>);

    impl SourceResolver for OwnedMapParts {
        fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
            self.0.get(name).cloned().ok_or_else(|| PartResolveError {
                name: name.to_string(),
                reason: "absent from the fixture".to_string(),
            })
        }
    }

    #[test]
    fn directory_parts_resolve_beside_the_source_and_refuse_non_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("networking.tkd"), b"part").unwrap();
        let parts = DirectoryPartSources::new(dir.path(), "tkd");

        assert_eq!(parts.resolve("networking").unwrap().as_ref(), b"part");

        for refused in ["", "a/b", "a\\b", "..", "a.b"] {
            let error = parts.resolve(refused).unwrap_err();
            assert!(
                error.reason.contains("bare identifiers"),
                "{refused:?}: {error}"
            );
        }

        let missing = parts.resolve("absent").unwrap_err();
        assert!(missing.reason.contains("absent.tkd"), "{missing}");
    }

    #[test]
    fn the_no_parts_path_refuses_by_name() {
        let error = NoPartSources.resolve("networking").unwrap_err();
        assert!(error.to_string().contains("`networking`"), "{error}");
        assert!(error.reason.contains("no part sources"), "{error}");
    }
}
