//! Definition source admission, frontend evaluation, and invocation-bound realization.

use std::{collections::BTreeMap, fmt, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::{
    author::LocatedValue,
    error::{
        DefinitionError, FrontendDiagnostic, KindError, ProjectionError, VerificationFinding,
        VerificationReport,
    },
    graph::{VerifiedGraph, WritebackValue},
    kind::{Kind, PlacementContext},
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
    /// Decode one authored kind; `None` when the name is not this
    /// namespace's.
    pub decode: fn(&str, LocatedValue) -> Option<Result<Box<dyn Kind>, KindError>>,
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
    pub graph: VerifiedGraph<Box<dyn Kind>>,
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
    /// every part the evaluation loaded, in first-request order. A
    /// single-document definition keeps [`Self::compute`]'s `sha256-v1`
    /// digest byte-for-byte, so recorded identities never move when this
    /// variant is introduced.
    fn compute_set(
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
}

/// Evaluate a source against the declared provider namespaces; retain no
/// frontend runtime state.
pub fn evaluate_definition<Cx, F>(
    frontend: &F,
    source: DefinitionSource,
    context: &Cx,
    namespaces: &[Namespace],
    parts: &dyn SourceResolver,
) -> Result<EvaluatedDefinition<Box<dyn Kind>>, DefinitionError>
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
    })
}

/// Definition whose complete concrete kind set passed pure input validation.
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
    /// Borrow the exact evaluated definition that was validated.
    pub fn definition(&self) -> &'a EvaluatedDefinition<K> {
        self.definition
    }
}

/// Validate every concrete kind input without fabricating invocation facts.
pub fn verify_definition<K: Kind>(
    definition: &EvaluatedDefinition<K>,
) -> Result<VerifiedDefinition<'_, K>, VerificationReport> {
    let findings =
        definition
            .graph
            .resources()
            .iter()
            .filter_map(|resource| {
                resource.kind().validate_input().err().map(|error| {
                    VerificationFinding::InvalidInput {
                        resource: format!("{}/{}", resource.module(), resource.logical_id()),
                        provider_kind: resource.kind().name().to_string(),
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
    manifests: BTreeMap<tokeira_iac::ResourceId, serde_json::Value>,
}

impl fmt::Debug for RealizedResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealizedResources")
            .field("index", &self.index)
            .field("resource_count", &self.resources.len())
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

    /// Transfer resources to the infrastructure engine.
    pub fn into_resources(self) -> Vec<Box<dyn tokeira_iac::Resource>> {
        self.resources
    }

    /// Provider-owned desired manifests keyed by executed resource identity.
    pub fn manifests(&self) -> &BTreeMap<tokeira_iac::ResourceId, serde_json::Value> {
        &self.manifests
    }
}

impl<K: Kind> VerifiedDefinition<'_, K> {
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
            let dependencies: Vec<tokeira_iac::ResourceId> = node
                .dependencies()
                .iter()
                .map(|dependency| {
                    index
                        .get(dependency.module(), dependency.logical_id())
                        .cloned()
                        .expect("the selected resource has realized dependencies")
                })
                .collect();
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
            let manifest = node.kind().desired_manifest(&placement);
            let resource = node
                .kind()
                .realize(&placement)
                .map_err(|error| ProjectionError {
                    resource: format!("{}/{}", node.module(), node.logical_id()),
                    provider_kind: node.kind().name().to_string(),
                    message: error.message,
                })?;
            let resource_id = resource.resource_id();
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
            realized[node_index] = Some(resource);
        }

        let mut resources = Vec::with_capacity(realized.len());
        for entry in realized {
            resources.push(entry.expect("every verified resource was realized"));
        }
        Ok(RealizedResources {
            index,
            resources,
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
            let mut graph = StructuralGraphBuilder::<Box<dyn Kind>>::new();
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
