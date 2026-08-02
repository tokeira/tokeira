//! Definition frontend contract, source admission, evaluation, and configuration identity.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokeira_orchestrator::DefinitionFormatId;

use crate::{
    author::{AuthorNode, AuthorSession},
    binding::{Platform, PlatformBinding},
    catalog::PlacementContext,
    error::{DefinitionError, FrontendDiagnostic, VerificationFinding, VerificationReport},
    graph::{DeploymentHandle, VerifiedGraph},
};

/// Safe canonical path relative to one deployment root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RelativeDefinitionPath(String);

/// Canonical source-file extension without a leading dot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DefinitionSourceExtension(String);

impl DefinitionSourceExtension {
    /// Validate a portable lower-kebab source extension.
    pub fn new(value: impl Into<String>) -> Result<Self, DefinitionSourceExtensionError> {
        let value = value.into();
        DefinitionFormatId::new(value.clone()).map_err(|source| {
            DefinitionSourceExtensionError {
                value: value.clone(),
                source,
            }
        })?;
        Ok(Self(value))
    }

    /// Borrow the extension without a leading dot.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DefinitionSourceExtension {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Rejection of a non-portable definition source extension.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid source extension `{value}`: {source}")]
pub struct DefinitionSourceExtensionError {
    value: String,
    source: tokeira_orchestrator::IdentifierError,
}

impl RelativeDefinitionPath {
    /// Validate a portable deployment-relative definition path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, DefinitionPathError> {
        let path = path.as_ref();
        let Some(value) = path.to_str() else {
            return Err(DefinitionPathError::NonUtf8);
        };
        if value.is_empty() {
            return Err(DefinitionPathError::Empty);
        }
        if path.is_absolute() || value.starts_with('/') {
            return Err(DefinitionPathError::Absolute(value.to_string()));
        }
        if value.contains('\\') || value.contains(':') {
            return Err(DefinitionPathError::NonCanonical(value.to_string()));
        }
        if value
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(DefinitionPathError::NonCanonical(value.to_string()));
        }
        Ok(Self(value.to_string()))
    }

    /// Borrow the portable slash-separated path.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrow as a host path only after deployment-root validation.
    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl<'de> Deserialize<'de> for RelativeDefinitionPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Rejection reason for recorded deployment-definition paths.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DefinitionPathError {
    /// Path has no components.
    #[error("definition path cannot be empty")]
    Empty,
    /// Absolute paths could escape the deployment root.
    #[error("definition path `{0}` must be deployment-relative")]
    Absolute(String),
    /// Path contains aliases, escaping components, separators, or empty components.
    #[error("definition path `{0}` is not canonical and deployment-relative")]
    NonCanonical(String),
    /// Deployment metadata paths must be portable UTF-8.
    #[error("definition path is not valid UTF-8")]
    NonUtf8,
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

/// Exact admitted source and independently selected format.
#[derive(Debug, Clone)]
pub struct DefinitionSource {
    /// Recorded or explicitly selected frontend format.
    pub format: DefinitionFormatId,
    /// Display/persistence-safe source identity.
    pub source_name: DefinitionSourceName,
    /// Exact bytes evaluated and hashed for configuration identity.
    pub bytes: Arc<[u8]>,
}

/// Borrowed source supplied to one statically selected definition frontend.
#[derive(Debug, Clone, Copy)]
pub struct FrontendSource<'a> {
    /// Display-safe source identity.
    pub source_name: &'a DefinitionSourceName,
    /// Exact source bytes.
    pub bytes: &'a [u8],
}

/// Frontend result before typed config and graph completion admission.
#[derive(Debug)]
pub struct FrontendOutput {
    /// Host-free configuration value.
    pub config: AuthorNode,
    /// Opaque final deployment handle from the supplied author session.
    pub deployment: DeploymentHandle,
}

/// Statically assembled parser/checker/evaluator for one definition format.
pub trait DefinitionFrontend<P: Platform>: Clone + Send + Sync + 'static {
    /// Open validated format identity embedded in the assembled provisioner.
    fn format(&self) -> &DefinitionFormatId;

    /// Parse, check, and evaluate while driving only the language-neutral author session.
    fn evaluate(
        &self,
        source: FrontendSource<'_>,
        author: &mut AuthorSession<P>,
    ) -> Result<FrontendOutput, FrontendDiagnostic>;
}

/// Input to one pure platform definition evaluation.
#[derive(Debug)]
pub struct DefinitionRequest<P: Platform> {
    /// Admitted format, source name, and exact bytes.
    pub source: DefinitionSource,
    /// Immutable platform context for this invocation.
    pub context: P::Context,
}

/// Versioned content identity of format plus exact definition bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationIdentity {
    /// Identity algorithm/version.
    pub algorithm: String,
    /// Lowercase SHA-256 digest.
    pub digest: String,
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
            algorithm: "sha256-v1".to_string(),
            digest: hex::encode(digest.finalize()),
        }
    }
}

/// Typed config, immutable graph, and source-derived identity admitted in memory.
#[derive(Debug)]
pub struct EvaluatedDefinition<P: Platform> {
    /// Typed platform config; the source remains its sole persisted desired representation.
    pub config: P::Config,
    /// Completed language-neutral graph.
    pub graph: VerifiedGraph,
    /// Format-plus-source configuration identity.
    pub configuration_identity: ConfigurationIdentity,
}

/// One selected platform binding and one statically selected definition frontend.
#[derive(Debug, Clone)]
pub struct DefinitionEngine<P: Platform, F: DefinitionFrontend<P>> {
    binding: PlatformBinding<P>,
    frontend: F,
}

impl<P: Platform, F: DefinitionFrontend<P>> DefinitionEngine<P, F> {
    /// Assemble from a validated platform binding and one frontend.
    pub fn new(binding: PlatformBinding<P>, frontend: F) -> Self {
        Self { binding, frontend }
    }

    /// Parse, evaluate, admit typed config, and complete the graph without I/O.
    pub fn evaluate(
        &self,
        request: DefinitionRequest<P>,
    ) -> Result<EvaluatedDefinition<P>, DefinitionError> {
        if &request.source.format != self.frontend.format() {
            return Err(DefinitionError::FormatMismatch {
                source_format: request.source.format,
                frontend_format: self.frontend.format().clone(),
            });
        }
        let identity =
            ConfigurationIdentity::compute(&request.source.format, request.source.bytes.as_ref());
        let format = request.source.format.clone();
        let source_name = request.source.source_name.clone();
        let mut author = AuthorSession::new(self.binding.clone(), request.context);
        let output = self.frontend.evaluate(
            FrontendSource {
                source_name: &request.source.source_name,
                bytes: request.source.bytes.as_ref(),
            },
            &mut author,
        )?;
        let config =
            self.binding
                .config
                .admit(output.config)
                .map_err(|error| DefinitionError::Config {
                    format: format.clone(),
                    source_name: source_name.clone(),
                    error,
                })?;
        let graph = author
            .finish(output.deployment)
            .map_err(|error| DefinitionError::Graph {
                format,
                source_name,
                error: Box::new(error),
            })?;
        Ok(EvaluatedDefinition {
            config,
            graph,
            configuration_identity: identity,
        })
    }

    /// Realize and verify the complete provider-resource set without clients or state access.
    pub fn verify<'a>(
        &self,
        definition: &'a EvaluatedDefinition<P>,
    ) -> Result<VerifiedDefinition<'a, P>, VerificationReport> {
        let mut resources = Vec::<Box<dyn tokeira_iac::Resource>>::new();
        let mut realized_ids = BTreeMap::<(String, String), tokeira_iac::ResourceId>::new();
        let mut kind_by_id = BTreeMap::<tokeira_iac::ResourceId, String>::new();
        let mut findings = Vec::new();

        for resource in definition.graph.resources() {
            let logical = format!("{}/{}", resource.module(), resource.logical_id());
            let mut dependencies = Vec::new();
            for (module, logical_id) in resource.dependencies() {
                if let Some(id) = realized_ids.get(&(module.to_string(), logical_id.to_string())) {
                    dependencies.push(id.clone());
                } else {
                    findings.push(VerificationFinding::MissingDependency {
                        resource: logical.clone(),
                        dependency: format!("{module}/{logical_id}"),
                    });
                }
            }
            match resource.kind().realize(&PlacementContext {
                deployment_id: "definition-check".to_string(),
                module: resource.module().to_string(),
                logical_id: resource.logical_id().to_string(),
                dependencies,
                tags: BTreeMap::new(),
            }) {
                Ok(realized) => {
                    kind_by_id.insert(
                        realized.resource_id(),
                        resource.kind().kind_name().to_string(),
                    );
                    realized_ids.insert(
                        (
                            resource.module().to_string(),
                            resource.logical_id().to_string(),
                        ),
                        realized.resource_id(),
                    );
                    resources.push(realized);
                }
                Err(error) => findings.push(VerificationFinding::CannotRealize {
                    resource: logical,
                    provider_kind: resource.kind().kind_name().to_string(),
                    message: error.message,
                }),
            }
        }

        let ids = resources
            .iter()
            .map(|resource| resource.resource_id())
            .collect::<BTreeSet<_>>();
        let engine_findings = tokeira_iac::verify_resources(
            &resources
                .iter()
                .map(Box::as_ref)
                .collect::<Vec<&dyn tokeira_iac::Resource>>(),
        );
        let provider_finding_start = findings.len();
        for resource in &resources {
            if !resource.describes() {
                findings.push(VerificationFinding::CannotDescribe {
                    resource: resource.resource_id().0.clone(),
                    provider_kind: kind_by_id
                        .get(&resource.resource_id())
                        .cloned()
                        .unwrap_or_else(|| resource.resource_type().0),
                });
            }
            for dependency in resource.dependencies() {
                if !ids.contains(&dependency) {
                    findings.push(VerificationFinding::MissingDependency {
                        resource: resource.resource_id().0,
                        dependency: dependency.0,
                    });
                }
            }
        }
        debug_assert_eq!(
            engine_findings.len(),
            findings.len() - provider_finding_start,
            "typed definition findings must cover the engine verifier exactly"
        );

        if findings.is_empty() {
            Ok(VerifiedDefinition {
                definition,
                resources,
            })
        } else {
            Err(VerificationReport { findings })
        }
    }
}

/// Definition plus the complete pure-realized resource set accepted for execution.
pub struct VerifiedDefinition<'a, P: Platform> {
    /// Typed evaluated definition.
    pub definition: &'a EvaluatedDefinition<P>,
    resources: Vec<Box<dyn tokeira_iac::Resource>>,
}

impl<P: Platform> fmt::Debug for VerifiedDefinition<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedDefinition")
            .field("resource_count", &self.resources.len())
            .finish_non_exhaustive()
    }
}

impl<'a, P: Platform> VerifiedDefinition<'a, P> {
    /// Borrow realized resources in definition order.
    pub fn resources(&self) -> impl ExactSizeIterator<Item = &dyn tokeira_iac::Resource> {
        self.resources.iter().map(Box::as_ref)
    }
}
