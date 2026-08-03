//! Definition frontend contract, source admission, evaluation, and configuration identity.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokeira_orchestrator::DefinitionFormatId;

use crate::{
    author::LocatedValue,
    binding::{Platform, PlatformBinding},
    error::{DefinitionError, FrontendDiagnostic, VerificationFinding, VerificationReport},
    graph::VerifiedGraph,
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

/// Completed transient definition returned by one frontend evaluation.
#[derive(Debug)]
pub struct FrontendOutput {
    /// Host-free configuration value.
    pub config: LocatedValue,
    /// Completed structural graph built inside the frontend evaluator.
    pub graph: VerifiedGraph,
}

/// Statically assembled parser/checker/evaluator for one definition format.
pub trait DefinitionFrontend<P: Platform>: Clone + Send + Sync + 'static {
    /// Open validated format identity embedded in the assembled provisioner.
    fn format(&self) -> &DefinitionFormatId;

    /// Parse, check, and evaluate into one transient in-memory definition.
    fn evaluate(
        &self,
        source: FrontendSource<'_>,
        context: &P::Context,
        binding: &PlatformBinding<P>,
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
        let output = self.frontend.evaluate(
            FrontendSource {
                source_name: &request.source.source_name,
                bytes: request.source.bytes.as_ref(),
            },
            &request.context,
            &self.binding,
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
        Ok(EvaluatedDefinition {
            config,
            graph: output.graph,
            configuration_identity: identity,
        })
    }

    /// Validate every provider-kind input without fabricating invocation facts.
    pub fn verify<'a>(
        &self,
        definition: &'a EvaluatedDefinition<P>,
    ) -> Result<VerifiedDefinition<'a, P>, VerificationReport> {
        let mut findings = Vec::new();

        for resource in definition.graph.resources() {
            if let Err(error) = resource.kind().validate_input() {
                findings.push(VerificationFinding::InvalidInput {
                    resource: format!("{}/{}", resource.module(), resource.logical_id()),
                    provider_kind: resource.kind().kind_name().to_string(),
                    message: error.message,
                });
            }
        }

        if findings.is_empty() {
            Ok(VerifiedDefinition { definition })
        } else {
            Err(VerificationReport { findings })
        }
    }
}

/// Definition whose complete logical provider-kind set passed pure input validation.
pub struct VerifiedDefinition<'a, P: Platform> {
    /// Typed evaluated definition.
    pub definition: &'a EvaluatedDefinition<P>,
}

impl<P: Platform> fmt::Debug for VerifiedDefinition<'_, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedDefinition")
            .field("resource_count", &self.definition.graph.resources().len())
            .finish_non_exhaustive()
    }
}
